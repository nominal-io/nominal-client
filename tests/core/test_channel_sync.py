from __future__ import annotations

import gzip
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.core.channel import ChannelDataType
from nominal.experimental.migration.channel_sync import (
    ChannelSyncOptions,
    sync_missing_channel_data,
)
from nominal.experimental.migration.channel_sync import sync as sync_mod

SEC = 1_000_000_000


class FakeStream:
    """Captures enqueue_batch calls and supports the context-manager protocol."""

    def __init__(self) -> None:
        """Initialize the call capture buffer."""
        self.calls: list[tuple[str, list[Any], list[Any], Any]] = []

    def __enter__(self) -> FakeStream:
        """Enter the stream context."""
        return self

    def __exit__(self, *exc: object) -> None:
        """Exit the stream context."""
        return None

    def enqueue_batch(self, channel: str, timestamps: Any, values: Any, tags: Any) -> None:
        """Record an enqueue_batch call."""
        self.calls.append((channel, list(timestamps), list(values), tags))


def _write_csv_gz(path: Path, text: str) -> None:
    with gzip.open(path, "wb") as fh:
        fh.write(text.encode())


# --- _stream_file -------------------------------------------------------------------------


def test_stream_file_maps_columns_types_and_drops_nulls(tmp_path: Path) -> None:
    # rpm is DOUBLE but its values look integral; state is STRING with a missing middle cell.
    path = tmp_path / "part.csv.gz"
    _write_csv_gz(path, "timestamp,rpm,state\n0,1,on\n1000000000,2,\n2000000000,3,off\n")
    stream = FakeStream()
    type_by_name = {"rpm": ChannelDataType.DOUBLE, "state": ChannelDataType.STRING}

    points = sync_mod._stream_file(stream, path, type_by_name, {"unit": "rpm"})

    by_channel = {c[0]: c for c in stream.calls}
    # DOUBLE channel streams floats even though the CSV held integral text.
    assert by_channel["rpm"][1] == [0, SEC, 2 * SEC]
    assert by_channel["rpm"][2] == [1.0, 2.0, 3.0]
    assert all(isinstance(v, float) for v in by_channel["rpm"][2])
    # STRING channel drops the empty/null middle cell rather than streaming "".
    assert by_channel["state"][1] == [0, 2 * SEC]
    assert by_channel["state"][2] == ["on", "off"]
    # Tags are carried verbatim.
    assert by_channel["rpm"][3] == {"unit": "rpm"}
    assert points == 5


def test_stream_file_int_channel_tolerates_floats_and_recasts_integral(tmp_path: Path) -> None:
    # An INT-typed channel whose export holds a non-integral value must NOT crash on read (the bug:
    # forcing Int64 failed to parse "0.5"). Integral values re-cast to int; non-integral stay float.
    path = tmp_path / "part.csv.gz"
    _write_csv_gz(path, "timestamp,count\n0,5\n1000000000,7\n2000000000,0.5\n")
    stream = FakeStream()
    type_by_name = {"count": ChannelDataType.INT}

    points = sync_mod._stream_file(stream, path, type_by_name, None)

    assert points == 3
    values = stream.calls[0][2]
    assert values == [5, 7, 0.5]
    assert isinstance(values[0], int) and isinstance(values[1], int)
    assert isinstance(values[2], float)


def test_stream_file_identifies_timestamp_when_channel_named_timestamp(tmp_path: Path) -> None:
    # A data channel literally named "timestamp" forces the exporter to rename the time column.
    path = tmp_path / "part.csv.gz"
    _write_csv_gz(path, "timestamp.1,timestamp\n0,5\n1000000000,6\n")
    stream = FakeStream()
    type_by_name = {"timestamp": ChannelDataType.DOUBLE}

    points = sync_mod._stream_file(stream, path, type_by_name, None)

    assert points == 2
    assert stream.calls[0][0] == "timestamp"
    assert stream.calls[0][1] == [0, SEC]
    assert stream.calls[0][2] == [5.0, 6.0]


# --- _export_and_stream_range: stream-as-files-land via on_file_complete -------------------


class FakeHandler:
    """A PolarsExportHandler stand-in that writes gz files and fires on_file_complete per file."""

    def __init__(self, files: list[tuple[str, str]]) -> None:
        """Take ``(filename, csv_text)`` pairs to write on export."""
        self.files = files

    def export_to_files(
        self,
        channels: Any,
        start: int,
        end: int,
        out_dir: str,
        *,
        tags: Any = None,
        timestamp_type: Any = None,
        file_prefix: str = "export",
        show_progress: bool = False,
        on_file_planned: Any = None,
        on_file_complete: Any = None,
    ) -> list[Path]:
        """Write each file and invoke the hooks immediately, mimicking the pipelined exporter."""
        written: list[Path] = []
        for name, text in self.files:
            path = Path(out_dir) / name
            _write_csv_gz(path, text)
            written.append(path)
            if on_file_planned is not None:
                on_file_planned(path)
            if on_file_complete is not None:
                on_file_complete(path)
        return sorted(written)


def test_export_and_stream_range_streams_each_file_via_callback(tmp_path: Path) -> None:
    handler = FakeHandler(
        [
            ("a.csv.gz", "timestamp,rpm\n0,1\n"),
            ("b.csv.gz", "timestamp,rpm\n1000000000,2\n"),
        ]
    )
    stream = FakeStream()
    options = ChannelSyncOptions(output_dir=tmp_path)  # set output_dir -> files are kept

    points = sync_mod._export_and_stream_range(
        handler, stream, [_channel("rpm")], 0, SEC, {"rpm": ChannelDataType.DOUBLE}, options
    )

    assert points == 2
    assert [c[0] for c in stream.calls] == ["rpm", "rpm"]
    assert stream.calls[0][1] == [0] and stream.calls[1][1] == [SEC]
    # output_dir was provided, so files are left intact for inspection.
    assert (tmp_path / "a.csv.gz").exists()
    assert (tmp_path / "b.csv.gz").exists()


def test_export_and_stream_range_streaming_error_is_non_fatal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    handler = FakeHandler([("a.csv.gz", "timestamp,rpm\n0,1\n"), ("b.csv.gz", "timestamp,rpm\n1000000000,2\n")])
    stream = FakeStream()

    calls = {"n": 0}
    real_stream_file = sync_mod._stream_file

    def flaky_stream_file(*args: Any, **kwargs: Any) -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        return real_stream_file(*args, **kwargs)

    monkeypatch.setattr(sync_mod, "_stream_file", flaky_stream_file)

    # Must not raise; the failed file is swallowed (retried later via re-detect).
    points = sync_mod._export_and_stream_range(
        handler,
        stream,
        [_channel("rpm")],
        0,
        SEC,
        {"rpm": ChannelDataType.DOUBLE},
        ChannelSyncOptions(output_dir=tmp_path),
    )
    assert points == 1  # only the second file streamed successfully


# --- sync_missing_channel_data orchestration ----------------------------------------------


def _channel(name: str) -> SimpleNamespace:
    return SimpleNamespace(name=name, data_type=ChannelDataType.DOUBLE)


def test_sync_returns_early_when_nothing_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    source = SimpleNamespace(rid="src", search_channels=lambda: [_channel("a")])
    dest = SimpleNamespace(rid="dst")

    monkeypatch.setattr(sync_mod, "count_channels", lambda *a, **k: {})
    monkeypatch.setattr(sync_mod, "_detect_missing", lambda *a, **k: {})
    # If this were called it would try to construct a real handler -> fail loudly.
    monkeypatch.setattr(sync_mod, "PolarsExportHandler", lambda *a, **k: pytest.fail("should not export"))

    report = sync_missing_channel_data(source, object(), dest, 0, SEC)
    assert report.channels_examined == 1
    assert report.channels_missing == 0
    assert report.channels_synced == 0
    assert report.still_short == []


def test_sync_retries_then_reports_still_short(monkeypatch: pytest.MonkeyPatch) -> None:
    source = SimpleNamespace(rid="src", search_channels=lambda: [_channel("a"), _channel("b")])
    dest = SimpleNamespace(rid="dst")

    monkeypatch.setattr(sync_mod, "count_channels", lambda *a, **k: {})
    monkeypatch.setattr(sync_mod, "PolarsExportHandler", lambda *a, **k: object())
    monkeypatch.setattr(sync_mod.time, "sleep", lambda *_: None)

    # First detect: both short. After attempt 0: 'a' filled, 'b' still short. After attempt 1: still 'b'.
    detect_results = [
        {"a": [(0, SEC)], "b": [(0, SEC)]},
        {"b": [(0, SEC)]},
        {"b": [(0, SEC)]},
    ]
    calls = {"detect": 0, "stream": 0}

    def fake_detect(*_a: object, **_k: object) -> dict[str, list[tuple[int, int]]]:
        result = detect_results[calls["detect"]]
        calls["detect"] += 1
        return result

    def fake_stream(*_a: object, **_k: object) -> int:
        calls["stream"] += 1
        return 7

    monkeypatch.setattr(sync_mod, "_detect_missing", fake_detect)
    monkeypatch.setattr(sync_mod, "_stream_missing", fake_stream)

    report = sync_missing_channel_data(
        source, object(), dest, 0, SEC, ChannelSyncOptions(max_retries=1, settle_seconds=0)
    )

    # Initial detect + one re-detect per attempt (2 attempts) = 3 detect calls; 2 stream attempts.
    assert calls["detect"] == 3
    assert calls["stream"] == 2
    assert report.channels_missing == 2
    assert report.channels_synced == 1  # 'a' filled, 'b' not
    assert report.points_streamed == 14
    assert [s.channel for s in report.still_short] == ["b"]
    assert report.still_short[0].time_range == (0, SEC)
