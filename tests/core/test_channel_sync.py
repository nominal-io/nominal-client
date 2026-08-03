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

    points, _slices, _skipped = sync_mod._stream_file(stream, path, type_by_name, {"unit": "rpm"}, SEC)

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

    points, _slices, _skipped = sync_mod._stream_file(stream, path, type_by_name, None, SEC)

    assert points == 3
    values = stream.calls[0][2]
    assert values == [5, 7, 0.5]
    assert isinstance(values[0], int) and isinstance(values[1], int)
    assert isinstance(values[2], float)


def test_stream_file_does_not_crash_on_integral_then_float_column(tmp_path: Path) -> None:
    # A numeric column that looks integral for many rows before a float must not be inferred as i64
    # and crash the read (infer_schema_length=None scans the whole file). 'rpm' is overridden to
    # Float64; 'extra' is NOT in type_by_name (left to inference) yet must still not break the read.
    rows = "".join(f"{i * SEC},{i},{i}\n" for i in range(50)) + f"{50 * SEC},5,12.65\n"
    path = tmp_path / "part.csv.gz"
    _write_csv_gz(path, "timestamp,rpm,extra\n" + rows)
    stream = FakeStream()

    points, _slices, _skipped = sync_mod._stream_file(stream, path, {"rpm": ChannelDataType.DOUBLE}, None, SEC)

    # 51 rpm points stream; 'extra' (not a known channel) is ignored, but its float didn't crash.
    assert points == 51
    assert stream.calls[0][0] == "rpm"
    assert all(isinstance(v, float) for v in stream.calls[0][2])


def test_stream_file_tolerates_float_beyond_inference_sample(tmp_path: Path) -> None:
    # The header peek must not infer types: a column that stays integral well past polars' default
    # 100-row inference sample, then turns float, would otherwise infer i64 and crash the peek before
    # the type-forced main read runs (the real-data stall: most files failed to stream at the peek).
    n = 300
    rows = "".join(f"{i * SEC},{i}\n" for i in range(n)) + f"{n * SEC},0.5\n"
    path = tmp_path / "part.csv.gz"
    _write_csv_gz(path, "timestamp,rpm\n" + rows)
    stream = FakeStream()

    points, _slices, _skipped = sync_mod._stream_file(stream, path, {"rpm": ChannelDataType.DOUBLE}, None, SEC)

    assert points == n + 1
    assert stream.calls[0][2][-1] == 0.5  # the late float streamed as a float, no crash


def test_stream_file_identifies_timestamp_when_channel_named_timestamp(tmp_path: Path) -> None:
    # A data channel literally named "timestamp" forces the exporter to rename the time column.
    path = tmp_path / "part.csv.gz"
    _write_csv_gz(path, "timestamp.1,timestamp\n0,5\n1000000000,6\n")
    stream = FakeStream()
    type_by_name = {"timestamp": ChannelDataType.DOUBLE}

    points, _slices, _skipped = sync_mod._stream_file(stream, path, type_by_name, None, SEC)

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
        skip_rate_estimation: bool = False,
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

    def flaky_stream_file(*args: Any, **kwargs: Any) -> tuple[int, int]:
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


class HalvingFakeHandler:
    """export_to_files that fails for ranges wider than ``max_success_span`` (None = always fail).

    Records every (start, end) it was called with so tests can assert the recursive halving pattern.
    """

    def __init__(self, max_success_span: int | None) -> None:
        """``max_success_span``: widest range that succeeds (None = always fail)."""
        self.max_success_span = max_success_span
        self.calls: list[tuple[int, int]] = []

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
        skip_rate_estimation: bool = False,
    ) -> list[Path]:
        self.calls.append((start, end))
        if self.max_success_span is None or (end - start) > self.max_success_span:
            raise RuntimeError("export request too large")
        path = Path(out_dir) / f"{file_prefix}.csv.gz"
        _write_csv_gz(path, f"timestamp,c\n{start},1\n")
        if on_file_complete is not None:
            on_file_complete(path)
        return [path]


def test_export_and_stream_channel_recursively_halves_on_failure(tmp_path: Path) -> None:
    # Only single-bucket exports succeed -> the [0, 4h) range must halve down to four 1h exports.
    hour = 3600 * SEC
    handler = HalvingFakeHandler(max_success_span=hour)
    stream = FakeStream()
    options = ChannelSyncOptions(bucket=hour, output_dir=tmp_path)
    advanced: list[int] = []

    points = sync_mod._export_and_stream_channel(
        handler, stream, _channel("c"), 0, 4 * hour, {"c": ChannelDataType.DOUBLE}, options, advanced.append
    )

    assert points == 4  # four single-bucket files streamed (1 point each)
    assert sum(advanced) == 4  # one slice per successful bucket
    assert (0, 4 * hour) in handler.calls  # tried the whole range first
    succeeded = [(s, e) for s, e in handler.calls if e - s == hour]
    assert len(succeeded) == 4  # bottomed out at one-bucket exports


def test_export_and_stream_channel_gives_up_at_one_bucket(tmp_path: Path) -> None:
    # An export that fails even at one bucket must not recurse forever; it bottoms out and returns 0.
    hour = 3600 * SEC
    handler = HalvingFakeHandler(max_success_span=None)  # always fails
    options = ChannelSyncOptions(bucket=hour, output_dir=tmp_path)

    points = sync_mod._export_and_stream_channel(
        handler, FakeStream(), _channel("c"), 0, 2 * hour, {"c": ChannelDataType.DOUBLE}, options, None
    )

    assert points == 0
    # tried the full range, then each single bucket, then stopped (no sub-bucket splits)
    assert (0, 2 * hour) in handler.calls
    assert (0, hour) in handler.calls and (hour, 2 * hour) in handler.calls
    assert all(e - s >= hour for s, e in handler.calls)


def test_stream_missing_progress_total_and_advance_are_slices(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    hour = 3600 * SEC
    hour = 3600 * SEC

    class _RecordingProgress:
        def __init__(self) -> None:
            self.total = -1
            self.advanced = 0

        def advance(self, slices: int) -> None:
            self.advanced += slices

    recorder = _RecordingProgress()

    import contextlib

    @contextlib.contextmanager
    def fake_progress_bar(show: bool, total: int, description: str) -> Any:
        recorder.total = total
        yield recorder.advance

    monkeypatch.setattr(sync_mod, "_progress_bar", fake_progress_bar)

    # Two channels share one range [0, 2h) -> 1 group, 2 buckets -> 2 channels x 2 buckets = 4 slices.
    # The exported file carries a row in each bucket (t=0 and t=1h) so all 4 slices are covered.
    source_by_name = {"c1": _channel("c1"), "c2": _channel("c2")}
    missing = {"c1": [(0, 2 * hour)], "c2": [(0, 2 * hour)]}
    handler = FakeHandler([("f.csv.gz", f"timestamp,c1,c2\n0,1,2\n{hour},3,4\n")])
    dest = SimpleNamespace(get_write_stream=lambda batch_size: FakeStream())
    options = ChannelSyncOptions(bucket=hour, output_dir=tmp_path)

    sync_mod._stream_missing(handler, dest, missing, source_by_name, set(), options)

    assert recorder.total == 4
    assert recorder.advanced == 4  # per-file slices: 2 channels x 2 distinct buckets covered


class _PrefixRecordingHandler:
    """Records the file_prefix of each export_to_files call and writes one file named from it.

    Naming the on-disk file after file_prefix surfaces a collision two ways at once: a duplicated
    prefix shows up in ``prefixes`` and the second write silently overwrites the first file.
    """

    def __init__(self) -> None:
        """Initialize the recorded-prefix buffer."""
        self.prefixes: list[str] = []

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
        skip_rate_estimation: bool = False,
    ) -> list[Path]:
        """Record file_prefix, write a single file named from it, and fire on_file_complete."""
        self.prefixes.append(file_prefix)
        path = Path(out_dir) / f"{file_prefix}.csv.gz"
        _write_csv_gz(path, f"timestamp,{channels[0].name}\n{start},1\n")
        if on_file_complete is not None:
            on_file_complete(path)
        return [path]


def test_non_precise_channels_sharing_range_export_to_distinct_files(tmp_path: Path) -> None:
    """Regression (sync.py:526): two non-precise channels with an identical missing range must export
    to distinct file names so neither overwrites the other.

    Both channels take the per-channel recursive-halving fallback (``non_precise``), and both are short
    over exactly the same range. The file name must therefore carry a per-channel discriminator
    (``g{channel_idx:04d}``); without it both exports collide on a single ``sync_<start>_<end>`` file and
    one channel's data is silently lost.
    """
    hour = 3600 * SEC
    handler = _PrefixRecordingHandler()
    source_by_name = {"c1": _channel("c1"), "c2": _channel("c2")}
    missing = {"c1": [(0, hour)], "c2": [(0, hour)]}  # identical missing range
    non_precise = {"c1", "c2"}  # both routed to the per-channel fallback path
    dest = SimpleNamespace(get_write_stream=lambda batch_size: FakeStream())
    options = ChannelSyncOptions(bucket=hour, output_dir=tmp_path)  # output_dir set -> files are kept

    sync_mod._stream_missing(handler, dest, missing, source_by_name, non_precise, options)

    # Each channel exported once, with a distinct prefix -> no collision.
    assert len(handler.prefixes) == 2
    assert len(set(handler.prefixes)) == 2, f"file_prefix collision: {handler.prefixes}"
    # Both files persisted to distinct paths on disk (neither overwrote the other).
    assert len(list(tmp_path.glob("*.csv.gz"))) == 2


class _ThreadRecordingHandler(_PrefixRecordingHandler):
    """Records the thread name of each export_to_files call alongside the prefix."""

    def __init__(self) -> None:
        """Initialize the thread-name buffer."""
        super().__init__()
        self.thread_names: list[str] = []

    def export_to_files(self, *args: Any, **kwargs: Any) -> list[Path]:
        """Record the calling thread's name, then delegate to the prefix-recording base."""
        import threading

        self.thread_names.append(threading.current_thread().name)
        return super().export_to_files(*args, **kwargs)


def test_download_workers_fans_out_download_only_exports(tmp_path: Path) -> None:
    """With download_workers > 1 and download_only, every export runs on a pool thread, all ranges
    are exported exactly once, and file prefixes stay distinct (no collisions).
    """
    hour = 3600 * SEC
    handler = _ThreadRecordingHandler()
    source_by_name = {f"c{i}": _channel(f"c{i}") for i in range(4)}
    # Distinct ranges per channel -> 4 groups -> 4 independent export tasks.
    missing = {f"c{i}": [(i * hour, (i + 1) * hour)] for i in range(4)}
    options = ChannelSyncOptions(bucket=hour, output_dir=tmp_path, download_workers=4)

    sync_mod._stream_missing(handler, None, missing, source_by_name, set(), options, download_only=True)

    assert len(handler.prefixes) == 4
    assert len(set(handler.prefixes)) == 4, f"file_prefix collision: {handler.prefixes}"
    assert len(list(tmp_path.glob("*.csv.gz"))) == 4
    # Every export ran on a pool worker thread, never the main thread.
    assert all(name.startswith("sync-download") for name in handler.thread_names), handler.thread_names


def test_download_workers_ignored_when_streaming(tmp_path: Path) -> None:
    """The streaming path must stay serial (one shared write stream) even when download_workers > 1."""
    hour = 3600 * SEC
    handler = _ThreadRecordingHandler()
    source_by_name = {"c1": _channel("c1"), "c2": _channel("c2")}
    missing = {"c1": [(0, hour)], "c2": [(hour, 2 * hour)]}
    dest = SimpleNamespace(get_write_stream=lambda batch_size: FakeStream())
    options = ChannelSyncOptions(bucket=hour, output_dir=tmp_path, download_workers=4)

    sync_mod._stream_missing(handler, dest, missing, source_by_name, set(), options)

    assert len(handler.prefixes) == 2
    # No pool threads: everything ran inline on the calling thread.
    assert all(not name.startswith("sync-download") for name in handler.thread_names), handler.thread_names


class _FlakyHandler:
    """Fails export for any range wider than max_ok_span; writes one file per surviving call."""

    def __init__(self, max_ok_span: int) -> None:
        """Set the widest range that exports successfully."""
        self.max_ok_span = max_ok_span
        self.calls: list[tuple[int, int]] = []

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
        skip_rate_estimation: bool = False,
    ) -> list[Path]:
        """Raise for wide ranges (simulating a backend timeout); write a file otherwise."""
        self.calls.append((start, end))
        if end - start > self.max_ok_span:
            raise RuntimeError("Compute:TimeoutExceeded (simulated)")
        path = Path(out_dir) / f"{file_prefix}.csv.gz"
        _write_csv_gz(path, f"timestamp,{channels[0].name}\n{start},1\n")
        if on_file_complete is not None:
            on_file_complete(path)
        return [path]


def test_download_grouped_export_failure_halves_and_retries(tmp_path: Path) -> None:
    """Download-only grouped exports must halve-and-retry on failure (no re-detect loop exists to
    save them). A 4-bucket range failing wholesale converges to four single-bucket successes.
    """
    hour = 3600 * SEC
    handler = _FlakyHandler(max_ok_span=hour)  # only single-bucket exports succeed
    source_by_name = {"c1": _channel("c1"), "c2": _channel("c2")}
    missing = {"c1": [(0, 4 * hour)], "c2": [(0, 4 * hour)]}  # one group, one wide range
    options = ChannelSyncOptions(bucket=hour, output_dir=tmp_path, download_workers=4)

    sync_mod._stream_missing(handler, None, missing, source_by_name, set(), options, download_only=True)

    # The wide range failed, then halves failed, then all four single buckets succeeded.
    assert len(list(tmp_path.glob("*.csv.gz"))) == 4
    spans = sorted((e - s) for s, e in handler.calls)
    assert spans[-1] == 4 * hour  # the original wide attempt happened
    assert spans[:4] == [hour] * 4  # ...and bottomed out at per-bucket exports


def test_download_only_export_with_no_file_raises(tmp_path: Path) -> None:
    """An export that silently returns no file (transient planner miss) must raise in download-only:
    the range came from detection, so source data exists by construction.
    """

    class _NoFileHandler:
        def export_to_files(self, *args: Any, **kwargs: Any) -> list[Path]:
            return []

    options = ChannelSyncOptions(output_dir=tmp_path)
    with pytest.raises(RuntimeError, match="no file"):
        sync_mod._export_and_stream(_NoFileHandler(), None, [_channel("c1")], 0, SEC, {}, options, None)


def test_reconcile_downloaded_files_reports_uncovered_ranges(tmp_path: Path) -> None:
    """Reconciliation flags exactly the short buckets no downloaded file covers."""
    hour = 3600 * SEC
    # c1 short over [0, 4h); files cover [0, 1h) and [2h, 3h) -> holes at [1h, 2h) and [3h, 4h).
    _write_csv_gz(tmp_path / f"sync_0_{hour}_g0000_x.csv.gz", '"timestamp","c1"\n0,1\n')
    _write_csv_gz(tmp_path / f"sync_{2 * hour}_{3 * hour}_g0000_x.csv.gz", '"timestamp","c1"\n0,1\n')
    # c2 short over [0, 1h) and fully covered.
    _write_csv_gz(tmp_path / f"sync_0_{hour}_g0001_x.csv.gz", '"timestamp","c2"\n0,1\n')

    options = ChannelSyncOptions(bucket=hour, output_dir=tmp_path, tags={"stand": "1"})
    still = sync_mod._reconcile_downloaded_files({"c1": [(0, 4 * hour)], "c2": [(0, hour)]}, options)

    holes = {(s.channel, s.time_range) for s in still}
    assert holes == {("c1", (hour, 2 * hour)), ("c1", (3 * hour, 4 * hour))}
    assert all(s.tags == {"stand": "1"} for s in still)


# --- _stream_file: stream-time coverage gate ------------------------------------------------


def test_stream_file_gate_skips_covered_buckets(tmp_path: Path) -> None:
    """Rows in buckets the destination already fully covers (dest_count >= file rows) are skipped;
    everything else streams. Guards against re-appending data detection over-downloaded when the
    source held overlapping duplicate ingest sessions.
    """
    path = tmp_path / "part.csv.gz"
    # Bucket [0, 1s): 2 rows; bucket [1s, 2s): 2 rows.
    _write_csv_gz(path, f"timestamp,rpm\n0,1\n500000000,2\n{SEC},3\n{SEC + 500000000},4\n")
    stream = FakeStream()
    # Destination fully covers bucket 0 (2 >= 2) but only partially covers bucket 1 (1 < 2).
    dest_counts = {"rpm": {0: 2, SEC: 1}}

    points, _slices, skipped = sync_mod._stream_file(
        stream, path, {"rpm": ChannelDataType.DOUBLE}, None, SEC, dest_counts=dest_counts, grid_anchor=0
    )

    assert points == 2
    assert skipped == 2
    assert stream.calls[0][1] == [SEC, SEC + 500000000]  # only bucket-1 rows streamed
    assert stream.calls[0][2] == [3.0, 4.0]


def test_stream_file_gate_ignores_channels_without_dest_counts(tmp_path: Path) -> None:
    """A channel absent from dest_counts streams in full -- the gate never suppresses by default."""
    path = tmp_path / "part.csv.gz"
    _write_csv_gz(path, "timestamp,rpm\n0,1\n1000000000,2\n")
    stream = FakeStream()

    points, _slices, skipped = sync_mod._stream_file(
        stream, path, {"rpm": ChannelDataType.DOUBLE}, None, SEC, dest_counts={"other": {0: 99}}, grid_anchor=0
    )

    assert points == 2
    assert skipped == 0


def test_stream_file_gate_respects_grid_anchor(tmp_path: Path) -> None:
    """Bucket alignment follows grid_anchor, not epoch zero: a row at anchor+0.5s lands in the
    anchor bucket, so an anchor-keyed dest count gates it.
    """
    anchor = 7 * SEC  # grid offset (detection windows rarely start on epoch-aligned seconds)
    path = tmp_path / "part.csv.gz"
    _write_csv_gz(path, f"timestamp,rpm\n{anchor + 500000000},1\n")
    stream = FakeStream()

    _points, _slices, skipped = sync_mod._stream_file(
        stream,
        path,
        {"rpm": ChannelDataType.DOUBLE},
        None,
        SEC,
        dest_counts={"rpm": {anchor: 1}},
        grid_anchor=anchor,
    )

    assert skipped == 1
    assert not stream.calls


# --- _ingest_rid_snapshot -------------------------------------------------------------------


def test_ingest_rid_snapshot_collects_per_channel_rids() -> None:
    ch_a = SimpleNamespace(
        name="a", get_available_tags=lambda s, e: {"_nominal_ingest_rid": {"rid-1", "rid-2"}, "stand": {"1"}}
    )
    ch_b = SimpleNamespace(name="b", get_available_tags=lambda s, e: {"stand": {"1"}})

    def _boom(s: Any, e: Any) -> dict[str, set[str]]:
        raise RuntimeError("api down")

    ch_c = SimpleNamespace(name="c", get_available_tags=_boom)

    snapshot = sync_mod._ingest_rid_snapshot([ch_a, ch_b, ch_c], 0, SEC)

    assert snapshot["a"] == {"rid-1", "rid-2"}
    assert snapshot["b"] == set()
    assert "c" not in snapshot  # probe failure skips the channel rather than raising


def test_progress_bar_renders_nothing_when_total_is_zero() -> None:
    # Nothing to count (e.g. detecting against a freshly empty destination) -> no bar, not a phantom 1.
    with sync_mod._progress_bar(show=True, total=0, description="Counting destination channels") as advance:
        assert advance is None


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


# --- phase selection (plan / download / stream) -------------------------------------------


def test_sync_phase_plan_detects_only_and_records_ranges(monkeypatch: pytest.MonkeyPatch) -> None:
    source = SimpleNamespace(rid="src", search_channels=lambda: [_channel("a"), _channel("b")])
    dest = SimpleNamespace(rid="dst")

    monkeypatch.setattr(sync_mod, "count_channels", lambda *a, **k: {})
    monkeypatch.setattr(sync_mod, "_detect_missing", lambda *a, **k: {"a": [(0, SEC)], "b": [(SEC, 2 * SEC)]})
    # plan must not export or stream.
    monkeypatch.setattr(sync_mod, "PolarsExportHandler", lambda *a, **k: pytest.fail("plan must not export"))
    monkeypatch.setattr(sync_mod, "_stream_missing", lambda *a, **k: pytest.fail("plan must not stream"))

    report = sync_missing_channel_data(source, object(), dest, 0, 2 * SEC, ChannelSyncOptions(phase="plan"))

    assert report.channels_missing == 2
    assert report.planned_ranges == {"a": [(0, SEC)], "b": [(SEC, 2 * SEC)]}
    assert report.points_streamed == 0


def test_sync_phase_download_exports_without_streaming(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    source = SimpleNamespace(rid="src", search_channels=lambda: [_channel("rpm")])
    # A write stream must never be opened in download phase.
    dest = SimpleNamespace(rid="dst", get_write_stream=lambda **k: pytest.fail("download must not open a stream"))

    monkeypatch.setattr(sync_mod, "count_channels", lambda *a, **k: {})
    monkeypatch.setattr(sync_mod, "_detect_missing", lambda *a, **k: {"rpm": [(0, SEC)]})
    handler = FakeHandler([("rpm.csv.gz", "timestamp,rpm\n0,1\n1000000000,2\n")])
    monkeypatch.setattr(sync_mod, "PolarsExportHandler", lambda *a, **k: handler)

    report = sync_missing_channel_data(
        source, object(), dest, 0, SEC, ChannelSyncOptions(phase="download", output_dir=tmp_path)
    )

    # File was downloaded and kept; nothing was streamed.
    assert (tmp_path / "rpm.csv.gz").exists()
    assert report.points_streamed == 0
    assert report.channels_missing == 1


def test_sync_phase_stream_reads_dir_without_detecting(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _write_csv_gz(tmp_path / "part.csv.gz", "timestamp,rpm\n0,1\n1000000000,2\n")
    source = SimpleNamespace(rid="src", search_channels=lambda: [_channel("rpm")])
    stream = FakeStream()
    dest = SimpleNamespace(rid="dst", get_write_stream=lambda **k: stream)

    # stream phase must skip detection and export entirely.
    monkeypatch.setattr(sync_mod, "count_channels", lambda *a, **k: pytest.fail("stream must not detect"))
    monkeypatch.setattr(sync_mod, "_detect_missing", lambda *a, **k: pytest.fail("stream must not detect"))
    monkeypatch.setattr(sync_mod, "PolarsExportHandler", lambda *a, **k: pytest.fail("stream must not export"))

    report = sync_missing_channel_data(
        source, object(), dest, 0, SEC, ChannelSyncOptions(phase="stream", output_dir=tmp_path)
    )

    assert report.points_streamed == 2
    assert [c[0] for c in stream.calls] == ["rpm"]
    assert stream.calls[0][1] == [0, SEC]


@pytest.mark.parametrize("phase", ["download", "stream"])
def test_sync_phase_requires_output_dir(phase: str) -> None:
    source = SimpleNamespace(rid="src", search_channels=lambda: [_channel("a")])
    dest = SimpleNamespace(rid="dst")
    with pytest.raises(ValueError, match="requires output_dir"):
        sync_missing_channel_data(source, object(), dest, 0, SEC, ChannelSyncOptions(phase=phase))  # type: ignore[arg-type]


def test_stream_from_dir_streams_every_file(tmp_path: Path) -> None:
    _write_csv_gz(tmp_path / "a.csv.gz", "timestamp,rpm\n0,1\n")
    _write_csv_gz(tmp_path / "b.csv.gz", "timestamp,rpm\n1000000000,2\n")
    stream = FakeStream()
    dest = SimpleNamespace(get_write_stream=lambda **k: stream)

    points = sync_mod._stream_from_dir(dest, tmp_path, {"rpm": ChannelDataType.DOUBLE}, ChannelSyncOptions())

    assert points == 2
    assert sorted(c[1][0] for c in stream.calls) == [0, SEC]


def test_stream_file_measures_without_streaming_when_stream_is_none(tmp_path: Path) -> None:
    path = tmp_path / "part.csv.gz"
    _write_csv_gz(path, "timestamp,rpm\n0,1\n1000000000,2\n")
    # stream=None -> read + count, but never enqueue (download-only measurement path).
    points, slices, _skipped = sync_mod._stream_file(None, path, {"rpm": ChannelDataType.DOUBLE}, None, SEC)
    assert points == 2
    assert slices == 2


# --- _build_underconstrained_expansion -----------------------------------------------------------


def _fake_channel(name: str, get_available_tags_result: dict[str, set[str]] | None = None) -> Any:
    """Build a minimal channel stand-in for underconstrained expansion tests."""

    def _get_available_tags(
        *, start_time: Any = None, end_time: Any = None, initial_tags: Any = None
    ) -> dict[str, set[str]]:
        return get_available_tags_result or {}

    return SimpleNamespace(
        name=name,
        data_type=ChannelDataType.DOUBLE,
        _clients=object(),
        get_available_tags=_get_available_tags,
    )


def test_build_underconstrained_expansion_returns_original_when_none_underconstrained(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ch_a = _fake_channel("a")
    ch_b = _fake_channel("b")

    # Batch check reports both channels present, neither underconstrained.
    monkeypatch.setattr(sync_mod, "_batch_check_channels_have_data", lambda *a, **k: ([ch_a, ch_b], []))

    passes = sync_mod._build_underconstrained_expansion([ch_a, ch_b], {"stand": "1"}, 0, SEC)

    assert passes == [({"stand": "1"}, None, None)]


def test_build_underconstrained_expansion_splits_underconstrained_channel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # ch_a: fully constrained by {stand: "1"}.
    # ch_b: underconstrained — has an extra discriminating tag "ts" with values 1 and 2.
    ch_a = _fake_channel("a")
    ch_b = _fake_channel("b", get_available_tags_result={"stand": {"1"}, "ts": {"1", "2"}})

    def fake_batch_check(clients: Any, batch: Any, *a: Any, **k: Any) -> tuple[list[Any], list[str]]:
        all_channels = list(batch)
        underconstrained = [ch.name for ch in all_channels if ch.name == "b"]
        return all_channels, underconstrained

    monkeypatch.setattr(sync_mod, "_batch_check_channels_have_data", fake_batch_check)

    passes = sync_mod._build_underconstrained_expansion([ch_a, ch_b], {"stand": "1"}, 0, SEC)

    # Should produce 3 passes:
    #   1. {stand: "1"}        — allowlist={a}   (fully constrained)
    #   2. {stand: "1", ts: "1"} — allowlist={b} (expansion combo)
    #   3. {stand: "1", ts: "2"} — allowlist={b} (expansion combo)
    assert len(passes) == 3
    tags_list = [p[0] for p in passes]
    allowlist_list = [p[1] for p in passes]

    assert {"stand": "1"} in tags_list
    assert {"stand": "1", "ts": "1"} in tags_list
    assert {"stand": "1", "ts": "2"} in tags_list

    original_idx = tags_list.index({"stand": "1"})
    assert allowlist_list[original_idx] == frozenset({"a"})

    ts1_idx = tags_list.index({"stand": "1", "ts": "1"})
    assert allowlist_list[ts1_idx] == frozenset({"b"})

    ts2_idx = tags_list.index({"stand": "1", "ts": "2"})
    assert allowlist_list[ts2_idx] == frozenset({"b"})


def test_build_underconstrained_expansion_nominal_only_uses_canonical_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # ch_a: fully constrained by {stand: "1"}.
    # ch_b: underconstrained only by _nominal_ingest_rid — multiple ingest sessions.
    # Expected: both ch_a and ch_b are merged into a single canonical-filter pass.
    # Per-RID expansion is intentionally skipped: it misses data from sessions whose RIDs
    # were truncated by get_available_tags (value-count limit) or that predate auto-tagging.
    # The canonical filter captures all data regardless of tagging era.
    ch_a = _fake_channel("a")
    ch_b = _fake_channel(
        "b",
        get_available_tags_result={"stand": {"1"}, "_nominal_ingest_rid": {"session-1", "session-2"}},
    )

    def fake_batch_check(clients: Any, batch: Any, *a: Any, **k: Any) -> tuple[list[Any], list[str]]:
        all_ch = list(batch)
        underconstrained = [ch.name for ch in all_ch if ch.name == "b"]
        return all_ch, underconstrained

    monkeypatch.setattr(sync_mod, "_batch_check_channels_have_data", fake_batch_check)

    passes = sync_mod._build_underconstrained_expansion([ch_a, ch_b], {"stand": "1"}, 0, SEC)

    # 1 pass: canonical filter for both ch_a and ch_b (ch_b routes through canonical, not per-RID).
    assert len(passes) == 1
    export_tags, allowlist, dir_tags = passes[0]
    assert export_tags == {"stand": "1"}
    assert allowlist == frozenset({"a", "b"})
    assert dir_tags is None

    # No per-RID expansion passes.
    rid_passes = [p for p in passes if "_nominal_ingest_rid" in p[0]]
    assert len(rid_passes) == 0


def test_build_underconstrained_expansion_all_underconstrained_omits_original_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Only channel b exists and it's underconstrained — no original-filter pass should be emitted.
    ch_b = _fake_channel("b", get_available_tags_result={"stand": {"1"}, "ts": {"1", "2"}})

    monkeypatch.setattr(sync_mod, "_batch_check_channels_have_data", lambda *a, **k: ([ch_b], ["b"]))

    passes = sync_mod._build_underconstrained_expansion([ch_b], {"stand": "1"}, 0, SEC)

    tags_list = [p[0] for p in passes]
    assert {"stand": "1"} not in tags_list  # original omitted
    assert {"stand": "1", "ts": "1"} in tags_list
    assert {"stand": "1", "ts": "2"} in tags_list


def test_build_underconstrained_expansion_user_visible_disabled_routes_to_original(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # expand_user_visible=False: user-visible underconstrained channels fall back to the original
    # filter instead of being split, while _nominal_*-only channels still get per-RID passes.
    ch_a = _fake_channel("a")
    ch_b = _fake_channel("b", get_available_tags_result={"stand": {"1"}, "ts": {"1", "2"}})
    ch_c = _fake_channel(
        "c",
        get_available_tags_result={"stand": {"1"}, "_nominal_ingest_rid": {"rid-1", "rid-2"}},
    )

    def fake_batch_check(clients: Any, batch: Any, *a: Any, **k: Any) -> tuple[list[Any], list[str]]:
        all_ch = list(batch)
        underconstrained = [ch.name for ch in all_ch if ch.name in ("b", "c")]
        return all_ch, underconstrained

    monkeypatch.setattr(sync_mod, "_batch_check_channels_have_data", fake_batch_check)

    passes = sync_mod._build_underconstrained_expansion(
        [ch_a, ch_b, ch_c], {"stand": "1"}, 0, SEC, expand_user_visible=False
    )

    tags_list = [p[0] for p in passes]
    allowlist_list = [p[1] for p in passes]
    dir_tags_list = [p[2] for p in passes]

    # All three channels route through the canonical filter pass.
    # ch_c was previously expanded per-RID, but canonical is now used to capture data
    # from sessions truncated by get_available_tags or predating auto-tagging.
    assert len(passes) == 1
    assert tags_list[0] == {"stand": "1"}
    assert allowlist_list[0] == frozenset({"a", "b", "c"})
    assert dir_tags_list[0] is None

    # No per-RID expansion passes.
    rid_passes = [p for p in passes if "_nominal_ingest_rid" in p[0]]
    assert len(rid_passes) == 0


# --- channel_allowlist in sync_missing_channel_data -------------------------------------------


def test_detect_missing_strips_nominal_tags_for_destination(monkeypatch: pytest.MonkeyPatch) -> None:
    # Source is counted with the full filter including _nominal_ingest_rid.
    # Destination must be counted with only canonical tags — _nominal_* tags are internal to
    # source ingest sessions and will never appear on data written by the sync tool.
    from nominal.experimental.migration.channel_sync.sync import ChannelBucketCounts, _detect_missing

    ch = _channel("temp")
    dest = SimpleNamespace(
        rid="dst",
        search_channels=lambda: [ch],
    )

    captured_dest_tags: list[Any] = []
    captured_fallback: list[Any] = []

    def fake_count(channels: Any, start: Any, end: Any, bucket: Any, tags: Any, **k: Any) -> dict[str, Any]:
        if channels and channels[0].name == "temp":
            captured_dest_tags.append(tags)
            captured_fallback.append(k.get("presence_fallback"))
        return {"temp": ChannelBucketCounts("temp", {0: 5}, True)}

    monkeypatch.setattr(sync_mod, "count_channels", fake_count)

    source_counts = {"temp": ChannelBucketCounts("temp", {0: 5}, True)}
    options = ChannelSyncOptions(tags={"stand": "1", "_nominal_ingest_rid": "ri.ingest.main.streaming-session.abc123"})
    _detect_missing(source_counts, dest, 0, SEC, options)

    assert len(captured_dest_tags) == 1
    dest_tags = captured_dest_tags[0]
    assert "_nominal_ingest_rid" not in (dest_tags or {})
    assert (dest_tags or {}).get("stand") == "1"
    # Destination counting must treat imprecisely-countable channels as empty (re-download) rather
    # than presence-probed (any live-ingest data would read as full coverage and mask real gaps).
    assert captured_fallback == ["empty"]


def test_channel_allowlist_restricts_channels_processed(monkeypatch: pytest.MonkeyPatch) -> None:
    # Dataset has channels a, b, c.  allowlist={a} should cause only a to be examined.
    source = SimpleNamespace(rid="src", search_channels=lambda: [_channel("a"), _channel("b"), _channel("c")])
    dest = SimpleNamespace(rid="dst")

    examined: list[str] = []

    def fake_count(channels: Any, *a: Any, **k: Any) -> dict[str, Any]:
        examined.extend(ch.name for ch in channels)
        return {}

    monkeypatch.setattr(sync_mod, "count_channels", fake_count)
    monkeypatch.setattr(sync_mod, "_detect_missing", lambda *a, **k: {})

    from nominal.experimental.migration.channel_sync import ChannelSyncOptions, sync_missing_channel_data

    report = sync_missing_channel_data(
        source, object(), dest, 0, SEC, ChannelSyncOptions(channel_allowlist=frozenset({"a"}))
    )

    assert report.channels_examined == 1
    assert examined == ["a"]
