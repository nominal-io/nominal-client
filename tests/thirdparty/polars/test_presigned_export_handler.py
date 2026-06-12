from __future__ import annotations

import datetime
import pathlib
from types import SimpleNamespace
from typing import Callable, Mapping, Sequence, cast
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import requests

from nominal.core._utils.multipart_downloader import DownloadItem, DownloadResults
from nominal.core.channel import Channel, ChannelDataType
from nominal.thirdparty.polars.export_handler import _ExportJob, _TimeRange
from nominal.thirdparty.polars.presigned_export_handler import PresignedExportHandler

_DOWNLOADER_PATH = "nominal.thirdparty.polars.presigned_export_handler.MultipartFileDownloader"
_BUCKETS_PATH = "nominal.thirdparty.polars.presigned_export_handler._channel_data_buckets"
_ENUM_BUCKETS_PATH = "nominal.thirdparty.polars.presigned_export_handler._channel_enum_buckets"


def _handler() -> PresignedExportHandler:
    return PresignedExportHandler(MagicMock())


def _ch(name: str) -> Channel:
    return cast(Channel, SimpleNamespace(name=name, data_source="ds1", data_type=ChannelDataType.DOUBLE))


def _enum_ch(name: str) -> Channel:
    return cast(Channel, SimpleNamespace(name=name, data_source="ds1", data_type=ChannelDataType.STRING))


def _bucket(timestamp: int, count: int) -> object:
    return SimpleNamespace(timestamp=timestamp, count=count)


def _enum_bucket(timestamp: int) -> object:
    return SimpleNamespace(timestamp=timestamp, frequencies={"value": 1})


def _job(datasource_rid: str, channel_names: list[str]) -> _ExportJob:
    return _ExportJob(
        datasource_rid=datasource_rid,
        channel_names=channel_names,
        channel_types={name: ChannelDataType.DOUBLE for name in channel_names},
        time_slice=_TimeRange(0, 1_000_000_000),
        tags={},
    )


def _echo_download() -> Callable[..., DownloadResults]:
    """A download_files_pipelined stand-in: 'succeeds' every item and fires both callbacks."""

    def _dl(
        items: Sequence[DownloadItem],
        *,
        on_file_planned: Callable[[pathlib.Path], None] | None = None,
        on_file_complete: Callable[[pathlib.Path], None] | None = None,
    ) -> DownloadResults:
        for item in items:
            if on_file_planned is not None:
                on_file_planned(item.destination)
            if on_file_complete is not None:
                on_file_complete(item.destination)
        return DownloadResults(succeeded=[item.destination for item in items], failed={})

    return _dl


# ---- export() ----


def test_export_returns_sorted_paths_and_wires_progress_callback(tmp_path: pathlib.Path) -> None:
    """A successful export returns sorted written paths and passes an on_file_complete callback."""
    handler = _handler()
    jobs: Mapping[_TimeRange, Sequence[_ExportJob]] = {
        _TimeRange(0, 1_000_000_000): [_job("ri.a.b.datasource.ds1", ["a"]), _job("ri.a.b.datasource.ds1", ["b"])]
    }

    with (
        patch.object(handler, "_compute_export_jobs", return_value=jobs),
        patch(_DOWNLOADER_PATH) as mock_dl_cls,
    ):
        downloader = mock_dl_cls.create.return_value.__enter__.return_value
        downloader.download_files_pipelined.side_effect = _echo_download()
        paths = handler.export([MagicMock()], 0, 1_000_000_000, tmp_path)

    assert paths == sorted([tmp_path / "export_ds1_s0000_g000.csv", tmp_path / "export_ds1_s0000_g001.csv"])
    assert tmp_path.exists()
    kwargs = downloader.download_files_pipelined.call_args.kwargs
    assert callable(kwargs["on_file_planned"])
    assert callable(kwargs["on_file_complete"])


def test_export_raises_on_download_failure(tmp_path: pathlib.Path) -> None:
    """Any failed file surfaces as a RuntimeError naming the failure count."""
    handler = _handler()
    jobs: Mapping[_TimeRange, Sequence[_ExportJob]] = {
        _TimeRange(0, 1_000_000_000): [_job("ri.a.b.datasource.ds1", ["a"])]
    }
    failed_path = tmp_path / "export_ds1_s0000_g000.csv"

    with (
        patch.object(handler, "_compute_export_jobs", return_value=jobs),
        patch(_DOWNLOADER_PATH) as mock_dl_cls,
    ):
        downloader = mock_dl_cls.create.return_value.__enter__.return_value
        downloader.download_files_pipelined.return_value = DownloadResults(
            succeeded=[], failed={failed_path: RuntimeError("boom")}
        )
        with pytest.raises(RuntimeError, match="Failed to export 1 of 1 file"):
            handler.export([MagicMock()], 0, 1_000_000_000, tmp_path)


def test_export_empty_channels_short_circuits(tmp_path: pathlib.Path) -> None:
    """No channels returns an empty list without touching the downloader."""
    handler = _handler()
    with patch(_DOWNLOADER_PATH) as mock_dl_cls:
        assert handler.export([], 0, 1_000_000_000, tmp_path) == []
        mock_dl_cls.create.assert_not_called()


def test_export_rejects_buckets_and_resolution(tmp_path: pathlib.Path) -> None:
    """Requesting both decimation modes is rejected."""
    handler = _handler()
    with pytest.raises(ValueError, match="both buckets and resolution"):
        handler.export([MagicMock()], 0, 1_000_000_000, tmp_path, buckets=10, resolution=1_000)


def test_build_download_items_names_one_file_per_job(tmp_path: pathlib.Path) -> None:
    """Each (slice, job) maps to one DownloadItem with a deterministic, ordered file name."""
    handler = _handler()
    jobs: Mapping[_TimeRange, Sequence[_ExportJob]] = {
        _TimeRange(0, 1_000_000_000): [_job("ri.a.b.datasource.ds1", ["a"]), _job("ri.a.b.datasource.ds1", ["b"])]
    }

    items = handler._build_download_items(jobs, tmp_path, "export")

    assert [item.destination.name for item in items] == [
        "export_ds1_s0000_g000.csv",
        "export_ds1_s0000_g001.csv",
    ]


# ---- _compute_export_jobs pruning ----


def test_compute_export_jobs_prunes_channels_absent_from_a_slice() -> None:
    """A numeric channel is only exported for slices where its buckets show data."""
    handler = _handler()
    a, b = _ch("a"), _ch("b")
    # a has data only in the first 1s slice; b only in the second.
    buckets = {"a": [_bucket(500_000_000, 10)], "b": [_bucket(1_500_000_000, 10)]}

    with patch(_BUCKETS_PATH, return_value=buckets):
        jobs = handler._compute_export_jobs(
            [a, b],
            _TimeRange(0, 2_000_000_000),
            "epoch_seconds",
            batch_duration=datetime.timedelta(seconds=1),  # -> two 1s slices
        )

    by_slice = {(slc.start_time, slc.end_time): [j.channel_names for j in js] for slc, js in jobs.items()}
    assert by_slice[(0, 1_000_000_000)] == [["a"]]
    assert by_slice[(1_000_000_000, 2_000_000_000)] == [["b"]]


def test_compute_export_jobs_prunes_enum_channels_absent_from_a_slice() -> None:
    """Enum (string) channels are also pruned per slice using enum bucket presence."""
    handler = _handler()
    e1, e2 = _enum_ch("e1"), _enum_ch("e2")
    enum_buckets = ({"e1": [_enum_bucket(500_000_000)], "e2": [_enum_bucket(1_500_000_000)]}, set())

    with patch(_ENUM_BUCKETS_PATH, return_value=enum_buckets), patch(_BUCKETS_PATH, return_value={}):
        jobs = handler._compute_export_jobs(
            [e1, e2],
            _TimeRange(0, 2_000_000_000),
            "epoch_seconds",
            batch_duration=datetime.timedelta(seconds=1),
        )

    by_slice = {(slc.start_time, slc.end_time): [j.channel_names for j in js] for slc, js in jobs.items()}
    assert by_slice[(0, 1_000_000_000)] == [["e1"]]
    assert by_slice[(1_000_000_000, 2_000_000_000)] == [["e2"]]


def test_compute_export_jobs_exports_undetermined_enum_in_all_slices() -> None:
    """Enum channels whose presence couldn't be computed are exported for every slice (not dropped)."""
    handler = _handler()
    e = _enum_ch("e")
    # No presence data, but flagged undetermined (e.g. Compute:TooManyCategories).
    with patch(_ENUM_BUCKETS_PATH, return_value=({}, {"e"})), patch(_BUCKETS_PATH, return_value={}):
        jobs = handler._compute_export_jobs(
            [e], _TimeRange(0, 2_000_000_000), "epoch_seconds", batch_duration=datetime.timedelta(seconds=1)
        )

    by_slice = {(slc.start_time, slc.end_time): [j.channel_names for j in js] for slc, js in jobs.items()}
    assert by_slice[(0, 1_000_000_000)] == [["e"]]
    assert by_slice[(1_000_000_000, 2_000_000_000)] == [["e"]]


def test_compute_export_jobs_skips_empty_channels_entirely() -> None:
    """Channels with no data anywhere produce no jobs."""
    handler = _handler()
    a, b = _ch("a"), _ch("b")

    with patch(_BUCKETS_PATH, return_value={"a": [_bucket(500_000_000, 10)]}):  # b absent -> no data
        jobs = handler._compute_export_jobs(
            [a, b], _TimeRange(0, 1_000_000_000), "epoch_seconds", batch_duration=datetime.timedelta(seconds=1)
        )

    all_names = [name for js in jobs.values() for j in js for name in j.channel_names]
    assert all_names == ["a"]


# ---- presigned link retry ----


def _http_error(status: int) -> requests.HTTPError:
    return requests.HTTPError(response=cast(requests.Response, SimpleNamespace(status_code=status)))


def test_generate_presigned_link_retries_transient_5xx() -> None:
    """A transient 5xx on link generation is retried and then succeeds."""
    handler = _handler()
    ok = SimpleNamespace(presigned_url=SimpleNamespace(url="https://s3/obj"), file_size_bytes=10)
    dataexport = handler._client._clients.dataexport
    dataexport.generate_export_channel_data_presigned_link.side_effect = [_http_error(500), ok]

    with patch("nominal.thirdparty.polars.presigned_export_handler.time.sleep"):
        result = handler._generate_presigned_link(MagicMock())

    assert result is ok
    assert dataexport.generate_export_channel_data_presigned_link.call_count == 2


def test_generate_presigned_link_raises_immediately_on_4xx() -> None:
    """A non-transient 4xx is raised without retrying."""
    handler = _handler()
    dataexport = handler._client._clients.dataexport
    dataexport.generate_export_channel_data_presigned_link.side_effect = _http_error(400)

    with patch("nominal.thirdparty.polars.presigned_export_handler.time.sleep"), pytest.raises(Exception):
        handler._generate_presigned_link(MagicMock())

    assert dataexport.generate_export_channel_data_presigned_link.call_count == 1


def test_generate_presigned_link_gives_up_after_max_retries() -> None:
    """Persistent transient errors raise after exhausting retries."""
    handler = PresignedExportHandler(MagicMock(), max_link_retries=3)
    dataexport = handler._client._clients.dataexport
    dataexport.generate_export_channel_data_presigned_link.side_effect = _http_error(503)

    with patch("nominal.thirdparty.polars.presigned_export_handler.time.sleep"), pytest.raises(Exception):
        handler._generate_presigned_link(MagicMock())

    assert dataexport.generate_export_channel_data_presigned_link.call_count == 3


# ---- readiness wait ----


def test_wait_until_materialized_polls_until_full_size() -> None:
    """The readiness wait polls the object size until it reaches the authoritative file_size_bytes."""
    handler = _handler()
    with (
        patch.object(handler, "_served_size", side_effect=[100, 100, 1000]) as served,
        patch("nominal.thirdparty.polars.presigned_export_handler.time.sleep"),
    ):
        handler._wait_until_materialized("https://s3/obj", expected_size=1000)
    assert served.call_count == 3


def test_wait_until_materialized_gives_up_after_timeout() -> None:
    """If the object never reaches the expected size, the wait returns (proceeds) instead of hanging."""
    handler = PresignedExportHandler(MagicMock(), readiness_timeout=0.0)
    with (
        patch.object(handler, "_served_size", return_value=100) as served,
        patch("nominal.thirdparty.polars.presigned_export_handler.time.sleep"),
    ):
        handler._wait_until_materialized("https://s3/obj", expected_size=1000)
    assert served.call_count == 1  # one probe, then deadline (timeout=0) is hit


# ---- merge() ----


def test_merge_concats_same_schema_and_outer_joins_differing(tmp_path: pathlib.Path) -> None:
    """Same-schema files are vertically concatenated; differing schemas are outer-joined on timestamp."""
    (tmp_path / "f0.csv").write_text("timestamp,a\n0,1\n1,2\n")
    (tmp_path / "f1.csv").write_text("timestamp,a\n2,3\n")  # same schema as f0 -> vertical concat
    (tmp_path / "f2.csv").write_text("timestamp,b\n0,10\n1,11\n")  # differing schema -> outer join

    merged = PresignedExportHandler.merge([tmp_path / "f0.csv", tmp_path / "f1.csv", tmp_path / "f2.csv"]).collect()

    assert set(merged.columns) == {"timestamp", "a", "b"}
    assert merged["timestamp"].to_list() == [0, 1, 2]
    assert merged["a"].to_list() == [1, 2, 3]
    assert merged["b"].to_list() == [10, 11, None]


def test_merge_handles_integer_then_float_columns(tmp_path: pathlib.Path) -> None:
    """A numeric column that looks integer for many rows then turns float must not mis-infer as i64."""
    # Column 'a' is integer for the first rows, float later (and split across two same-schema files),
    # which trips polars' sampled schema inference unless we infer over the full file.
    header = "timestamp,a\n"
    early = "".join(f"{i}.0,0\n" for i in range(200))
    (tmp_path / "f0.csv").write_text(header + early)
    (tmp_path / "f1.csv").write_text(header + "200.0,0.14173234\n201.0,1\n")

    merged = PresignedExportHandler.merge([tmp_path / "f0.csv", tmp_path / "f1.csv"]).collect()

    assert merged.schema["a"] == pl.Float64
    assert merged.height == 202
    assert merged["timestamp"].to_list()[:2] == [0.0, 1.0]


def test_merge_handles_channel_in_different_groupings_across_slices(tmp_path: pathlib.Path) -> None:
    """A channel that appears in different column groupings across slices must coalesce, not collide."""
    # slice 0 grouped [a, b]; slice 1 grouped [a, c] (a regrouped). Overlapping on 'a', different
    # timestamps. The old outer-join-on-timestamp produced an 'a_right' duplicate column and failed.
    (tmp_path / "s0_g0.csv").write_text("timestamp,a,b\n0,1,10\n")
    (tmp_path / "s1_g0.csv").write_text("timestamp,a,c\n1,2,20\n")

    merged = PresignedExportHandler.merge([tmp_path / "s0_g0.csv", tmp_path / "s1_g0.csv"]).collect()

    assert set(merged.columns) == {"timestamp", "a", "b", "c"}
    assert merged["timestamp"].to_list() == [0, 1]
    assert merged["a"].to_list() == [1, 2]
    assert merged["b"].to_list() == [10, None]
    assert merged["c"].to_list() == [None, 20]


def test_merge_coalesces_column_fragments_within_a_slice(tmp_path: pathlib.Path) -> None:
    """Files sharing timestamps but holding different channels collapse into one row per timestamp."""
    (tmp_path / "g0.csv").write_text("timestamp,a\n0,1\n1,2\n")
    (tmp_path / "g1.csv").write_text("timestamp,b\n0,10\n1,11\n")

    merged = PresignedExportHandler.merge([tmp_path / "g0.csv", tmp_path / "g1.csv"]).collect()

    assert set(merged.columns) == {"timestamp", "a", "b"}
    assert merged["timestamp"].to_list() == [0, 1]
    assert merged["a"].to_list() == [1, 2]
    assert merged["b"].to_list() == [10, 11]


def test_merge_skips_header_only_files(tmp_path: pathlib.Path) -> None:
    """Header-only files (no data rows, str-typed timestamp) are skipped instead of breaking the join."""
    (tmp_path / "data.csv").write_text("timestamp,a\n0,1\n1,2\n")
    (tmp_path / "empty_same.csv").write_text("timestamp,a\n")  # header only, same schema
    (tmp_path / "empty_diff.csv").write_text("timestamp,b\n")  # header only, differing schema

    merged = PresignedExportHandler.merge(
        [tmp_path / "data.csv", tmp_path / "empty_same.csv", tmp_path / "empty_diff.csv"]
    ).collect()

    assert merged.columns == ["timestamp", "a"]
    assert merged["timestamp"].to_list() == [0, 1]
    assert merged["a"].to_list() == [1, 2]


def test_merge_raises_when_all_files_empty(tmp_path: pathlib.Path) -> None:
    """If every file is header-only there is nothing to merge."""
    (tmp_path / "e1.csv").write_text("timestamp,a\n")
    (tmp_path / "e2.csv").write_text("timestamp,b\n")
    with pytest.raises(ValueError, match="No data rows found"):
        PresignedExportHandler.merge([tmp_path / "e1.csv", tmp_path / "e2.csv"])


def test_merge_raises_on_missing_timestamp_column(tmp_path: pathlib.Path) -> None:
    """A file without the timestamp column cannot be merged."""
    (tmp_path / "bad.csv").write_text("x,y\n1,2\n")
    with pytest.raises(ValueError, match="no 'timestamp' column"):
        PresignedExportHandler.merge([tmp_path / "bad.csv"])


def test_merge_raises_on_empty_paths() -> None:
    """Merging nothing is an error."""
    with pytest.raises(ValueError, match="No paths provided"):
        PresignedExportHandler.merge([])
