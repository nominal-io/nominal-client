from __future__ import annotations

import datetime
import gzip
import io
import json
from concurrent.futures import Future
from contextlib import contextmanager
from unittest.mock import MagicMock

import polars as pl
import pytest

from nominal.core.bounds import Bounds
from nominal.core.channel import ChannelDataType
from nominal.protos.compute.v1 import (
    categorical_results_pb2,
    execution_results_pb2,
    numeric_results_pb2,
    result_common_pb2,
)
from nominal.protos.exportjobs.v1 import export_jobs_pb2 as jobs
from nominal.protos.types.time import time_pb2
from nominal.thirdparty.polars._point_counts import _extract_bucket_counts, _peak_points_per_second
from nominal.thirdparty.polars.polars_export_handler import (
    PolarsExportHandler,
    _ExportJob,
)
from nominal.ts import Relative

TEN_SECONDS_NS = 10_000_000_000
TWENTY_SECONDS_NS = 20_000_000_000


def test_extract_numeric_bucketed(make_numeric_response):
    """Bucketed numeric response yields (timestamp_ns, count) pairs preserving bucket order."""
    result = _extract_bucket_counts(make_numeric_response([50, 75]))

    assert result == [(0, 50), (TEN_SECONDS_NS, 75)]


def test_extract_numeric_undecimated():
    """Undecimated numeric series yields count=1 per timestamp."""
    response = execution_results_pb2.ComputationResult(
        numeric_samples=numeric_results_pb2.NumericSamples(
            samples=[result_common_pb2.NumericSample(timestamp=time_pb2.Timestamp(seconds=s)) for s in (100, 200, 300)]
        )
    )

    result = _extract_bucket_counts(response)

    assert len(result) == 3
    assert all(count == 1 for _, count in result)


def test_extract_numeric_single_point():
    """A single numeric point yields one entry with count=1."""
    response = execution_results_pb2.ComputationResult(
        numeric_sample=execution_results_pb2.OptionalNumericSampleResult(
            sample=result_common_pb2.NumericSample(timestamp=time_pb2.Timestamp(seconds=100))
        )
    )

    assert _extract_bucket_counts(response) == [(100_000_000_000, 1)]


def test_extract_enum_bucketed(make_enum_response):
    """Bucketed enum response sums histogram frequencies per bucket."""
    result = _extract_bucket_counts(make_enum_response([{0: 30, 1: 20}, {0: 10}]))

    assert result == [(0, 50), (TEN_SECONDS_NS, 10)]


def test_extract_enum_undecimated():
    """Undecimated enum series yields count=1 per timestamp."""
    response = execution_results_pb2.ComputationResult(
        categorical_samples=categorical_results_pb2.CategoricalSamples(
            samples=[
                categorical_results_pb2.CategoricalCodeSample(timestamp=time_pb2.Timestamp(seconds=s))
                for s in (100, 200)
            ]
        )
    )

    result = _extract_bucket_counts(response)

    assert len(result) == 2
    assert all(count == 1 for _, count in result)


def test_extract_unrecognized_response_returns_unknown():
    """An unrecognized result gives the planner an unknown rate."""
    response = execution_results_pb2.ComputationResult()

    assert _extract_bucket_counts(response) is None


@pytest.mark.parametrize(
    ("buckets", "end", "expected"),
    [
        ([], 10, 0),
        ([(5, 100)], 10, 10),
        ([(5, 100)], 0, 0),
        ([(1, 10), (2, 100), (3, 20)], 3, 100),
        ([(1, 10), (1, 50), (2, 20)], 2, 40),
        ([(1, 23), (2, 0)], 2, 23),
    ],
)
def test_peak_points_per_second(buckets, end, expected):
    assert _peak_points_per_second([(s * 10**9, n) for s, n in buckets], 0, end * 10**9) == expected


def set_storage_types(mock_client, rows):
    mock_client._clients.resolve_default_workspace_rid.return_value = "workspace"
    payload = "\n".join(json.dumps({"channel": name, "data_type": kind}) for name, kind in rows).encode()
    mock_client._clients.sql.Query.return_value = [MagicMock(payload=payload)]


@pytest.mark.parametrize(
    ("rates", "budget", "cap", "expected"),
    [
        ([100, 100, 50], 250, 3, [([0, 1, 2], None)]),
        ([200] * 5, 500, 5, [([0, 1], None), ([2, 3], None), ([4], None)]),
        ([10_000, 100], 500, 2, [([0], 50_000_000), ([1], None)]),
        ([1, 0, 0], 1000, 2, [([0, 1], None), ([2], None)]),
        ([], 1000, 2, []),
    ],
)
def test_channel_groups_respect_both_budgets(mock_client, make_channel, rates, budget, cap, expected):
    channels = [make_channel(str(i)) for i in range(len(rates))]
    handler = PolarsExportHandler(mock_client, points_per_request=budget, channels_per_request=cap)
    groups = handler._channel_groups(
        channels, {(ch.data_source, ch.name): rate for ch, rate in zip(channels, rates)}, 10**9
    )
    assert [([int(ch.name) for ch in group], step) for group, step in groups] == expected


def test_auto_sizing_keeps_channels_with_failed_rates(
    mock_client, mock_clients, make_channel, make_compute_result, make_numeric_response, make_series_count_response
):
    """Confirmed channels survive a failed estimate, while successful zero-rate results are excluded."""
    channels = [
        make_channel("known", ChannelDataType.DOUBLE),
        make_channel("none_rate", ChannelDataType.DOUBLE),
        make_channel("zero_rate", ChannelDataType.DOUBLE),
    ]
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1, 1, 1])
    mock_client._clients.compute.BatchComputeWithUnits.return_value = MagicMock(
        results=[
            make_compute_result(success=make_numeric_response([50, 100])),
            make_compute_result(error="compute failed"),
            make_compute_result(success=make_numeric_response([])),
        ]
    )

    handler = PolarsExportHandler(client=mock_client)
    jobs = handler._compute_export_jobs(channels, Bounds(0, TEN_SECONDS_NS))

    exported_names = {ch.name for job_list in jobs for job in job_list for ch in job.channels}
    assert exported_names == {"known", "none_rate"}


@pytest.mark.parametrize("failure", ["unavailable", "missing", "malformed"])
def test_explicit_point_budget_fails_before_export_when_counts_are_unknown(
    mock_client, mock_clients, make_channel, make_compute_result, make_series_count_response, failure
):
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1])
    sample = mock_client._clients.compute.BatchComputeWithUnits
    if failure == "unavailable":
        sample.side_effect = RuntimeError("service unavailable")
    else:
        sample.return_value.results = [] if failure == "missing" else [make_compute_result()]
    handler = PolarsExportHandler(mock_client, points_per_request=10)
    with pytest.raises(RuntimeError, match="Could not size export jobs for 1 channels"):
        next(handler.export([make_channel("signal")], 0, TEN_SECONDS_NS))
    mock_client._clients.export_jobs.CreateExportJob.assert_not_called()


def test_compute_export_jobs_handles_cross_datasource_name_collision(
    mock_client, mock_clients, make_channel, make_compute_result, make_numeric_response, make_series_count_response
):
    channels = [
        make_channel("temp", ChannelDataType.DOUBLE, data_source="ds-a"),
        make_channel("temp", ChannelDataType.INT, data_source="ds-b"),
    ]
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1, 1])
    mock_client._clients.compute.BatchComputeWithUnits.return_value = MagicMock(
        results=[
            make_compute_result(success=make_numeric_response([50, 100])),
            make_compute_result(success=make_numeric_response([50, 100])),
        ]
    )

    handler = PolarsExportHandler(client=mock_client)
    jobs = handler._compute_export_jobs(channels, Bounds(0, TEN_SECONDS_NS))

    assert {(ch.data_source, ch.name, ch.data_type) for group in jobs for job in group for ch in job.channels} == {
        ("ds-a", "temp", ChannelDataType.DOUBLE),
        ("ds-b", "temp", ChannelDataType.INT),
    }


def test_export_excludes_unsupported_channel_types(
    mock_client, mock_clients, make_channel, make_compute_result, make_numeric_response, make_series_count_response
):
    """export() filters out LOG/UNKNOWN channels so only DOUBLE/INT/STRING reach the API pipeline."""
    channels = [
        make_channel("temp", ChannelDataType.DOUBLE),
        make_channel("logs", ChannelDataType.LOG),
        make_channel("mystery", ChannelDataType.UNKNOWN),
    ]
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1])
    mock_client._clients.compute.BatchComputeWithUnits.return_value = MagicMock(
        results=[make_compute_result(success=make_numeric_response([50, 100]))]
    )
    service = mock_client._clients.export_jobs
    mock_client._clients.resolve_default_workspace_rid.return_value = "workspace"
    service.CreateExportJob.return_value.export_job.export_id = "job-id"
    service.GetExportJob.return_value.export_job = jobs.ExportJob(
        export_id="job-id", status=jobs.EXPORT_JOB_STATUS_FAILED
    )

    with pytest.raises(RuntimeError, match="job-id.*FAILED"):
        list(PolarsExportHandler(mock_client).export(channels, start=0, end=TEN_SECONDS_NS, buckets=10))

    request = service.CreateExportJob.call_args.args[0]
    assert request.workspace_rid == "workspace"
    definition = request.compute.definition
    assert len(definition.time_domain.columns) == 1
    assert definition.resolution.bucket_count == 10
    service.GetExportJob.assert_called_once_with(jobs.GetExportJobRequest(export_id="job-id"))


def test_completed_compute_export_downloads_csv(
    mock_client,
    mock_clients,
    make_channel,
    make_series_count_response,
    make_compute_result,
    make_numeric_response,
    monkeypatch,
):
    """A completed compute export yields a typed frame from the presigned file."""
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1])
    mock_client._clients.compute.BatchComputeWithUnits.return_value = MagicMock(
        results=[make_compute_result(success=make_numeric_response([50, 100]))]
    )
    service = mock_client._clients.export_jobs
    mock_client._clients.resolve_default_workspace_rid.return_value = "workspace"
    service.CreateExportJob.return_value.export_job.export_id = "job"
    service.GetExportJob.side_effect = [
        jobs.GetExportJobResponse(
            export_job=jobs.ExportJob(export_id="job", status=jobs.EXPORT_JOB_STATUS_IN_PROGRESS)
        ),
        jobs.GetExportJobResponse(
            export_job=jobs.ExportJob(
                export_id="job",
                status=jobs.EXPORT_JOB_STATUS_COMPLETED,
                files=[jobs.ExportFile(download_url="https://storage.example/file")],
            )
        ),
    ]
    monkeypatch.setattr("time.sleep", lambda _: None)
    response = MagicMock()
    response.__enter__.return_value.status_code = 200
    response.__enter__.return_value.raw = io.BytesIO(gzip.compress(b"timestamp,0\n0,12.5\n"))
    monkeypatch.setattr("requests.get", MagicMock(return_value=response))
    frames = list(
        PolarsExportHandler(mock_client).export(
            [make_channel("timestamp")], start=0, end=TEN_SECONDS_NS, resolution=TEN_SECONDS_NS
        )
    )
    assert len(frames) == 1
    assert frames[0].equals(pl.DataFrame({"timestamp.1": [0.0], "timestamp": [12.5]}))


# -- large_channels sub-slice path --


def test_compute_export_jobs_subdivides_large_channel_into_sub_slices(
    mock_client, mock_clients, make_channel, make_compute_result, make_numeric_response, make_series_count_response
):
    """Explicit sample budgets split a high-rate channel into gapless, disjoint inclusive windows."""
    points_per_request = 500
    parent_range = Bounds(0, TEN_SECONDS_NS)
    batch_duration = datetime.timedelta(seconds=10)
    channel = make_channel("firehose", ChannelDataType.DOUBLE)

    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1])
    mock_client._clients.compute.BatchComputeWithUnits.return_value = MagicMock(
        results=[make_compute_result(success=make_numeric_response([1000, 1000]))]
    )

    handler = PolarsExportHandler(client=mock_client, points_per_request=points_per_request)
    jobs = handler._compute_export_jobs([channel], parent_range, batch_duration=batch_duration)

    # All sub-slices roll up under a single parent time_slice.
    (job_list,) = map(list, jobs)
    parent_slice = parent_range
    assert len(job_list) > 1

    assert all(list(job.channels) == [channel] for job in job_list)

    # Sub-slices tile the parent: no gaps, no overlaps, full coverage, each narrower than parent.
    sub_ranges = sorted((job.bounds for job in job_list), key=lambda window: window.start)
    assert sub_ranges[0].start == parent_slice.start
    assert sub_ranges[-1].end == parent_slice.end
    for a, b in zip(sub_ranges, sub_ranges[1:]):
        assert a.end + 1 == b.start
    assert all(r.duration < parent_slice.duration for r in sub_ranges)


def test_raw_parquet_uses_typed_points_and_skips_rate_sampling(
    mock_client, mock_clients, make_channel, make_series_count_response, make_enriched_dataset, monkeypatch
):
    """Raw exports preserve Int64 values, adjacent nanoseconds and exact-tag filters."""
    start = 1_800_000_000_000_000_000
    name = "count's"
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1])
    set_storage_types(mock_client, [(name, "INT64")])
    mock_client._clients.catalog.get_enriched_datasets.return_value = [make_enriched_dataset("ds-1")]
    mock_client._clients.resolve_default_workspace_rid.return_value = "workspace"
    service = mock_client._clients.export_jobs
    service.CreateExportJob.return_value.export_job.export_id = "raw"
    service.GetExportJob.return_value.export_job = jobs.ExportJob(
        export_id="raw",
        status=jobs.EXPORT_JOB_STATUS_COMPLETED,
        files=[jobs.ExportFile(download_url="https://example.test/raw")],
    )
    payload = io.BytesIO()
    pl.DataFrame({"ts": [start, start + 1], "channel": [name, name], "value": [2**53 + 1, 2**53 + 3]}).with_columns(
        pl.col("ts").cast(pl.Datetime("ns"))
    ).write_parquet(payload)
    payload.seek(0)
    response = MagicMock()
    response.__enter__.return_value.status_code = 200
    response.__enter__.return_value.raw = payload
    monkeypatch.setattr("requests.get", MagicMock(return_value=response))
    frames = list(
        PolarsExportHandler(mock_client).export(
            [make_channel(name, ChannelDataType.DOUBLE)],
            start,
            start + 10_000,
            tags={"flight": "pilot's"},
            batch_duration=datetime.timedelta(seconds=1),
            timestamp_type=Relative("nanoseconds", start),
        )
    )
    assert len(frames) == 1
    assert frames[0].schema[name] == pl.Int64
    assert frames[0].equals(pl.DataFrame({"timestamp": [0.0, 1.0], name: [2**53 + 1, 2**53 + 3]}))
    request = service.CreateExportJob.call_args.args[0]
    assert request.settings.file_format == jobs.EXPORT_FILE_FORMAT_PARQUET
    assert "FROM points_int" in request.sql.query
    assert "'count''s'" in request.sql.query
    assert "tags['flight'] = 'pilot''s'" in request.sql.query
    mock_client._clients.compute.BatchComputeWithUnits.assert_not_called()


@pytest.mark.parametrize("integer", [17, 2**53 + 1])
@pytest.mark.parametrize("split", [False, True], ids=["native-frame", "joined-across-requests"])
def test_raw_numeric_type_change_preserves_values_or_raises(
    mock_client,
    mock_clients,
    make_channel,
    make_enriched_dataset,
    make_series_count_response,
    make_compute_result,
    make_numeric_response,
    monkeypatch,
    integer,
    split,
):
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1])
    mock_client._clients.catalog.get_enriched_datasets.return_value = [make_enriched_dataset("ds-1")]
    set_storage_types(mock_client, [("signal", "DOUBLE"), ("signal", "INT64")])
    mock_client._clients.compute.BatchComputeWithUnits.return_value.results = [
        make_compute_result(success=make_numeric_response([2]))
    ]
    data = pl.DataFrame(
        {"ts": [0, 10**9], "channel": ["signal", "signal"], "value": [None, 1.5], "integer_value": [integer, None]},
        schema={"ts": pl.Datetime("ns"), "channel": pl.String, "value": pl.Float64, "integer_value": pl.Int64},
    )
    files = iter(data.iter_slices(1) if split else [data])

    @contextmanager
    def download(_, request):
        assert "UNION ALL" in request.sql.query
        file = io.BytesIO()
        next(files).write_parquet(file)
        file.seek(0)
        yield file

    monkeypatch.setattr("nominal.thirdparty.polars.polars_export_handler.download_export", download)
    handler = PolarsExportHandler(mock_client, points_per_request=1 if split else None, num_workers=1)
    frames = handler.export(
        [make_channel("signal")], 0, 2 * 10**9, batch_duration=datetime.timedelta(seconds=2), join_batches=split
    )
    if integer == 17:
        (frame,) = frames
        assert frame.to_dict(as_series=False) == {"timestamp": [0.0, 1.0], "signal": [17.0, 1.5]}
    else:
        with pytest.raises(ValueError, match="signal.*lose precision"):
            list(frames)


def test_unsupported_physical_numeric_type_fails_before_export(
    mock_client, mock_clients, make_channel, make_enriched_dataset, make_series_count_response
):
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1])
    mock_client._clients.catalog.get_enriched_datasets.return_value = [make_enriched_dataset("ds-1")]
    set_storage_types(mock_client, [("counter", "UINT64")])
    frames = PolarsExportHandler(mock_client).export([make_channel("counter", ChannelDataType.DOUBLE)], 0, 1)
    with pytest.raises(ValueError, match="counter.*UINT64"):
        next(frames)
    mock_client._clients.export_jobs.CreateExportJob.assert_not_called()


@pytest.mark.parametrize(
    ("point_target", "join_batches", "buckets", "fixed_windows"),
    [
        pytest.param(None, False, None, True, id="native"),
        pytest.param(25_000_000, False, None, False, id="explicit-frame-target"),
        pytest.param(None, True, None, False, id="joined"),
        pytest.param(None, False, 100, False, id="decimated"),
    ],
)
def test_automatic_window_sizing(
    mock_client,
    mock_clients,
    make_channel,
    make_series_count_response,
    make_enriched_dataset,
    make_numeric_response,
    make_compute_result,
    point_target,
    join_batches,
    buckets,
    fixed_windows,
):
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1])
    set_storage_types(mock_client, [("signal", "DOUBLE")])
    mock_client._clients.catalog.get_enriched_datasets.return_value = [make_enriched_dataset("ds-1")]
    sample = mock_client._clients.compute.BatchComputeWithUnits
    sample.return_value = MagicMock(results=[make_compute_result(success=make_numeric_response([10]))])
    end = 130_000_000_000
    plan = PolarsExportHandler(mock_client, points_per_dataframe=point_target)._compute_export_jobs(
        [make_channel("signal")], Bounds(0, end), join_batches=join_batches, buckets=buckets
    )
    bounds = [job.bounds for window in plan for job in window]
    if fixed_windows:
        assert bounds == [Bounds(0, 119_999_999_999), Bounds(120_000_000_000, end)]
        sample.assert_not_called()
    else:
        assert bounds == [Bounds(0, end)]
        sample.assert_called_once()


@pytest.mark.parametrize("count", [6000, 1_000_001, None])
def test_long_native_numeric_export_only_coalesces_a_confirmed_small_result(
    mock_client,
    mock_clients,
    make_channel,
    make_enriched_dataset,
    make_series_count_response,
    make_numeric_response,
    make_compute_result,
    count,
):
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1])
    set_storage_types(mock_client, [("signal", "DOUBLE")])
    mock_client._clients.catalog.get_enriched_datasets.return_value = [make_enriched_dataset("ds-1")]
    compute = mock_client._clients.compute.BatchComputeWithUnits
    compute.return_value = MagicMock(
        results=[] if count is None else [make_compute_result(success=make_numeric_response([count]))]
    )
    end = 6_000_000_000_000 - 1
    plan = PolarsExportHandler(mock_client)._compute_export_jobs(
        [make_channel("signal")], Bounds(0, end), join_batches=False
    )
    windows = [job.bounds for group in plan for job in group]
    assert len(windows) == (1 if count == 6000 else 50)
    assert windows[0].start == 0 and windows[-1].end == end
    assert compute.call_args.args[0].inputs[0].computation.series_summary.summarization.bucket_count.bucket_count == 1


def test_raw_jobs_require_plain_dataset_and_single_series(
    mock_client, mock_clients, make_channel, make_enriched_dataset, make_series_count_response
):
    """Use compute for ambiguous series and derived datasets even without decimation."""
    channels = [
        make_channel("raw", data_source="physical"),
        make_channel("multi", data_source="physical"),
        make_channel("derived", data_source="view"),
    ]
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1, 2, 1])
    set_storage_types(mock_client, [("raw", "DOUBLE"), ("multi", "DOUBLE")])
    mock_client._clients.catalog.get_enriched_datasets.return_value = [
        make_enriched_dataset("physical"),
        make_enriched_dataset("view", derived_definition=MagicMock()),
    ]
    plan = PolarsExportHandler(mock_client)._compute_export_jobs(
        channels, Bounds(0, TEN_SECONDS_NS), batch_duration=datetime.timedelta(seconds=10)
    )
    assert {ch.name: job.raw_kind for group in plan for job in group for ch in job.channels} == {
        "raw": "points_double",
        "multi": None,
        "derived": None,
    }


def test_unjoined_raw_frames_follow_exact_sample_times(
    mock_client, mock_clients, make_channel, make_series_count_response, make_enriched_dataset, monkeypatch
):
    """One SQL download can contain shared, unequal, and partially coincident clocks."""
    names = ["slow", "fast", "timestamp", "jitter", "empty"]
    clocks = [[0, 4], [0, 1, 4], [0, 1, 4], [0, 2, 4], []]
    points = (
        pl.DataFrame(
            [(ts, name, 2**53 + i + ts) for i, (name, clock) in enumerate(zip(names, clocks)) for ts in clock],
            schema=["ts", "channel", "value"],
            orient="row",
        )
        .sort("ts")
        .with_columns(pl.col("ts").cast(pl.Datetime("ns")))
    )
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1] * len(names))
    set_storage_types(mock_client, [(n, "INT64") for n in names])
    mock_client._clients.catalog.get_enriched_datasets.return_value = [make_enriched_dataset("ds-1")]
    mock_client._clients.resolve_default_workspace_rid.return_value = "workspace"
    requests = []

    @contextmanager
    def download(_, request):
        requests.append(request)
        payload = io.BytesIO()
        points.write_parquet(payload)
        payload.seek(0)
        yield payload

    monkeypatch.setattr("nominal.thirdparty.polars.polars_export_handler.download_export", download)
    frames = list(
        PolarsExportHandler(mock_client).export(
            [make_channel(name, ChannelDataType.INT) for name in names],
            0,
            4,
            join_batches=False,
            timestamp_type=Relative("nanoseconds", 0),
        )
    )
    assert len(requests) == 1
    assert requests[0].HasField("sql")
    assert {tuple(df.columns[1:]) for df in frames} == {("slow",), ("fast", "timestamp"), ("jitter",)}
    for df in frames:
        for name in df.columns[1:]:
            i = names.index(name)
            assert df["timestamp.1"].to_list() == clocks[i]
            assert df[name].to_list() == [2**53 + i + ts for ts in clocks[i]]


@pytest.mark.parametrize(
    ("join_batches", "initial_jobs", "remaining_frames"),
    [(False, [0, 1, 2, 2], [1, 2, 2, 3, 4, 5]), (True, [0, 1], [1, 2, 3, 4, 5])],
)
def test_export_prefetch_preserves_window_order(
    mock_client, make_channel, monkeypatch, join_batches, initial_jobs, remaining_frames
):
    windows = [Bounds(i, i) for i in range(6)]
    jobs = {window: [_ExportJob(channels=[make_channel("left")], bounds=window, tags={})] for window in windows}
    # Two channel groups in the third window require two worker slots.
    jobs[windows[2]].append(_ExportJob(channels=[make_channel("right")], bounds=windows[2], tags={}))
    submitted = []

    def submit(_fn, job, *_args):
        submitted.append(job.bounds.start)
        future = Future()
        future.set_result([pl.DataFrame({"timestamp": [job.bounds.start], job.channels[0].name: [10]})])
        return future

    pool = MagicMock()
    pool.submit.side_effect = submit
    monkeypatch.setattr("concurrent.futures.ThreadPoolExecutor", lambda max_workers: pool)
    handler = PolarsExportHandler(mock_client, num_workers=4)
    frames = handler._export_dataframes(
        iter(jobs.values()), "timestamp", "epoch_nanoseconds", join_batches=join_batches
    )

    assert next(frames)["timestamp"].item() == 0
    assert submitted == initial_jobs
    assert [frame["timestamp"][0] for frame in frames] == remaining_frames


@pytest.mark.parametrize("overlap_count", [2, None])
def test_mixed_raw_job_resolves_ambiguous_channels_for_its_own_window(
    mock_client, mock_clients, make_channel, make_enriched_dataset, make_series_count_response, overlap_count
):
    channels = [make_channel("slow"), make_channel("fast")]
    ds = mock_clients.datasource.batch_get_series_count
    ds.return_value = make_series_count_response([1, 2])
    set_storage_types(mock_client, [("slow", "DOUBLE"), ("fast", "DOUBLE")])
    mock_client._clients.catalog.get_enriched_datasets.return_value = [make_enriched_dataset("ds-1")]
    plan = PolarsExportHandler(mock_client)._compute_export_jobs(
        channels,
        Bounds(0, TWENTY_SECONDS_NS - 1),
        tags={"flight": "a"},
        batch_duration=datetime.timedelta(seconds=10),
    )
    first, second = map(list, plan)
    assert len(first) == len(second) == 1

    ds.return_value = make_series_count_response([1])
    assert [(job.raw_kind, list(job.channels)) for job in first[0].resolve_sources()] == [("points_double", channels)]
    request = ds.call_args.args[1].requests
    assert len(request) == 1 and request[0].channel == "fast"
    assert request[0].range.end.seconds == 9 and request[0].range.end.nanos == 999_999_999
    assert request[0].tag_filters is not None

    ds.return_value = make_series_count_response([0])
    assert [(job.raw_kind, list(job.channels)) for job in second[0].resolve_sources()] == [("points_double", channels)]

    if overlap_count is None:
        ds.side_effect = RuntimeError("unavailable")
    else:
        ds.return_value = make_series_count_response([overlap_count])
    assert [(job.raw_kind, list(job.channels)) for job in second[0].resolve_sources()] == [
        ("points_double", channels[:1]),
        (None, channels[1:]),
    ]


def test_multifile_channel_can_use_sql_on_its_own(
    mock_client, mock_clients, make_channel, make_enriched_dataset, make_series_count_response
):
    """Resolve a channel independently of its peers, and send a still-ambiguous window to compute."""
    channel = make_channel("fast")
    counts = mock_clients.datasource.batch_get_series_count
    counts.side_effect = [make_series_count_response([count]) for count in (3, 1, 2)]
    set_storage_types(mock_client, [("fast", "DOUBLE")])
    mock_client._clients.catalog.get_enriched_datasets.return_value = [make_enriched_dataset("ds-1")]
    windows = PolarsExportHandler(mock_client)._compute_export_jobs(
        [channel], Bounds(0, TWENTY_SECONDS_NS - 1), batch_duration=datetime.timedelta(seconds=10)
    )
    assert [source.raw_kind for window in windows for job in window for source in job.resolve_sources()] == [
        "points_double",
        None,
    ]


def test_missing_series_count_uses_compute(mock_client, mock_clients, make_channel, make_series_count_response):
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([None, 0])
    plan = PolarsExportHandler(mock_client)._compute_export_jobs(
        [make_channel("external"), make_channel("absent")],
        Bounds(0, 10),
        batch_duration=datetime.timedelta(seconds=1),
    )
    assert [(job.raw_kind, [ch.name for ch in job.channels]) for group in plan for job in group] == [
        (None, ["external"])
    ]


def test_single_categorical_sample_counts():
    response = execution_results_pb2.ComputationResult()
    response.categorical_sample.sample.timestamp.nanos = 123
    assert _extract_bucket_counts(response) == [(123, 1)]


def test_submicrosecond_window_keeps_channel(
    mock_client, mock_clients, make_channel, make_series_count_response, make_compute_result, make_numeric_response
):
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1])
    mock_client._clients.compute.BatchComputeWithUnits.return_value.results = [
        make_compute_result(success=make_numeric_response([1]))
    ]
    plan = PolarsExportHandler(mock_client, points_per_dataframe=1, points_per_request=1)._compute_export_jobs(
        [make_channel("edge")], Bounds(0, 10)
    )
    assert [job.bounds for group in plan for job in group] == [Bounds(0, 10)]


def test_joined_export_rejects_duplicate_channel_names(mock_client, make_channel):
    channels = [make_channel("temp", data_source=rid) for rid in ("first", "second")]
    with pytest.raises(ValueError, match="temp.*join_batches=False"):
        list(PolarsExportHandler(mock_client).export(channels, 0, 100))
    mock_client._clients.export_jobs.CreateExportJob.assert_not_called()


def test_joined_subdivisions_keep_tag_expansions_in_one_column(mock_client, make_channel, monkeypatch):
    """A single worker can refill the queue mid-window without splitting tag-expanded columns."""
    channel = make_channel("signal")
    jobs = [_ExportJob([channel], Bounds(i, i), {}) for i in range(2)]

    def export(job, *_):
        i = job.bounds.start
        return [pl.DataFrame({"timestamp": [i], "signal": [None], "signal_a": [i], **({"signal_b": [7]} if i else {})})]

    monkeypatch.setattr("nominal.thirdparty.polars.polars_export_handler._export_job", export)
    frames = PolarsExportHandler(mock_client, num_workers=1)._export_dataframes(
        iter([jobs]), "timestamp", "epoch_nanoseconds", True
    )
    (frame,) = frames
    assert frame.to_dict(as_series=False) == {
        "timestamp": [0.0, 1.0],
        "signal": [None, None],
        "signal_a": [0, 1],
        "signal_b": [None, 7],
    }


def test_joined_disjoint_clocks_preserve_both_timestamps(mock_client, make_channel, monkeypatch):
    window = Bounds(0, 1)
    jobs = [_ExportJob([make_channel(name)], window, {}) for name in ("slow", "timestamp_right", "empty")]

    def export(job, *_):
        name = job.channels[0].name
        timestamps = {"slow": [0], "timestamp_right": [1], "empty": []}[name]
        return [
            pl.DataFrame({"timestamp": timestamps, name: timestamps}, schema={"timestamp": pl.Int64, name: pl.Int64})
        ]

    monkeypatch.setattr("nominal.thirdparty.polars.polars_export_handler._export_job", export)
    (frame,) = PolarsExportHandler(mock_client)._export_dataframes(iter([jobs]), "timestamp", "epoch_nanoseconds", True)
    assert frame.to_dict(as_series=False) == {
        "timestamp": [0.0, 1.0],
        "slow": [0, None],
        "timestamp_right": [None, 1],
        "empty": [None, None],
    }


def test_first_window_does_not_plan_the_remaining_export(
    mock_client, mock_clients, make_channel, make_series_count_response, monkeypatch
):
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1])
    planned = []

    def fit_query_limit(job):
        planned.append(job.bounds)
        return (job,)

    monkeypatch.setattr(_ExportJob, "fit_query_limit", fit_query_limit)
    plan = PolarsExportHandler(mock_client)._compute_export_jobs(
        [make_channel("signal")], Bounds(0, 10**15), batch_duration=datetime.timedelta(seconds=1)
    )
    assert next(next(plan)).bounds == Bounds(0, 10**9 - 1)
    assert planned == [Bounds(0, 10**9 - 1)]


def test_tag_expanded_compute_csv_preserves_string_values(
    mock_client, mock_clients, make_channel, make_series_count_response, monkeypatch
):
    mock_client._clients.resolve_default_workspace_rid.return_value = "workspace"
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([2, 1])

    @contextmanager
    def download(_, request):
        grouped, plain = [col.name for col in request.compute.definition.time_domain.columns]
        csv = f"timestamp,{grouped}_region_eu,{plain}\n123,00042,9007199254740993\n"
        yield io.BytesIO(gzip.compress(csv.encode()))

    monkeypatch.setattr("nominal.thirdparty.polars.polars_export_handler.download_export", download)
    channels = [make_channel("timestamp", ChannelDataType.STRING), make_channel("count", ChannelDataType.INT)]
    (df,) = PolarsExportHandler(mock_client).export(channels, 0, 1000, batch_duration=datetime.timedelta(seconds=1))
    assert df["timestamp_region_eu"].to_list() == ["00042"]
    assert df["timestamp"].to_list() == [None]
    assert df["count"].to_list() == [9007199254740993]


def test_a_large_unjoined_window_starts_with_bounded_work(mock_client, make_channel, monkeypatch):
    """The consumer can stop after one frame without planning or submitting the entire window."""
    planned = []

    def jobs():
        for i in range(100):
            planned.append(i)
            yield _ExportJob([make_channel("signal")], Bounds(i, i), {})

    def export(job, *_):
        return [pl.DataFrame({"timestamp": [job.bounds.start], "signal": [1]})]

    monkeypatch.setattr("nominal.thirdparty.polars.polars_export_handler._export_job", export)
    frames = PolarsExportHandler(mock_client, num_workers=3)._export_dataframes(
        iter([jobs()]), "timestamp", "epoch_nanoseconds", join_batches=False
    )
    assert next(frames)["signal"].item() == 1
    frames.close()
    # Three worker slots plus at most one unsubmitted job for lookahead.
    assert len(planned) <= 4
