from __future__ import annotations

import contextlib
import datetime
import logging
import time
from unittest.mock import MagicMock

import pytest
import requests
from nominal_api import api, scout_compute_api

from nominal.core._utils.multipart_downloader import DownloadResults
from nominal.core.channel import ChannelDataType
from nominal.thirdparty.polars import polars_export_handler as peh
from nominal.thirdparty.polars.polars_export_handler import (
    ExportNotReadyError,
    PolarsExportHandler,
    _batch_channel_points_per_second,
    _build_channel_groups,
    _ExportJob,
    _extract_bucket_counts,
    _is_transient_error,
    _peak_points_per_second,
    _TimeRange,
)

# Note: for new test coverage, prefer testing through `PolarsExportHandler._compute_export_jobs`
# (with MagicMock on the client fixtures) rather than direct unit tests of private helpers like
# `_batch_channel_points_per_second`. Integration-style tests are less brittle to internal refactors.

TEN_SECONDS_NS = 10_000_000_000
TWENTY_SECONDS_NS = 20_000_000_000


def _empty_response() -> MagicMock:
    """Create a ComputeNodeResponse with all union variants unset (the caller fills one in)."""
    response = MagicMock(spec=scout_compute_api.ComputeNodeResponse)
    response.bucketed_numeric = None
    response.numeric = None
    response.numeric_point = None
    response.bucketed_enum = None
    response.enum = None
    return response


# -- _extract_bucket_counts --


def test_extract_numeric_bucketed(make_numeric_response):
    """Bucketed numeric response yields (timestamp_ns, count) pairs preserving bucket order."""
    result = _extract_bucket_counts(make_numeric_response([50, 75]))

    assert result == [(0, 50), (TEN_SECONDS_NS, 75)]


def test_extract_numeric_undecimated():
    """Undecimated numeric series yields count=1 per timestamp."""
    response = _empty_response()
    response.numeric = MagicMock()
    response.numeric.timestamps = [api.Timestamp(seconds=s, nanos=0) for s in (100, 200, 300)]

    result = _extract_bucket_counts(response)

    assert len(result) == 3
    assert all(count == 1 for _, count in result)


def test_extract_numeric_single_point():
    """A single numeric point yields one entry with count=1."""
    response = _empty_response()
    response.numeric_point = MagicMock()
    response.numeric_point.timestamp = api.Timestamp(seconds=100, nanos=0)

    assert _extract_bucket_counts(response) == [(100_000_000_000, 1)]


def test_extract_enum_bucketed(make_enum_response):
    """Bucketed enum response sums histogram frequencies per bucket."""
    result = _extract_bucket_counts(make_enum_response([{0: 30, 1: 20}, {0: 10}]))

    assert result == [(0, 50), (TEN_SECONDS_NS, 10)]


def test_extract_enum_undecimated():
    """Undecimated enum series yields count=1 per timestamp."""
    response = _empty_response()
    response.enum = MagicMock()
    response.enum.timestamps = [api.Timestamp(seconds=s, nanos=0) for s in (100, 200)]

    result = _extract_bucket_counts(response)

    assert len(result) == 2
    assert all(count == 1 for _, count in result)


def test_extract_unrecognized_response_returns_empty():
    """An unrecognized response shape returns an empty list rather than raising."""
    response = _empty_response()
    response.type = "something_unknown"

    assert _extract_bucket_counts(response) == []


# -- _peak_points_per_second --


def test_peak_pps_empty_buckets():
    """No buckets yields zero PPS."""
    assert _peak_points_per_second([], 0, TEN_SECONDS_NS) == 0.0


def test_peak_pps_single_bucket():
    """A single bucket divides its count by the full time range."""
    result = _peak_points_per_second([(5_000_000_000, 100)], start_ns=0, end_ns=TEN_SECONDS_NS)

    assert result == pytest.approx(10.0)


def test_peak_pps_returns_peak_across_buckets():
    """Multiple buckets return the highest PPS between any consecutive pair."""
    buckets = [
        (1_000_000_000, 10),
        (2_000_000_000, 100),
        (3_000_000_000, 20),
    ]
    assert _peak_points_per_second(buckets, start_ns=0, end_ns=3_000_000_000) == pytest.approx(100.0)


def test_peak_pps_zero_duration_returns_zero():
    """A zero-length time range returns 0 instead of dividing by zero."""
    assert _peak_points_per_second([(5_000_000_000, 100)], start_ns=0, end_ns=0) == 0.0


def test_peak_pps_skips_duplicate_timestamps():
    """Consecutive buckets with identical timestamps are skipped, not divided by zero."""
    buckets = [
        (1_000_000_000, 10),
        (1_000_000_000, 50),
        (2_000_000_000, 20),
    ]
    # Only the 1s→2s interval contributes: 20 pts / 1s = 20 PPS
    assert _peak_points_per_second(buckets, start_ns=0, end_ns=2_000_000_000) == pytest.approx(20.0)


# -- _batch_channel_points_per_second --


@pytest.mark.parametrize(
    "data_type,response_kind",
    [
        (ChannelDataType.DOUBLE, "numeric"),
        (ChannelDataType.INT, "numeric"),
        (ChannelDataType.STRING, "enum"),
    ],
)
def test_pps_supported_types_produce_positive_rates(
    mock_client,
    make_channel,
    make_numeric_response,
    make_enum_response,
    make_compute_result,
    data_type,
    response_kind,
):
    """All supported channel types produce positive PPS from a bucketed compute response."""
    response = (
        make_numeric_response([50, 100])
        if response_kind == "numeric"
        else make_enum_response([{0: 30, 1: 20}, {0: 10}])
    )
    mock_client._clients.compute.batch_compute_with_units.return_value = MagicMock(
        results=[make_compute_result(success=response)]
    )

    result = _batch_channel_points_per_second(
        mock_client, [make_channel("ch", data_type)], 0, TWENTY_SECONDS_NS, {}, 100
    )

    rate = result[("ds-1", "ch")]
    assert rate is not None and rate > 0


def test_pps_unsupported_type_raises(mock_client, make_channel):
    """Unsupported channel types surface as a ValueError to the caller.

    Callers are expected to pre-filter to DOUBLE/INT/STRING so that this function has a
    single source of truth for type support rather than silently coercing to None.
    """
    with pytest.raises(ValueError):
        _batch_channel_points_per_second(
            mock_client, [make_channel("mystery", ChannelDataType.UNKNOWN)], 0, TEN_SECONDS_NS, {}, 100
        )


def test_pps_empty_success_returns_none(mock_client, make_channel, make_compute_result):
    """A compute result with neither success nor error payload produces None.

    Covers a malformed conjure response where both union variants are unset; the caller
    should see None rather than an AssertionError bubbling up.
    """
    mock_client._clients.compute.batch_compute_with_units.return_value = MagicMock(
        results=[make_compute_result(success=None, error=None)]
    )

    result = _batch_channel_points_per_second(mock_client, [make_channel("empty")], 0, TEN_SECONDS_NS, {}, 100)

    assert result == {("ds-1", "empty"): None}


def test_pps_empty_channels_skips_api(mock_client):
    """An empty channel list returns empty results without calling the compute API."""
    result = _batch_channel_points_per_second(mock_client, [], 0, TEN_SECONDS_NS, {}, 100)

    assert result == {}
    mock_client._clients.compute.batch_compute_with_units.assert_not_called()


def test_pps_api_failure_returns_none_for_all(mock_client, make_channel):
    """If the compute API raises, all channels get None PPS so callers can distinguish failure from zero-data."""
    mock_client._clients.compute.batch_compute_with_units.side_effect = RuntimeError("API down")

    result = _batch_channel_points_per_second(
        mock_client, [make_channel("a"), make_channel("b")], 0, TEN_SECONDS_NS, {}, 100
    )

    assert result == {("ds-1", "a"): None, ("ds-1", "b"): None}


def test_pps_per_channel_error_returns_none_for_failed(
    mock_client, make_channel, make_numeric_response, make_compute_result
):
    """Individual channel errors produce None for that channel while peers still succeed."""
    mock_client._clients.compute.batch_compute_with_units.return_value = MagicMock(
        results=[
            make_compute_result(success=make_numeric_response([50, 100])),
            make_compute_result(error="channel not found"),
        ]
    )

    result = _batch_channel_points_per_second(
        mock_client, [make_channel("good"), make_channel("bad")], 0, TWENTY_SECONDS_NS, {}, 100
    )

    good_rate = result[("ds-1", "good")]
    assert good_rate is not None and good_rate > 0
    assert result[("ds-1", "bad")] is None


# -- _build_channel_groups --


def _group_args(pairs):
    """Build (pps_by_key, channels_by_key) dicts from a list of (Channel, rate) pairs."""
    pps_by_key = {(ch.data_source, ch.name): rate for ch, rate in pairs}
    channels_by_key = {(ch.data_source, ch.name): ch for ch, _ in pairs}
    return pps_by_key, channels_by_key


def test_groups_mixed_types_share_a_group_when_budget_allows(make_channel):
    """Numeric (DOUBLE/INT) and string channels are packed into the same group under the rate budget.

    Now that PPS estimation works for all types, grouping is budget-driven and no longer
    partitions by data type. With a roomy budget, all channels fit in a single group.
    """
    pps, channels = _group_args(
        [
            (make_channel("temp", ChannelDataType.DOUBLE), 100.0),
            (make_channel("pressure", ChannelDataType.INT), 100.0),
            (make_channel("status", ChannelDataType.STRING), 50.0),
        ]
    )

    groups, large = _build_channel_groups(
        pps,
        channels,
        points_per_request=1_000_000,
        max_channels_per_group=100,
        batch_duration=datetime.timedelta(seconds=10),
    )

    assert large == []
    assert len(groups) == 1
    assert {ch.name for ch in groups[0]} == {"temp", "pressure", "status"}


def test_groups_split_when_rate_budget_exceeded(make_channel):
    """Channels are split into new groups when cumulative PPS exceeds the per-request budget."""
    # 5 channels at 200 PPS each, budget 500 PPS → expect groups of 2, 2, 1.
    pps, channels = _group_args([(make_channel(f"ch{i}"), 200.0) for i in range(5)])

    groups, large = _build_channel_groups(
        pps,
        channels,
        points_per_request=500,
        max_channels_per_group=100,
        batch_duration=datetime.timedelta(seconds=1),
    )

    assert len(groups) == 3
    assert large == []


def test_groups_isolate_high_rate_channels(make_channel):
    """A channel whose individual rate exceeds the per-group budget is returned as large."""
    pps, channels = _group_args(
        [
            (make_channel("fast"), 10_000.0),
            (make_channel("slow"), 100.0),
        ]
    )

    groups, large = _build_channel_groups(
        pps,
        channels,
        points_per_request=500,
        max_channels_per_group=100,
        batch_duration=datetime.timedelta(seconds=1),
    )

    assert [ch.name for ch in large] == ["fast"]
    assert len(groups) == 1
    assert [ch.name for ch in groups[0]] == ["slow"]


def test_groups_respect_max_channels_per_group(make_channel):
    """Groups are capped at the configured max channels per group."""
    pps, channels = _group_args([(make_channel(f"ch{i}"), 1.0) for i in range(10)])

    groups, large = _build_channel_groups(
        pps,
        channels,
        points_per_request=1_000_000,
        max_channels_per_group=3,
        batch_duration=datetime.timedelta(seconds=1),
    )

    assert large == []
    assert sum(len(g) for g in groups) == 10
    assert all(len(g) <= 3 for g in groups)


def test_groups_empty_input():
    """Empty inputs produce no groups and no large channels."""
    groups, large = _build_channel_groups(
        {},
        {},
        points_per_request=1_000_000,
        max_channels_per_group=100,
        batch_duration=datetime.timedelta(seconds=1),
    )

    assert groups == []
    assert large == []


def test_groups_include_zero_rate_channels(make_channel):
    """Channels with 0.0 PPS (unknown rate) are still grouped, not silently dropped."""
    pps, channels = _group_args(
        [
            (make_channel("known"), 100.0),
            (make_channel("unknown"), 0.0),
        ]
    )

    groups, large = _build_channel_groups(
        pps,
        channels,
        points_per_request=1_000_000,
        max_channels_per_group=100,
        batch_duration=datetime.timedelta(seconds=10),
    )

    assert large == []
    assert {ch.name for group in groups for ch in group} == {"known", "unknown"}


# -- _compute_export_jobs --


def test_compute_export_jobs_excludes_zero_and_none_pps_channels(
    mock_client, mock_clients, make_channel, make_compute_result, make_numeric_response, make_series_count_response
):
    """Channels whose rate estimate is 0.0 or None are excluded from export jobs.

    These channels have nothing to export — either no data in range (0.0) or the rate
    estimator errored (None). Including them would waste an export request per channel.
    """
    channels = [
        make_channel("known", ChannelDataType.DOUBLE),
        make_channel("none_rate", ChannelDataType.DOUBLE),
        make_channel("zero_rate", ChannelDataType.DOUBLE),
    ]
    # Data-presence filter: all three channels pass — the cheap pre-filter doesn't catch
    # every "no data" case, so the exclusion must be enforced after PPS compute too.
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1, 1, 1])
    # PPS estimation: "known" gets a positive rate, "none_rate" errors (None), "zero_rate"
    # succeeds with no buckets (0.0).
    mock_client._clients.compute.batch_compute_with_units.return_value = MagicMock(
        results=[
            make_compute_result(success=make_numeric_response([50, 100])),
            make_compute_result(error="compute failed"),
            make_compute_result(success=make_numeric_response([])),
        ]
    )

    handler = PolarsExportHandler(client=mock_client)
    jobs = handler._compute_export_jobs(channels, _TimeRange(0, TEN_SECONDS_NS), timestamp_type="epoch_seconds")

    exported_names = {name for job_list in jobs.values() for job in job_list for name in job.channel_names}
    assert exported_names == {"known"}


def test_compute_export_jobs_skip_rate_estimation_keeps_all_channels(mock_client, make_channel):
    """skip_rate_estimation bypasses rate planning: one single-channel job per channel, none dropped.

    This is the path for channels whose rate can't be estimated (e.g. high-cardinality enums that
    error with Compute:TooManyCategories) -- they must NOT be excluded.
    """
    channels = [
        make_channel("high_card_enum", ChannelDataType.STRING),
        make_channel("plain", ChannelDataType.DOUBLE),
    ]
    rng = _TimeRange(0, TEN_SECONDS_NS)
    handler = PolarsExportHandler(client=mock_client)

    jobs = handler._compute_export_jobs(channels, rng, timestamp_type="epoch_nanoseconds", skip_rate_estimation=True)

    all_jobs = [job for job_list in jobs.values() for job in job_list]
    assert {name for job in all_jobs for name in job.channel_names} == {"high_card_enum", "plain"}
    assert all(len(job.channel_names) == 1 for job in all_jobs)  # one channel per request
    assert all(job.time_slice == rng for job in all_jobs)  # whole range, no time-batching
    # No compute calls were made -- rate estimation was skipped entirely.
    mock_client._clients.compute.batch_compute_with_units.assert_not_called()


def test_compute_export_jobs_handles_cross_datasource_name_collision(
    mock_client, mock_clients, make_channel, make_compute_result, make_numeric_response, make_series_count_response
):
    """Two channels sharing a name but different datasources both appear with correct attributes.

    Regression guard for name-keyed internal maps: when `channels_by_key` was keyed by name
    alone, a same-named channel from a second datasource would silently overwrite the first.
    The maps are now keyed by `(data_source, name)` tuples.
    """
    channels = [
        make_channel("temp", ChannelDataType.DOUBLE, data_source="ds-a"),
        make_channel("temp", ChannelDataType.INT, data_source="ds-b"),
    ]
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1, 1])
    mock_client._clients.compute.batch_compute_with_units.return_value = MagicMock(
        results=[
            make_compute_result(success=make_numeric_response([50, 100])),
            make_compute_result(success=make_numeric_response([50, 100])),
        ]
    )

    handler = PolarsExportHandler(client=mock_client)
    jobs = handler._compute_export_jobs(channels, _TimeRange(0, TEN_SECONDS_NS), timestamp_type="epoch_seconds")

    jobs_by_ds: dict[str, list] = {}
    for job_list in jobs.values():
        for job in job_list:
            jobs_by_ds.setdefault(job.datasource_rid, []).append(job)

    assert set(jobs_by_ds.keys()) == {"ds-a", "ds-b"}
    assert any(job.channel_types == {"temp": ChannelDataType.DOUBLE} for job in jobs_by_ds["ds-a"])
    assert any(job.channel_types == {"temp": ChannelDataType.INT} for job in jobs_by_ds["ds-b"])


def test_export_excludes_unsupported_channel_types(
    mock_client, mock_clients, make_channel, make_compute_result, make_numeric_response, make_series_count_response
):
    """export() filters out LOG/UNKNOWN channels so only DOUBLE/INT/STRING reach the API pipeline."""
    channels = [
        make_channel("temp", ChannelDataType.DOUBLE),
        make_channel("logs", ChannelDataType.LOG),
        make_channel("mystery", ChannelDataType.UNKNOWN),
    ]
    # Wire up just enough of the pipeline that it can reach (but not necessarily complete)
    # the data-presence check — which is what we care about observing.
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1])
    mock_client._clients.compute.batch_compute_with_units.return_value = MagicMock(
        results=[make_compute_result(success=make_numeric_response([50, 100]))]
    )
    mock_client._clients.dataexport.export_channel_data.return_value = MagicMock()
    mock_client.get_datasource.return_value = MagicMock()

    handler = PolarsExportHandler(client=mock_client)
    # The export() pipeline may error downstream on MagicMock CSV responses; we only need the
    # type filter (which runs before any yield) to have executed.
    with contextlib.suppress(Exception):
        list(handler.export(channels, start=0, end=TEN_SECONDS_NS))

    # Only the DOUBLE channel reached the data-presence API — LOG/UNKNOWN were filtered upstream.
    request = mock_clients.datasource.batch_get_series_count.call_args[0][1]
    assert [req.channel for req in request.requests] == ["temp"]


# -- large_channels sub-slice path --


def test_compute_export_jobs_subdivides_large_channel_into_sub_slices(
    mock_client, mock_clients, make_channel, make_compute_result, make_numeric_response, make_series_count_response
):
    """Channels exceeding the per-group rate budget are split into one job per sub-slice.

    Behavior verified:
    * The high-PPS channel is routed to ``large_channels`` and subdivided into sub-slices of
      width ``points_per_request / channel_rate`` seconds.
    * Each sub-slice becomes its own ``_ExportJob`` with ``channel_names`` equal to the single
      large channel's name and ``time_slice`` equal to the sub-slice range.
    * All sub-slice jobs for one parent batch are keyed under that parent ``time_slice``.
    * ``channel_types`` on each sub-slice job contains exactly one entry — this was a silent
      bug fix: the pre-PR code reused the group-wide dict for single-channel sub-slices.
    * Sub-slices tile the parent range with no gaps, no overlaps, and full coverage.
    """
    # Tune numbers so the math is clean:
    #   * channel rate = 1000 pts across 10s = 100 PPS
    #   * per-group budget = points_per_request / batch_duration_s = 500 / 10 = 50 PPS
    #     -> 100 > 50, so firehose lands in large_channels
    #   * sub-slice width = points_per_request / channel_rate = 500 / 100 = 5s
    #   * 10s parent slice / 5s sub-slice = 2 sub-slices
    points_per_request = 500
    parent_range = _TimeRange(0, TEN_SECONDS_NS)
    batch_duration = datetime.timedelta(seconds=10)
    channel = make_channel("firehose", ChannelDataType.DOUBLE)

    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1])
    mock_client._clients.compute.batch_compute_with_units.return_value = MagicMock(
        results=[make_compute_result(success=make_numeric_response([1000, 1000]))]
    )

    handler = PolarsExportHandler(client=mock_client, points_per_request=points_per_request)
    jobs = handler._compute_export_jobs(
        [channel], parent_range, timestamp_type="epoch_seconds", batch_duration=batch_duration
    )

    # All sub-slices roll up under a single parent time_slice.
    assert len(jobs) == 1
    parent_slice, job_list = next(iter(jobs.items()))
    assert parent_slice == parent_range
    assert len(job_list) > 1

    # Every sub-slice job is single-channel with the per-channel channel_types dict.
    for job in job_list:
        assert job.channel_names == ["firehose"]
        assert job.channel_types == {"firehose": ChannelDataType.DOUBLE}

    # Sub-slices tile the parent: no gaps, no overlaps, full coverage, each narrower than parent.
    sub_ranges = sorted(job.time_slice for job in job_list)
    assert sub_ranges[0].start_time == parent_slice.start_time
    assert sub_ranges[-1].end_time == parent_slice.end_time
    for a, b in zip(sub_ranges, sub_ranges[1:]):
        assert a.end_time == b.start_time
    assert all(r.duration_ns() < parent_slice.duration_ns() for r in sub_ranges)


# -- presigned write-to-disk export (export_to_files) --


def _make_transient_error(status: int = 503) -> Exception:
    """Build an exception that `_is_transient_error` treats as transient (has `.response.status_code`)."""
    exc = RuntimeError("transient backend error")
    exc.response = MagicMock(status_code=status)  # type: ignore[attr-defined]
    return exc


def _job(datasource_rid: str, name: str) -> _ExportJob:
    return _ExportJob(
        datasource_rid=datasource_rid,
        channel_names=[name],
        channel_types={name: ChannelDataType.DOUBLE},
        time_slice=_TimeRange(0, TEN_SECONDS_NS),
        tags={},
    )


# -- _is_transient_error --


@pytest.mark.parametrize("status", [429, 500, 503])
def test_is_transient_error_true_for_5xx_and_429(status):
    assert _is_transient_error(_make_transient_error(status))


@pytest.mark.parametrize("status", [400, 401, 404])
def test_is_transient_error_false_for_4xx(status):
    assert not _is_transient_error(_make_transient_error(status))


def test_is_transient_error_true_for_network_error():
    assert _is_transient_error(requests.ConnectionError("connection reset"))


def test_is_transient_error_false_for_plain_exception():
    assert not _is_transient_error(ValueError("not a network error"))


# -- _file_name --


def test_file_name_uses_short_rid_and_gz_extension():
    job = _job("ri.datasource.x.abc123", "a")
    assert PolarsExportHandler._file_name("export", job, 3, 7) == "export_abc123_s0003_g007.csv.gz"


# -- _generate_presigned_link --


def test_generate_presigned_link_retries_transient_then_succeeds(mock_client, monkeypatch):
    """A transient (5xx/429) failure is retried; the eventual success is returned."""
    monkeypatch.setattr(peh.time, "sleep", lambda _s: None)
    monkeypatch.setattr(peh.random, "uniform", lambda _a, _b: 0.0)
    response = MagicMock()
    endpoint = mock_client._clients.dataexport.generate_export_channel_data_presigned_link
    endpoint.side_effect = [_make_transient_error(), response]

    handler = PolarsExportHandler(client=mock_client)
    assert handler._generate_presigned_link(MagicMock()) is response
    assert endpoint.call_count == 2


def test_generate_presigned_link_raises_immediately_on_non_transient(mock_client, monkeypatch):
    """A non-transient error (e.g. 4xx / programming error) is not retried."""
    monkeypatch.setattr(peh.time, "sleep", lambda _s: None)
    endpoint = mock_client._clients.dataexport.generate_export_channel_data_presigned_link
    endpoint.side_effect = ValueError("bad request")

    handler = PolarsExportHandler(client=mock_client)
    with pytest.raises(ValueError):
        handler._generate_presigned_link(MagicMock())
    assert endpoint.call_count == 1


def test_generate_presigned_link_raises_after_max_retries(mock_client, monkeypatch):
    """Persistent transient failures exhaust the retry budget and re-raise."""
    monkeypatch.setattr(peh.time, "sleep", lambda _s: None)
    monkeypatch.setattr(peh.random, "uniform", lambda _a, _b: 0.0)
    monkeypatch.setattr(peh, "DEFAULT_MAX_LINK_RETRIES", 3)
    endpoint = mock_client._clients.dataexport.generate_export_channel_data_presigned_link
    endpoint.side_effect = _make_transient_error()

    handler = PolarsExportHandler(client=mock_client)
    with pytest.raises(RuntimeError):
        handler._generate_presigned_link(MagicMock())
    assert endpoint.call_count == 3


# -- _wait_until_materialized / _served_size --


def test_wait_until_materialized_returns_etag_when_object_is_complete(mock_client, monkeypatch):
    """Once the served size reaches the expected size, the wait returns the probed ETag."""
    resp = MagicMock(status_code=206)
    resp.headers = {"Content-Range": "bytes 0-0/100", "ETag": "abc123"}
    monkeypatch.setattr(peh.requests, "get", lambda *a, **k: resp)

    handler = PolarsExportHandler(client=mock_client)
    assert handler._wait_until_materialized("https://s3/export", expected_size=100) == "abc123"


def test_wait_until_materialized_raises_on_timeout(mock_client, monkeypatch):
    """If the object never reaches the expected size, ExportNotReadyError is raised at the deadline."""
    monkeypatch.setattr(peh, "DEFAULT_READINESS_TIMEOUT_SECS", 0.0)
    monkeypatch.setattr(peh.time, "sleep", lambda _s: None)
    resp = MagicMock(status_code=206)
    resp.headers = {"Content-Range": "bytes 0-0/10"}  # served (10) < expected (1000)
    monkeypatch.setattr(peh.requests, "get", lambda *a, **k: resp)

    handler = PolarsExportHandler(client=mock_client)
    with pytest.raises(ExportNotReadyError):
        handler._wait_until_materialized("https://s3/export", expected_size=1000)


def test_served_size_returns_none_on_request_error(mock_client, monkeypatch):
    def _boom(*_a, **_k):
        raise requests.ConnectionError("connection reset")

    monkeypatch.setattr(peh.requests, "get", _boom)
    assert PolarsExportHandler._served_size("https://s3/export") == (None, None)


def test_planning_profile_summary_logs_phase_breakdown(caplog):
    """log_summary reports per-phase totals so slow planning can be attributed to a cause."""
    profile = peh._PlanningProfile()
    profile.record(semaphore_s=0.1, link_s=1.0, materialize_s=2.0)
    profile.record(semaphore_s=0.2, link_s=3.0, materialize_s=0.5)

    with caplog.at_level(logging.INFO, logger="nominal.thirdparty.polars.polars_export_handler"):
        profile.log_summary()

    assert "Planning profile over 2 file(s)" in caplog.text
    assert "link-gen total=4.0s" in caplog.text  # 1.0 + 3.0
    assert "materialize-wait total=2.5s" in caplog.text  # 2.0 + 0.5


def test_planning_profile_summary_noop_when_empty(caplog):
    """With no recorded files, log_summary emits nothing (avoids divide-by-zero / noise)."""
    with caplog.at_level(logging.INFO, logger="nominal.thirdparty.polars.polars_export_handler"):
        peh._PlanningProfile().log_summary()
    assert caplog.text == ""


def test_served_size_returns_size_and_etag(mock_client, monkeypatch):
    """A successful ranged probe returns the full object size (from Content-Range) and the ETag."""
    resp = MagicMock(status_code=206)
    resp.headers = {"Content-Range": "bytes 0-0/4096", "ETag": "deadbeef"}
    monkeypatch.setattr(peh.requests, "get", lambda *a, **k: resp)

    assert PolarsExportHandler._served_size("https://s3/export") == (4096, "deadbeef")


def test_presigned_url_provider_returns_metadata_and_bounds_link_concurrency(mock_client, monkeypatch):
    """The provider's fetch returns a PresignedURL with size+etag, and the semaphore caps concurrent link gen."""
    import concurrent.futures
    import threading

    handler = PolarsExportHandler(client=mock_client)
    # The fetch builds an export request via job.export_request(datasource); an empty channel list
    # keeps that construction trivial without a real datasource.
    datasource = MagicMock()
    datasource.get_channels.return_value = []
    job = _job("ri.datasource.x.dsa", "a")

    # Track how many link generations run concurrently; sleep so overlap is observable.
    concurrency = {"cur": 0, "max": 0}
    lock = threading.Lock()

    def _fake_gen(_request):
        with lock:
            concurrency["cur"] += 1
            concurrency["max"] = max(concurrency["max"], concurrency["cur"])
        time.sleep(0.02)
        with lock:
            concurrency["cur"] -= 1
        resp = MagicMock()
        resp.presigned_url.url = "https://s3/export"
        resp.file_size_bytes = 4096
        return resp

    monkeypatch.setattr(handler, "_generate_presigned_link", _fake_gen)
    monkeypatch.setattr(handler, "_wait_until_materialized", lambda _url, _size: "etag-1")

    semaphore = threading.BoundedSemaphore(2)
    profile = peh._PlanningProfile()
    providers = [handler._presigned_url_provider(job, datasource, semaphore, profile) for _ in range(8)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda p: p.get(), providers))

    assert all(r.total_size == 4096 and r.etag == "etag-1" and r.url == "https://s3/export" for r in results)
    assert concurrency["max"] <= 2
    # The profile recorded one timing sample per file (used for the planning summary).
    assert len(profile._link) == 8


# -- export_to_files --


class _FakeDownloader:
    """Stand-in for MultipartFileDownloader that records items and reports a configured outcome."""

    last_items: list = []
    fail: bool = False

    @classmethod
    def create(cls, **_kwargs):
        return cls()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def download_files_pipelined(self, items, *, on_file_planned=None, on_file_complete=None):
        type(self).last_items = list(items)
        if type(self).fail:
            return DownloadResults(succeeded=[], failed={items[0].destination: RuntimeError("boom")})
        for it in items:
            if on_file_planned is not None:
                on_file_planned(it.destination)
            if on_file_complete is not None:
                on_file_complete(it.destination)
        return DownloadResults(succeeded=[it.destination for it in items], failed={})


def test_export_to_files_writes_one_file_per_job(mock_client, make_channel, monkeypatch, tmp_path):
    """Each planned _ExportJob maps to exactly one written .csv.gz file (1:1 file:dataframe)."""
    jobs = {_TimeRange(0, TEN_SECONDS_NS): [_job("ri.datasource.x.dsa", "a"), _job("ri.datasource.x.dsa", "b")]}
    monkeypatch.setattr(PolarsExportHandler, "_compute_export_jobs", lambda *a, **k: jobs)
    _FakeDownloader.fail = False
    monkeypatch.setattr(peh, "MultipartFileDownloader", _FakeDownloader)

    handler = PolarsExportHandler(client=mock_client)
    paths = handler.export_to_files([make_channel("a"), make_channel("b")], 0, TEN_SECONDS_NS, tmp_path / "out")

    assert len(paths) == 2
    assert paths == sorted(paths)
    assert all(p.name.endswith(".csv.gz") for p in paths)
    assert (tmp_path / "out").is_dir()
    # One download item per job, each destined for a distinct file under the output dir.
    assert len(_FakeDownloader.last_items) == 2
    assert {it.destination.parent for it in _FakeDownloader.last_items} == {tmp_path / "out"}


def test_export_to_files_empty_channels_returns_empty(mock_client, tmp_path):
    handler = PolarsExportHandler(client=mock_client)
    assert handler.export_to_files([], 0, TEN_SECONDS_NS, tmp_path) == []


def test_export_to_files_raises_when_downloads_fail(mock_client, make_channel, monkeypatch, tmp_path):
    jobs = {_TimeRange(0, TEN_SECONDS_NS): [_job("ri.datasource.x.dsa", "a")]}
    monkeypatch.setattr(PolarsExportHandler, "_compute_export_jobs", lambda *a, **k: jobs)
    _FakeDownloader.fail = True
    monkeypatch.setattr(peh, "MultipartFileDownloader", _FakeDownloader)

    handler = PolarsExportHandler(client=mock_client)
    with pytest.raises(RuntimeError):
        handler.export_to_files([make_channel("a")], 0, TEN_SECONDS_NS, tmp_path / "out")
