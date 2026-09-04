from __future__ import annotations

import contextlib
import datetime
from unittest.mock import MagicMock

import pytest
from nominal_api import api, scout_compute_api

from nominal.core.channel import ChannelDataType
from nominal.thirdparty.polars.polars_export_handler import (
    PolarsExportHandler,
    _batch_channel_points_per_second,
    _build_channel_groups,
    _extract_bucket_counts,
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
