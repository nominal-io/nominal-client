from __future__ import annotations

import datetime
from unittest.mock import MagicMock

import pytest
from nominal_api import api, scout_compute_api

from nominal.core.channel import ChannelDataType
from nominal.thirdparty.polars.polars_export_handler import (
    _batch_channel_points_per_second,
    _build_channel_groups,
    _extract_bucket_counts,
    _max_points_per_second,
)

# -- Fixtures --


@pytest.fixture
def mock_client():
    """A mock NominalClient for compute API calls."""
    return MagicMock()


@pytest.fixture
def make_numeric_response():
    """Factory that builds a bucketed numeric ComputeNodeResponse."""

    def _make(bucket_counts: list[int], bucket_interval_seconds: int = 10):
        response = MagicMock(spec=scout_compute_api.ComputeNodeResponse)
        response.bucketed_numeric = MagicMock()
        response.bucketed_numeric.timestamps = [
            api.Timestamp(seconds=i * bucket_interval_seconds, nanos=0) for i in range(len(bucket_counts))
        ]
        response.bucketed_numeric.buckets = [
            scout_compute_api.NumericBucket(
                count=c,
                min=0.0,
                max=1.0,
                mean=0.5,
                variance=0.1,
                first_point=MagicMock(),
                last_point=MagicMock(),
            )
            for c in bucket_counts
        ]
        response.numeric = None
        response.numeric_point = None
        response.bucketed_enum = None
        response.enum = None
        return response

    return _make


@pytest.fixture
def make_enum_response():
    """Factory that builds a bucketed enum ComputeNodeResponse."""

    def _make(histograms: list[dict[int, int]], bucket_interval_seconds: int = 10):
        response = MagicMock(spec=scout_compute_api.ComputeNodeResponse)
        response.bucketed_numeric = None
        response.numeric = None
        response.numeric_point = None
        response.bucketed_enum = MagicMock()
        response.bucketed_enum.timestamps = [
            api.Timestamp(seconds=i * bucket_interval_seconds, nanos=0) for i in range(len(histograms))
        ]
        response.bucketed_enum.buckets = [
            scout_compute_api.EnumBucket(histogram=h, first_point=MagicMock(), last_point=None) for h in histograms
        ]
        response.enum = None
        return response

    return _make


@pytest.fixture
def make_compute_result():
    """Factory that builds a single compute result entry for batch responses."""

    def _make(success=None, error=None):
        result = MagicMock()
        result.compute_result = MagicMock()
        result.compute_result.error = error
        result.compute_result.success = success
        return result

    return _make


def _make_response(spec):
    """Create a ComputeNodeResponse with all variant fields set to None."""
    response = MagicMock(spec=spec)
    response.bucketed_numeric = None
    response.numeric = None
    response.numeric_point = None
    response.bucketed_enum = None
    response.enum = None
    return response


# -- _extract_bucket_counts --


def test_extract_numeric_bucketed(make_numeric_response):
    """Bucketed numeric response yields (timestamp, count) pairs."""
    result = _extract_bucket_counts(make_numeric_response([50, 75]))
    assert result == [(0, 50), (10_000_000_000, 75)]


def test_extract_numeric_undecimated():
    """Undecimated numeric series yields count=1 per timestamp."""
    response = _make_response(scout_compute_api.ComputeNodeResponse)
    response.numeric = MagicMock()
    response.numeric.timestamps = [api.Timestamp(seconds=s, nanos=0) for s in (100, 200, 300)]

    result = _extract_bucket_counts(response)

    assert len(result) == 3
    assert all(count == 1 for _, count in result)


def test_extract_numeric_single_point():
    """A single numeric point yields one entry with count=1."""
    response = _make_response(scout_compute_api.ComputeNodeResponse)
    response.numeric_point = MagicMock()
    response.numeric_point.timestamp = api.Timestamp(seconds=100, nanos=0)

    assert _extract_bucket_counts(response) == [(100_000_000_000, 1)]


def test_extract_enum_bucketed(make_enum_response):
    """Bucketed enum response sums histogram frequencies per bucket."""
    result = _extract_bucket_counts(make_enum_response([{0: 30, 1: 20}, {0: 10}]))
    assert result == [(0, 50), (10_000_000_000, 10)]


def test_extract_enum_undecimated():
    """Undecimated enum series yields count=1 per timestamp."""
    response = _make_response(scout_compute_api.ComputeNodeResponse)
    response.enum = MagicMock()
    response.enum.timestamps = [api.Timestamp(seconds=s, nanos=0) for s in (100, 200)]

    assert all(count == 1 for _, count in _extract_bucket_counts(response))


def test_extract_unrecognized_response_returns_empty():
    """An unrecognized response shape returns an empty list."""
    response = _make_response(scout_compute_api.ComputeNodeResponse)
    response.type = "something_unknown"

    assert _extract_bucket_counts(response) == []


# -- _max_points_per_second --


def test_max_pps_empty_buckets():
    """No buckets yields zero PPS."""
    assert _max_points_per_second([], 0, 10_000_000_000) == 0.0


def test_max_pps_single_bucket():
    """A single bucket divides its count by the full time range."""
    result = _max_points_per_second([(5_000_000_000, 100)], start_ns=0, end_ns=10_000_000_000)
    assert result == pytest.approx(10.0)


def test_max_pps_returns_peak_across_buckets():
    """Multiple buckets return the highest PPS between any consecutive pair."""
    buckets = [
        (1_000_000_000, 10),
        (2_000_000_000, 100),
        (3_000_000_000, 20),
    ]
    assert _max_points_per_second(buckets, start_ns=0, end_ns=3_000_000_000) == pytest.approx(100.0)


def test_max_pps_zero_duration_returns_zero():
    """A zero-length time range returns 0 instead of dividing by zero."""
    assert _max_points_per_second([(5_000_000_000, 100)], start_ns=0, end_ns=0) == 0.0


def test_max_pps_skips_duplicate_timestamps():
    """Consecutive buckets with identical timestamps are skipped, not divided by zero."""
    buckets = [
        (1_000_000_000, 10),
        (1_000_000_000, 50),
        (2_000_000_000, 20),
    ]
    # Only the 1s→2s interval contributes: 20 pts / 1s = 20 PPS
    assert _max_points_per_second(buckets, start_ns=0, end_ns=2_000_000_000) == pytest.approx(20.0)


# -- _batch_channel_points_per_second --


def test_pps_double_channels(mock_client, make_channel, make_numeric_response, make_compute_result):
    """DOUBLE channels produce positive PPS from bucketed numeric responses."""
    mock_client._clients.compute.batch_compute_with_units.return_value = MagicMock(
        results=[make_compute_result(success=make_numeric_response([50, 100]))]
    )

    result = _batch_channel_points_per_second(mock_client, [make_channel("temp")], 0, 20_000_000_000, {}, 100)

    assert result["temp"] is not None and result["temp"] > 0


def test_pps_int_channels(mock_client, make_channel, make_numeric_response, make_compute_result):
    """INT channels produce PPS values (backend treats them as numeric)."""
    mock_client._clients.compute.batch_compute_with_units.return_value = MagicMock(
        results=[make_compute_result(success=make_numeric_response([50, 100]))]
    )

    result = _batch_channel_points_per_second(
        mock_client, [make_channel("counter", ChannelDataType.INT)], 0, 20_000_000_000, {}, 100
    )

    assert result["counter"] is not None and result["counter"] > 0


def test_pps_string_channels(mock_client, make_channel, make_enum_response, make_compute_result):
    """STRING channels produce PPS values from enum histogram responses."""
    mock_client._clients.compute.batch_compute_with_units.return_value = MagicMock(
        results=[make_compute_result(success=make_enum_response([{0: 30, 1: 20}, {0: 10}]))]
    )

    result = _batch_channel_points_per_second(
        mock_client, [make_channel("status", ChannelDataType.STRING)], 0, 20_000_000_000, {}, 100
    )

    assert result["status"] is not None and result["status"] > 0


def test_pps_unknown_channels_return_none(mock_client, make_channel):
    """Channels with unsupported types get None PPS without calling the API."""
    mock_client._clients.compute.batch_compute_with_units.return_value = MagicMock(results=[])

    result = _batch_channel_points_per_second(
        mock_client, [make_channel("mystery", ChannelDataType.UNKNOWN)], 0, 10_000_000_000, {}, 100
    )

    assert result["mystery"] is None


def test_pps_empty_channels_skips_api(mock_client):
    """An empty channel list returns empty results without calling the compute API."""
    result = _batch_channel_points_per_second(mock_client, [], 0, 10_000_000_000, {}, 100)

    assert result == {}
    mock_client._clients.compute.batch_compute_with_units.assert_not_called()


def test_pps_api_failure_returns_none_for_all(mock_client, make_channel):
    """If the compute API raises, all channels get None PPS."""
    mock_client._clients.compute.batch_compute_with_units.side_effect = RuntimeError("API down")

    result = _batch_channel_points_per_second(
        mock_client, [make_channel("a"), make_channel("b")], 0, 10_000_000_000, {}, 100
    )

    assert result["a"] is None
    assert result["b"] is None


def test_pps_per_channel_error_returns_none_for_failed(
    mock_client, make_channel, make_numeric_response, make_compute_result
):
    """Individual channel errors produce None for that channel; others succeed."""
    mock_client._clients.compute.batch_compute_with_units.return_value = MagicMock(
        results=[
            make_compute_result(success=make_numeric_response([50, 100])),
            make_compute_result(error="channel not found"),
        ]
    )

    result = _batch_channel_points_per_second(
        mock_client, [make_channel("good"), make_channel("bad")], 0, 20_000_000_000, {}, 100
    )

    assert result["good"] is not None and result["good"] > 0
    assert result["bad"] is None


# -- _build_channel_groups --


def test_groups_separate_numeric_and_string_channels(make_channel):
    """Numeric and string channels are never placed in the same group."""
    channels = {
        "temp": make_channel("temp", ChannelDataType.DOUBLE),
        "pressure": make_channel("pressure", ChannelDataType.INT),
        "status": make_channel("status", ChannelDataType.STRING),
    }
    pps = {"temp": 100.0, "pressure": 100.0, "status": 50.0}

    groups, large = _build_channel_groups(
        pps, channels, points_per_request=1_000_000, channels_per_request=100,
        batch_duration=datetime.timedelta(seconds=10),
    )

    assert large == []
    assert len(groups) >= 2
    for group in groups:
        has_numeric = any(ch.data_type in (ChannelDataType.DOUBLE, ChannelDataType.INT) for ch in group)
        has_string = any(ch.data_type == ChannelDataType.STRING for ch in group)
        assert not (has_numeric and has_string), "Numeric and string channels must not share a group"


def test_groups_split_when_rate_budget_exceeded(make_channel):
    """Channels are split into new groups when cumulative PPS exceeds the budget."""
    channels = {f"ch{i}": make_channel(f"ch{i}") for i in range(5)}
    pps = {f"ch{i}": 200.0 for i in range(5)}

    groups, large = _build_channel_groups(
        pps, channels, points_per_request=500, channels_per_request=100,
        batch_duration=datetime.timedelta(seconds=1),
    )

    # 5 channels at 200 PPS, budget 500 PPS → groups of 2, 2, 1
    assert len(groups) == 3
    assert large == []


def test_groups_isolate_high_rate_channels(make_channel):
    """Channels exceeding the per-group rate budget are returned as large channels."""
    channels = {"fast": make_channel("fast"), "slow": make_channel("slow")}
    pps = {"fast": 10_000.0, "slow": 100.0}

    groups, large = _build_channel_groups(
        pps, channels, points_per_request=500, channels_per_request=100,
        batch_duration=datetime.timedelta(seconds=1),
    )

    assert [ch.name for ch in large] == ["fast"]
    assert len(groups) == 1 and groups[0][0].name == "slow"


def test_groups_respect_max_channels_per_request(make_channel):
    """Groups are capped at the configured max channels per request."""
    channels = {f"ch{i}": make_channel(f"ch{i}") for i in range(10)}
    pps = {f"ch{i}": 1.0 for i in range(10)}

    groups, large = _build_channel_groups(
        pps, channels, points_per_request=1_000_000, channels_per_request=3,
        batch_duration=datetime.timedelta(seconds=1),
    )

    assert all(len(g) <= 3 for g in groups)
    assert sum(len(g) for g in groups) == 10
    assert large == []


def test_groups_empty_input():
    """Empty inputs produce no groups and no large channels."""
    groups, large = _build_channel_groups(
        {}, {}, points_per_request=1_000_000, channels_per_request=100,
        batch_duration=datetime.timedelta(seconds=1),
    )

    assert groups == []
    assert large == []
