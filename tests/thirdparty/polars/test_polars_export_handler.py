from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from nominal_api import api, scout_compute_api

from nominal.core.channel import Channel, ChannelDataType
from nominal.thirdparty.polars.polars_export_handler import (
    _batch_channel_points_per_second,
    _extract_bucket_counts,
    _max_points_per_second,
)


@pytest.fixture
def mock_clients():
    clients = MagicMock()
    clients.auth_header = "Bearer test-token"
    return clients


@pytest.fixture
def mock_client():
    """A mock NominalClient for _batch_channel_points_per_second."""
    return MagicMock()


@pytest.fixture
def make_channel(mock_clients):
    """Factory fixture that creates Channel instances sharing the same mock clients."""
    def _make(name: str, data_type: ChannelDataType | None = ChannelDataType.DOUBLE, data_source: str = "ds-1"):
        return Channel(
            name=name, data_source=data_source, data_type=data_type,
            unit=None, description=None, _clients=mock_clients,
        )
    return _make


@pytest.fixture
def make_numeric_response():
    """Factory fixture that builds a mock bucketed numeric ComputeNodeResponse."""
    def _make(bucket_counts: list[int], bucket_interval_seconds: int = 10):
        response = MagicMock(spec=scout_compute_api.ComputeNodeResponse)
        response.bucketed_numeric = MagicMock()
        response.bucketed_numeric.timestamps = [
            api.Timestamp(seconds=i * bucket_interval_seconds, nanos=0)
            for i in range(len(bucket_counts))
        ]
        response.bucketed_numeric.buckets = [
            scout_compute_api.NumericBucket(
                count=c, min=0.0, max=1.0, mean=0.5, variance=0.1,
                first_point=MagicMock(), last_point=MagicMock(),
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
    """Factory fixture that builds a mock bucketed enum ComputeNodeResponse."""
    def _make(histograms: list[dict[int, int]], bucket_interval_seconds: int = 10):
        response = MagicMock(spec=scout_compute_api.ComputeNodeResponse)
        response.bucketed_numeric = None
        response.numeric = None
        response.numeric_point = None
        response.bucketed_enum = MagicMock()
        response.bucketed_enum.timestamps = [
            api.Timestamp(seconds=i * bucket_interval_seconds, nanos=0)
            for i in range(len(histograms))
        ]
        response.bucketed_enum.buckets = [
            scout_compute_api.EnumBucket(histogram=h, first_point=MagicMock(), last_point=None)
            for h in histograms
        ]
        response.enum = None
        return response
    return _make


def _wrap_compute_result(response):
    """Wrap a ComputeNodeResponse in the BatchComputeWithUnitsResponse structure."""
    result = MagicMock()
    result.compute_result = MagicMock()
    result.compute_result.error = None
    result.compute_result.success = response
    return result


# -- _extract_bucket_counts --


def test_extract_numeric_bucketed(make_numeric_response):
    """Extracts (timestamp, count) from a bucketed numeric response."""
    result = _extract_bucket_counts(make_numeric_response([50, 75]))
    assert result == [(0, 50), (10_000_000_000, 75)]


def test_extract_numeric_undecimated():
    """Undecimated numeric data returns count=1 per point."""
    response = MagicMock(spec=scout_compute_api.ComputeNodeResponse)
    response.bucketed_numeric = None
    response.numeric = MagicMock()
    response.numeric.timestamps = [api.Timestamp(seconds=s, nanos=0) for s in (100, 200, 300)]
    response.numeric_point = None
    response.bucketed_enum = None
    response.enum = None

    result = _extract_bucket_counts(response)

    assert len(result) == 3
    assert all(count == 1 for _, count in result)


def test_extract_numeric_single_point():
    """A single numeric point returns one entry with count=1."""
    response = MagicMock(spec=scout_compute_api.ComputeNodeResponse)
    response.bucketed_numeric = None
    response.numeric = None
    response.numeric_point = MagicMock()
    response.numeric_point.timestamp = api.Timestamp(seconds=100, nanos=0)
    response.bucketed_enum = None
    response.enum = None

    assert _extract_bucket_counts(response) == [(100_000_000_000, 1)]


def test_extract_enum_bucketed(make_enum_response):
    """Point counts are extracted from enum histograms by summing frequencies."""
    result = _extract_bucket_counts(make_enum_response([{0: 30, 1: 20}, {0: 10}]))
    assert result == [(0, 50), (10_000_000_000, 10)]


def test_extract_enum_undecimated():
    """Undecimated enum data returns count=1 per point."""
    response = MagicMock(spec=scout_compute_api.ComputeNodeResponse)
    response.bucketed_numeric = None
    response.numeric = None
    response.numeric_point = None
    response.bucketed_enum = None
    response.enum = MagicMock()
    response.enum.timestamps = [api.Timestamp(seconds=s, nanos=0) for s in (100, 200)]

    assert all(count == 1 for _, count in _extract_bucket_counts(response))


def test_extract_unrecognized_response_returns_empty():
    """An unrecognized response type returns an empty list."""
    response = MagicMock(spec=scout_compute_api.ComputeNodeResponse)
    response.bucketed_numeric = None
    response.numeric = None
    response.numeric_point = None
    response.bucketed_enum = None
    response.enum = None
    response.type = "something_unknown"

    assert _extract_bucket_counts(response) == []


# -- _max_points_per_second --


def test_max_pps_empty_buckets():
    """No buckets means zero PPS."""
    assert _max_points_per_second([], 0, 10_000_000_000) == 0.0


def test_max_pps_single_bucket():
    """Single bucket uses the full time range as the duration."""
    result = _max_points_per_second([(5_000_000_000, 100)], start_ns=0, end_ns=10_000_000_000)
    assert result == pytest.approx(10.0)


def test_max_pps_returns_peak_across_buckets():
    """With multiple buckets, returns the maximum PPS across consecutive pairs."""
    buckets = [
        (1_000_000_000, 10),   # 1s
        (2_000_000_000, 100),  # 2s — 100 pts in 1s = 100 PPS (peak)
        (3_000_000_000, 20),   # 3s — 20 pts in 1s = 20 PPS
    ]
    assert _max_points_per_second(buckets, start_ns=0, end_ns=3_000_000_000) == pytest.approx(100.0)


# -- _batch_channel_points_per_second --


def test_pps_for_double_channels(mock_client, make_channel, make_numeric_response):
    """DOUBLE channels get PPS values from bucketed numeric responses."""
    mock_client._clients.compute.batch_compute_with_units.return_value = MagicMock(
        results=[_wrap_compute_result(make_numeric_response([50, 100]))]
    )

    result = _batch_channel_points_per_second(mock_client, [make_channel("temp")], 0, 20_000_000_000, {}, 100)

    assert "temp" in result
    assert result["temp"] > 0


def test_pps_for_int_channels(mock_client, make_channel, make_numeric_response):
    """INT channels get PPS values — the backend casts INT to DOUBLE transparently."""
    mock_client._clients.compute.batch_compute_with_units.return_value = MagicMock(
        results=[_wrap_compute_result(make_numeric_response([50, 100]))]
    )

    result = _batch_channel_points_per_second(
        mock_client, [make_channel("counter", ChannelDataType.INT)], 0, 20_000_000_000, {}, 100,
    )

    assert "counter" in result
    assert result["counter"] > 0


def test_pps_for_string_channels(mock_client, make_channel, make_enum_response):
    """STRING channels get PPS values via enum histogram decimation."""
    mock_client._clients.compute.batch_compute_with_units.return_value = MagicMock(
        results=[_wrap_compute_result(make_enum_response([{0: 30, 1: 20}, {0: 10}]))]
    )

    result = _batch_channel_points_per_second(
        mock_client, [make_channel("status", ChannelDataType.STRING)], 0, 20_000_000_000, {}, 100,
    )

    assert "status" in result
    assert result["status"] > 0


def test_unknown_channels_get_none_pps(mock_client, make_channel):
    """UNKNOWN channels cannot be decimated and get None PPS."""
    mock_client._clients.compute.batch_compute_with_units.return_value = MagicMock(results=[])

    result = _batch_channel_points_per_second(
        mock_client, [make_channel("mystery", ChannelDataType.UNKNOWN)], 0, 10_000_000_000, {}, 100,
    )

    assert result["mystery"] is None
