from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from nominal_api import api, scout_compute_api


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
