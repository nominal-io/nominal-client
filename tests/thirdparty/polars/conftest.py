from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.protos.compute.v1 import (
    categorical_results_pb2,
    execution_results_pb2,
    numeric_results_pb2,
    result_common_pb2,
)
from nominal.protos.types.time import time_pb2


@pytest.fixture
def mock_client():
    """A mock NominalClient for compute API calls."""
    return MagicMock()


@pytest.fixture
def make_numeric_response():
    """Build numeric buckets with the requested counts."""

    def _make(bucket_counts: list[int], bucket_interval_seconds: int = 10):
        return execution_results_pb2.ComputationResult(
            numeric_buckets=numeric_results_pb2.NumericBuckets(
                buckets=[
                    numeric_results_pb2.NumericResultBucket(
                        end=time_pb2.Timestamp(seconds=i * bucket_interval_seconds),
                        statistics=numeric_results_pb2.NumericBucketStatistics(
                            count=count, min=0, max=1, mean=0.5, variance=0.1
                        ),
                    )
                    for i, count in enumerate(bucket_counts)
                ]
            )
        )

    return _make


@pytest.fixture
def make_enum_response():
    """Build categorical buckets with the requested frequencies."""

    def _make(histograms: list[dict[int, int]], bucket_interval_seconds: int = 10):
        return execution_results_pb2.ComputationResult(
            categorical_buckets=categorical_results_pb2.CategoricalBuckets(
                buckets=[
                    categorical_results_pb2.CategoricalResultBucket(
                        end=time_pb2.Timestamp(seconds=i * bucket_interval_seconds),
                        statistics=categorical_results_pb2.CategoricalBucketStatistics(
                            count_by_category_index=histogram
                        ),
                    )
                    for i, histogram in enumerate(histograms)
                ]
            )
        )

    return _make


@pytest.fixture
def make_compute_result():
    """Build a batch outcome, preserving a missing success/failure."""

    def _make(success=None, error=None):
        return execution_results_pb2.ComputationOutcomeWithUnits(
            outcome=execution_results_pb2.ComputationOutcome(
                success=success,
                failure=result_common_pb2.ComputationFailure(error_type=error) if error is not None else None,
            )
        )

    return _make
