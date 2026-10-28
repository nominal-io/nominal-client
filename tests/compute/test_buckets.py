from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.experimental.compute import batch_compute_buckets
from nominal.experimental.compute._buckets import batch_compute_enum_buckets
from nominal.experimental.compute.dsl import NumericExpr
from nominal.protos.compute.v1 import compute_pb2, execution_results_pb2, result_common_pb2


def _failed_result(error_type: str, code: int) -> execution_results_pb2.ComputationOutcomeWithUnits:
    return execution_results_pb2.ComputationOutcomeWithUnits(
        outcome=execution_results_pb2.ComputationOutcome(
            failure=result_common_pb2.ComputationFailure(error_type=error_type, http_status_code=code)
        )
    )


def test_batch_compute_buckets_raises_exception_group_of_failures():
    """Failed expressions in a batch surface as a built-in ExceptionGroup with one error per failure."""
    client = MagicMock()
    client._clients.compute.BatchComputeWithUnits.return_value = compute_pb2.BatchComputeWithUnitsResponse(
        results=[_failed_result("TIMEOUT", 504), _failed_result("NOT_FOUND", 404)]
    )
    exprs = [NumericExpr.asset_channel("asset", "scope", "a"), NumericExpr.asset_channel("asset", "scope", "b")]

    with pytest.raises(ExceptionGroup) as exc_info:
        batch_compute_buckets(client, exprs, start=0, end=1_000_000_000)

    assert [str(error) for error in exc_info.value.exceptions] == [
        "Failed to compute: TIMEOUT (504)",
        "Failed to compute: NOT_FOUND (404)",
    ]


@pytest.mark.parametrize("compute", [batch_compute_buckets, batch_compute_enum_buckets])
def test_empty_batch_returns_no_results_without_rpc(compute):
    """Empty one-shot iterables keep returning no results without sending an invalid request."""
    client = MagicMock()
    client._clients.compute.BatchComputeWithUnits.side_effect = AssertionError("Empty batch sent to service")

    assert compute(client, iter(()), start=0, end=1) == []

    client._clients.compute.BatchComputeWithUnits.assert_not_called()
