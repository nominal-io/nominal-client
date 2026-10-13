from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from nominal_api import scout_compute_api

from nominal.experimental.compute import batch_compute_buckets
from nominal.experimental.compute.dsl import NumericExpr


def _failed_result(error_type: str, code: int) -> scout_compute_api.ComputeWithUnitsResult:
    return scout_compute_api.ComputeWithUnitsResult(
        compute_result=scout_compute_api.ComputeNodeResult(
            error=scout_compute_api.ErrorResult(code=code, error_type=error_type)
        ),
        unit_result=scout_compute_api.ComputeUnitResult(single=scout_compute_api.UnitResult(no_unit_available=[])),
    )


def test_batch_compute_buckets_raises_exception_group_of_failures():
    """Failed expressions in a batch surface as a built-in ExceptionGroup with one error per failure."""
    client = MagicMock()
    client._clients.compute.batch_compute_with_units.return_value = scout_compute_api.BatchComputeWithUnitsResponse(
        results=[_failed_result("TIMEOUT", 504), _failed_result("NOT_FOUND", 404)]
    )
    exprs = [NumericExpr.asset_channel("asset", "scope", "a"), NumericExpr.asset_channel("asset", "scope", "b")]

    with pytest.raises(ExceptionGroup) as exc_info:
        batch_compute_buckets(client, exprs, start=0, end=1_000_000_000)

    assert [str(error) for error in exc_info.value.exceptions] == [
        "Failed to compute: TIMEOUT (504)",
        "Failed to compute: NOT_FOUND (404)",
    ]
