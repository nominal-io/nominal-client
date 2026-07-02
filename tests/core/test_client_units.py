from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest

from nominal.core.client import NominalClient
from nominal.core.exceptions import NominalPermissionDeniedError
from nominal.core.unit import Unit, _available_units, _error_on_invalid_units
from nominal.protos.units.v1 import units_pb2


def test_available_units_flattens_units_by_property() -> None:
    """get_all_units flattens the units-by-property map into a single flat list of units."""
    stub = MagicMock()
    resp = units_pb2.GetAllUnitsResponse(
        units_by_property={
            "electric": units_pb2.GetAllUnitsResponse.GetUnitsResponseUnitsByPropertyWrapper(
                value=[units_pb2.Unit(name="coulomb", symbol="C"), units_pb2.Unit(name="ampere", symbol="A")]
            )
        }
    )
    stub.GetAllUnits.return_value = resp

    assert _available_units(stub) == [Unit(name="coulomb", symbol="C"), Unit(name="ampere", symbol="A")]


def test_error_on_invalid_units_raises_for_unresolved_symbol() -> None:
    """A unit symbol the service does not resolve raises a ValueError naming the offending symbol."""
    stub = MagicMock()
    # only "C" resolves
    resp = units_pb2.GetBatchUnitsResponse(responses={"C": units_pb2.Unit(name="coulomb", symbol="C")})
    stub.GetBatchUnits.return_value = resp

    with pytest.raises(ValueError, match="not-a-unit"):
        _error_on_invalid_units({"ch1": "C", "ch2": "not-a-unit"}, stub)


def test_error_on_invalid_units_passes_when_all_resolve() -> None:
    """No error is raised when every provided unit symbol resolves."""
    stub = MagicMock()
    resp = units_pb2.GetBatchUnitsResponse(responses={"C": units_pb2.Unit(name="coulomb", symbol="C")})
    stub.GetBatchUnits.return_value = resp

    _error_on_invalid_units({"ch1": "C"}, stub)  # no raise


def test_error_on_invalid_units_none_is_not_invalid() -> None:
    """A channel mapped to None (clear unit) is not treated as an invalid unit symbol."""
    stub = MagicMock()
    resp = units_pb2.GetBatchUnitsResponse(responses={"C": units_pb2.Unit(name="coulomb", symbol="C")})
    stub.GetBatchUnits.return_value = resp

    _error_on_invalid_units({"ch1": None, "ch2": "C"}, stub)  # no raise


def _client() -> NominalClient:
    return NominalClient(_clients=MagicMock())


def test_get_unit_returns_none_when_response_has_no_unit() -> None:
    """An unrecognized symbol returned as a present-but-empty response resolves to None."""
    client = _client()
    client._clients.units.GetUnit.return_value = units_pb2.GetUnitResponse()  # type: ignore[attr-defined]

    assert client.get_unit("not-a-unit") is None


def test_get_unit_returns_none_when_backend_signals_not_found(fake_rpc_error) -> None:
    """An unrecognized symbol signaled via a NOT_FOUND status also resolves to None (contract preserved)."""
    client = _client()
    client._clients.units.GetUnit.side_effect = fake_rpc_error(grpc.StatusCode.NOT_FOUND)  # type: ignore[attr-defined]

    assert client.get_unit("not-a-unit") is None


def test_get_unit_propagates_non_lookup_errors(fake_rpc_error) -> None:
    """A genuine failure (e.g. PERMISSION_DENIED) is not swallowed into None — it propagates."""
    client = _client()
    client._clients.units.GetUnit.side_effect = fake_rpc_error(grpc.StatusCode.PERMISSION_DENIED)  # type: ignore[attr-defined]

    with pytest.raises(NominalPermissionDeniedError):
        client.get_unit("C")
