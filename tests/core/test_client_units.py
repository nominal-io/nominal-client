from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest

from nominal.core.client import NominalClient
from nominal.core.exceptions import NominalPermissionDeniedError
from nominal.core.unit import Unit, _available_units, _error_on_invalid_units
from nominal.protos.units.v1 import units_pb2


class _FakeRpcError(grpc.RpcError):
    """A grpc.RpcError with a controllable status code, for exercising error translation."""

    def __init__(self, code: grpc.StatusCode) -> None:
        self._code = code

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return "fake error"


def test_available_units_flattens_units_by_property() -> None:
    stub = MagicMock()
    resp = units_pb2.GetAllUnitsResponse()
    resp.units_by_property["electric"].value.extend(
        [units_pb2.Unit(name="coulomb", symbol="C"), units_pb2.Unit(name="ampere", symbol="A")]
    )
    stub.GetAllUnits.return_value = resp

    assert _available_units(stub) == [Unit(name="coulomb", symbol="C"), Unit(name="ampere", symbol="A")]


def test_error_on_invalid_units_raises_for_unresolved_symbol() -> None:
    stub = MagicMock()
    resp = units_pb2.GetBatchUnitsResponse()
    resp.responses["C"].CopyFrom(units_pb2.Unit(name="coulomb", symbol="C"))  # only "C" resolves
    stub.GetBatchUnits.return_value = resp

    with pytest.raises(ValueError, match="not-a-unit"):
        _error_on_invalid_units({"ch1": "C", "ch2": "not-a-unit"}, stub)


def test_error_on_invalid_units_passes_when_all_resolve() -> None:
    stub = MagicMock()
    resp = units_pb2.GetBatchUnitsResponse()
    resp.responses["C"].CopyFrom(units_pb2.Unit(name="coulomb", symbol="C"))
    stub.GetBatchUnits.return_value = resp

    _error_on_invalid_units({"ch1": "C"}, stub)  # no raise


def test_error_on_invalid_units_none_is_not_invalid() -> None:
    """None (clear unit) should not be treated as an invalid unit symbol."""
    stub = MagicMock()
    resp = units_pb2.GetBatchUnitsResponse()
    resp.responses["C"].CopyFrom(units_pb2.Unit(name="coulomb", symbol="C"))
    stub.GetBatchUnits.return_value = resp

    # ch1=None means "clear unit" — must not raise even though None is not in valid_units
    _error_on_invalid_units({"ch1": None, "ch2": "C"}, stub)  # no raise


def _client() -> NominalClient:
    return NominalClient(_clients=MagicMock())


def test_get_unit_returns_unit_when_present() -> None:
    client = _client()
    client._clients.units.GetUnit.return_value = units_pb2.GetUnitResponse(  # type: ignore[attr-defined]
        unit=units_pb2.Unit(name="coulomb", symbol="C")
    )

    assert client.get_unit("C") == Unit(name="coulomb", symbol="C")


def test_get_unit_returns_none_when_response_has_no_unit() -> None:
    """An unrecognized symbol returned as a present-but-empty response resolves to None."""
    client = _client()
    client._clients.units.GetUnit.return_value = units_pb2.GetUnitResponse()  # type: ignore[attr-defined]

    assert client.get_unit("not-a-unit") is None


def test_get_unit_returns_none_when_backend_signals_not_found() -> None:
    """An unrecognized symbol signaled via a NOT_FOUND status also resolves to None (contract preserved)."""
    client = _client()
    client._clients.units.GetUnit.side_effect = _FakeRpcError(grpc.StatusCode.NOT_FOUND)  # type: ignore[attr-defined]

    assert client.get_unit("not-a-unit") is None


def test_get_unit_propagates_non_lookup_errors() -> None:
    """A genuine failure (e.g. PERMISSION_DENIED) is NOT swallowed into None — it propagates."""
    client = _client()
    client._clients.units.GetUnit.side_effect = _FakeRpcError(grpc.StatusCode.PERMISSION_DENIED)  # type: ignore[attr-defined]

    with pytest.raises(NominalPermissionDeniedError):
        client.get_unit("C")
