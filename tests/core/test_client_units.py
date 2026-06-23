from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core.unit import Unit, _available_units, _error_on_invalid_units
from nominal.protos.units.v1 import units_pb2


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
