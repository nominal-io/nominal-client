from __future__ import annotations

from nominal.core.unit import Unit
from nominal.protos.units.v1 import units_pb2


def test_unit_from_proto_maps_name_and_symbol() -> None:
    assert Unit._from_proto(units_pb2.Unit(name="coulomb", symbol="C")) == Unit(name="coulomb", symbol="C")
