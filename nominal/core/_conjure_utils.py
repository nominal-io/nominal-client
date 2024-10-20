from __future__ import annotations

from typing import Sequence

from nominal._api.combined import timeseries_logicalseries_api
from nominal.core._clientsbunch import ClientsBunch
from nominal.core.unit import Unit


def _available_units(clients: ClientsBunch) -> Sequence[Unit]:
    """Retrieve the list of all allowable units within Nominal"""
    response = clients.units.get_all_units(clients.auth_header)
    return [Unit._from_conjure(unit) for units in response.units_by_property.values() for unit in units]


def _build_unit_update(symbol: str | None) -> timeseries_logicalseries_api.UnitUpdate:
    """Helper function for building a UnitUpdate enum from a potentially null unit symbol."""
    if symbol is None:
        return timeseries_logicalseries_api.UnitUpdate(clear_unit=timeseries_logicalseries_api.Empty())
    else:
        return timeseries_logicalseries_api.UnitUpdate(unit=symbol)
