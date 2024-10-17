from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import Sequence

from typing_extensions import Self

from .._api.combined import (
    datasource_api,
    scout_asset_api,
    scout_run_api,
    scout_units_api,
    timeseries_logicalseries_api,
)
from ._clientsbunch import ClientsBunch


@dataclass(frozen=True)
class Unit:
    """Combination of the name and symbol of a unit within the supported systems of measurement.

    This is primarily used when setting or retrieving the units of a channel within a dataset.
    """

    name: str
    """Plain english name of the unit (e.g. 'coulomb')"""

    symbol: str
    """ Abbreviated symbol for the unit (e.g. 'C')
    See: https://ucum.org/ucum
    """

    @classmethod
    def _from_conjure(
        cls, api_unit: scout_units_api.Unit | scout_run_api.Unit | scout_asset_api.Unit | datasource_api.Unit
    ) -> Self:
        """Construct a Unit from any conjure Unit across all API endpoints"""
        name = "" if api_unit.name is None else api_unit.name
        return cls(name=name, symbol=api_unit.symbol)


def _get_unit(clients: ClientsBunch, unit_symbol: str) -> Unit | None:
    api_unit = clients.units.get_unit(clients.auth_header, unit_symbol)
    return None if api_unit is None else Unit._from_conjure(api_unit)


@functools.cache
def _available_units(clients: ClientsBunch) -> Sequence[Unit]:
    """Retrieve the list of all allowable units within Nominal"""
    response = clients.units.get_all_units(clients.auth_header)
    return [Unit._from_conjure(unit) for units in response.units_by_property.values() for unit in units]


def _build_unit_update(unit_symbol: Unit | str | None) -> timeseries_logicalseries_api.UnitUpdate:
    """Helper function for building a UnitUpdate enum from a potentially null unit symbol."""
    if unit_symbol is None:
        return timeseries_logicalseries_api.UnitUpdate(clear_unit=timeseries_logicalseries_api.Empty())
    elif isinstance(unit_symbol, Unit):
        return timeseries_logicalseries_api.UnitUpdate(unit=unit_symbol.symbol)
    else:
        return timeseries_logicalseries_api.UnitUpdate(unit=unit_symbol)
