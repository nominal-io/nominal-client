from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Mapping, Sequence, Union

from nominal_api import (
    api,
    scout,
    scout_run_api,
    scout_units_api,
    timeseries_logicalseries_api,
)
from typing_extensions import Self, TypeAlias

logger = logging.getLogger(__name__)


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
    def _from_conjure(cls, api_unit: api.Unit | scout_units_api.Unit | scout_run_api.Unit) -> Self:
        """Construct a Unit from any conjure Unit across all API endpoints"""
        if isinstance(api_unit, api.Unit):
            return cls(name="", symbol=api_unit)
        name = "" if api_unit.name is None else api_unit.name
        return cls(name=name, symbol=api_unit.symbol)


UnitLike: TypeAlias = Union[Unit, str, None]
UnitMapping: TypeAlias = Mapping[str, UnitLike]


def _unit_symbol_from_unit_like(unit: UnitLike) -> str | None:
    if unit is None:
        return None
    elif isinstance(unit, Unit):
        return unit.symbol
    else:
        return unit


def _available_units(auth_header: str, client: scout.UnitsService) -> Sequence[Unit]:
    """Retrieve the list of all allowable units within Nominal"""
    response = client.get_all_units(auth_header)
    return [Unit._from_conjure(unit) for units in response.units_by_property.values() for unit in units]


def _build_unit_update(unit: UnitLike) -> timeseries_logicalseries_api.UnitUpdate:
    """Helper function for building a UnitUpdate enum from a potentially null unit symbol."""
    unit = _unit_symbol_from_unit_like(unit)
    if unit is None:
        return timeseries_logicalseries_api.UnitUpdate(clear_unit=api.Empty())
    else:
        return timeseries_logicalseries_api.UnitUpdate(unit=unit)


def _error_on_invalid_units(unit_map: UnitMapping, unit_service: scout.UnitsService, auth_header: str) -> None:
    # Normalize unit map to refer to channel names and unit symbols
    channels_to_units = {channel: _unit_symbol_from_unit_like(unit) for channel, unit in unit_map.items()}

    resolved_units = unit_service.get_batch_units(
        auth_header, [unit_symbol for unit_symbol in set(channels_to_units.values()) if unit_symbol is not None]
    )

    # Get set of all provided invalid units
    invalid_units = set(channels_to_units.values()) - set(resolved_units.keys())

    # error on invalid units
    for channel, unit_symbol in channels_to_units.items():
        if unit_symbol in invalid_units:
            raise ValueError(
                f"""Unit '{unit_symbol}' for channel '{channel}' is not recognized within Nominal's unit system.
Unit conversions will not be available for this channel.
For more information on valid symbols, see https://ucum.org/ucum
                """
            )
