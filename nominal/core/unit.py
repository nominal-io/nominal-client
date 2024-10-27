from __future__ import annotations

from dataclasses import dataclass

from typing_extensions import Self

from nominal._api.combined import datasource_api, scout_asset_api, scout_run_api, scout_units_api


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
