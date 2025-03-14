from __future__ import annotations

from dataclasses import dataclass

from nominal_api import api, scout_run_api, scout_units_api
from typing_extensions import Self


@dataclass(frozen=True)
class Unit:
    """Combination of the name and symbol of a unit within the supported systems of measurement.

    This is primarily used when setting or retrieving the units of a channel within a dataset.
    """

    _name: str
    _symbol: str

    @property
    def name(self) -> str:
        """Plain english name of the unit (e.g. 'coulomb')"""
        return self._name

    @property
    def symbol(self) -> str:
        """Abbreviated symbol for the unit (e.g. 'C')
        See: https://ucum.org/ucum
        """
        return self._symbol

    @classmethod
    def _from_conjure(cls, api_unit: api.Unit | scout_units_api.Unit | scout_run_api.Unit) -> Self:
        """Construct a Unit from any conjure Unit across all API endpoints"""
        if isinstance(api_unit, api.Unit):
            return cls(_name="", _symbol=api_unit)
        name = "" if api_unit.name is None else api_unit.name
        return cls(_name=name, _symbol=api_unit.symbol)
