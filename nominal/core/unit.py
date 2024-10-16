import dataclasses

from typing_extensions import Self

from .._api.combined import datasource_api, scout_asset_api, scout_run_api, scout_units_api


@dataclasses.dataclass(frozen=True)
class Unit:
    """Plain python wrapper to encapsulate the data from scout_units_api.Unit.

    This is primarily used when setting or retrieving the units of a channel within a dataset.
    """

    # Plain english name of the unit (e.g. 'coulomb')
    name: str

    # Abbreviated symbol for the unit (e.g. 'C')
    # See: https://ucum.org/ucum
    symbol: str

    # General physical property being measured by the unit (e.g. 'electric charge')
    # Units may only be converted to one another if they measure the same property
    property: str | None

    # Physical dimensions in terms of the base units of the system.
    # Units may only be converted to one another if they have the same dimension.
    dimension: dict[str, int] | None

    # System of measurement containing this unit.
    # Currently, all units come from the Universal Code for Units of Measure (UCUM).
    system: str | None

    @classmethod
    def _from_units_api_unit(cls, api_unit: scout_units_api.Unit) -> Self:
        """Construct a Unit from a fully-fleshed-out conjure Unit."""

        if api_unit.name is None:
            raise RuntimeError(f"Cannot deserialize api unit-- missing name: {api_unit}")
        elif api_unit.symbol is None:
            raise RuntimeError(f"Cannot deserialize api unit-- missing symbol: {api_unit}")

        base_dimensions = api_unit.dimension.base_dimensions if api_unit.dimension is not None else None
        return cls(
            name=api_unit.name,
            symbol=api_unit.symbol,
            property=api_unit.property,
            dimension=base_dimensions,
            system=api_unit.system,
        )

    @classmethod
    def _from_simple_api_unit(cls, api_unit: scout_run_api.Unit | scout_asset_api.Unit | datasource_api.Unit) -> Self:
        """Construct a Unit from a simple conjure Unit that contains only the name and symbol, but no associated metadata."""
        return cls(name=api_unit.name, symbol=api_unit.symbol, property=None, dimension=None, system=None)

    @classmethod
    def _from_conjure(
        cls, api_unit: scout_units_api.Unit | scout_run_api.Unit | scout_asset_api.Unit | datasource_api.Unit
    ) -> Self:
        """Construct a Unit from any conjure Unit across all API endpoints"""

        if isinstance(api_unit, scout_units_api.Unit):
            return cls._from_units_api_unit(api_unit)
        else:
            return cls._from_simple_api_unit(api_unit)
