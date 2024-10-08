from typing import Sequence

from .._api.combined import scout_units_api
from ._clientsbunch import ClientsBunch
from .unit import Unit


def _available_units(clients: ClientsBunch) -> Sequence[Unit]:
    """Retrieve the list of all allowable units within Nominal"""
    response = clients.units.get_all_units(clients.auth_header)
    return [Unit._from_conjure(unit) for units in response.units_by_property.values() for unit in units]
