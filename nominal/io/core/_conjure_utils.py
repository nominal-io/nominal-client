from __future__ import annotations

from typing import Sequence

from nominal_api import api, scout, scout_run_api, timeseries_logicalseries_api
from typing_extensions import TypeAlias

from nominal.core.unit import Unit


def _available_units(auth_header: str, client: scout.UnitsService) -> Sequence[Unit]:
    """Retrieve the list of all allowable units within Nominal"""
    response = client.get_all_units(auth_header)
    return [Unit._from_conjure(unit) for units in response.units_by_property.values() for unit in units]


def _build_unit_update(symbol: str | None) -> timeseries_logicalseries_api.UnitUpdate:
    """Helper function for building a UnitUpdate enum from a potentially null unit symbol."""
    if symbol is None:
        return timeseries_logicalseries_api.UnitUpdate(clear_unit=api.Empty())
    else:
        return timeseries_logicalseries_api.UnitUpdate(unit=symbol)


Link: TypeAlias = tuple[str, str]


def _build_links(links: Sequence[str] | Sequence[Link] | None) -> list[scout_run_api.Link] | None:
    if links is None:
        return None
    links_conjure = []
    for link in links:
        if isinstance(link, tuple):
            url, title = link
            links_conjure.append(scout_run_api.Link(url=url, title=title))
        else:
            links_conjure.append(scout_run_api.Link(url=link))
    return links_conjure
