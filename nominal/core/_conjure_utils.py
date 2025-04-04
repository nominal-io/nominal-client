from __future__ import annotations

from typing import Sequence

from nominal_api import scout_run_api
from typing_extensions import TypeAlias

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
