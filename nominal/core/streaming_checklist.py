from __future__ import annotations

from typing import Iterable, Protocol, Sequence

from nominal_api import scout_checklistexecution_api

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.pagination_tools import (
    list_streaming_checklists_for_asset_paginated,
    list_streaming_checklists_paginated,
)


class _Clients(HasScoutParams, Protocol):
    @property
    def checklist_execution(self) -> scout_checklistexecution_api.ChecklistExecutionService: ...


def _iter_list_streaming_checklists(
    clients: _Clients,
    asset_rid: str | None = None,
) -> Iterable[str]:
    if asset_rid is None:
        return list_streaming_checklists_paginated(clients.checklist_execution, clients.auth_header)
    return list_streaming_checklists_for_asset_paginated(clients.checklist_execution, clients.auth_header, asset_rid)


def _list_streaming_checklists(
    clients: _Clients,
    asset_rid: str | None = None,
) -> Sequence[str]:
    """List all Streaming Checklists.

    Args:
        asset_rid: if provided, only return checklists associated with the given asset.
    """
    return list(_iter_list_streaming_checklists(clients, asset_rid))
