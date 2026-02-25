from __future__ import annotations

from typing import Iterable, Protocol

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
