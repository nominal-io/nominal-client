from __future__ import annotations

from dataclasses import dataclass

from nominal_api import security_api_workspace
from typing_extensions import Self

from nominal.core._utils.api_tools import HasRid


@dataclass(frozen=True)
class Workspace(HasRid):
    rid: str
    id: str
    org: str
    display_name: str | None = None

    @classmethod
    def _from_conjure(cls, workspace: security_api_workspace.Workspace) -> Self:
        return cls(
            rid=workspace.rid,
            id=workspace.id,
            org=workspace.org,
            display_name=getattr(workspace, "display_name", None),
        )
