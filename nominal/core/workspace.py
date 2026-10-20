from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Self, Sequence

from nominal.core._datasource_types import DataSourceType
from nominal.core._utils.api_tools import HasRid
from nominal.protos.workspaces.v1 import workspaces_pb2


@dataclass(frozen=True)
class Workspace(HasRid):
    """A workspace used to scope resource creation and searches."""

    rid: str
    id: str
    org: str
    display_name: str | None = None
    """User-facing name, or ``None`` when no display name is configured."""

    preferred_refnames: Mapping[str, DataSourceType] = field(default_factory=dict)
    """Preferred refnames and their datasource categories; empty when unconfigured."""

    preferred_procedure_rids: Sequence[str] = ()
    """Procedure RIDs offered on the application home page; empty when unconfigured."""

    @classmethod
    def _from_proto(cls, workspace: workspaces_pb2.Workspace) -> Self:
        return cls(
            rid=workspace.rid,
            id=workspace.id,
            org=workspace.org,
            display_name=workspace.display_name if workspace.HasField("display_name") else None,
            preferred_refnames={
                refname.name: DataSourceType._from_proto(refname.type) for refname in workspace.settings.ref_names.v1
            },
            preferred_procedure_rids=tuple(workspace.settings.procedures.v1.workspace_procedures),
        )
