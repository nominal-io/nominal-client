from __future__ import annotations

from dataclasses import dataclass

from typing_extensions import Self

from nominal.core._utils.api_tools import HasRid
from nominal.protos.workspaces.v1 import workspaces_pb2


@dataclass(frozen=True)
class Workspace(HasRid):
    rid: str
    id: str
    org: str

    @classmethod
    def _from_proto(cls, workspace: workspaces_pb2.Workspace) -> Self:
        return cls(rid=workspace.rid, id=workspace.id, org=workspace.org)
