from __future__ import annotations

from nominal.core.workspace import Workspace
from nominal.protos.workspaces.v1 import workspaces_pb2


def test_workspace_from_proto_maps_rid_id_org() -> None:
    proto = workspaces_pb2.Workspace(rid="ri.workspace.1", id="ws1", org="acme")
    assert Workspace._from_proto(proto) == Workspace(rid="ri.workspace.1", id="ws1", org="acme")
