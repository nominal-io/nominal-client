from __future__ import annotations

from unittest.mock import MagicMock

from nominal.core.client import NominalClient
from nominal.core.workspace import Workspace
from nominal.protos.workspaces.v1 import workspaces_pb2


def _client() -> NominalClient:
    return NominalClient(_clients=MagicMock())


def test_list_workspaces_converts_each_proto_workspace() -> None:
    client = _client()
    resp = workspaces_pb2.GetWorkspacesResponse(workspaces=[workspaces_pb2.Workspace(rid="ri.ws.1", id="a", org="o")])
    client._clients.workspace.GetWorkspaces.return_value = resp  # type: ignore[attr-defined]

    assert client.list_workspaces() == [Workspace(rid="ri.ws.1", id="a", org="o")]


def test_get_workspace_converts_resolved_proto_workspace() -> None:
    client = _client()
    client._clients.resolve_workspace.return_value = workspaces_pb2.Workspace(  # type: ignore[attr-defined]
        rid="ri.ws.1", id="a", org="o"
    )

    assert client.get_workspace("ri.ws.1") == Workspace(rid="ri.ws.1", id="a", org="o")
