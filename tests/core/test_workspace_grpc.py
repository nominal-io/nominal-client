"""Exercise workspace calls and error translation over a real plaintext gRPC channel."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest

from nominal.core.client import NominalClient
from nominal.core.exceptions import NominalNotFoundError
from nominal.protos.workspaces.v1 import workspaces_pb2, workspaces_pb2_grpc

WORKSPACE_RID = "ri.security.desktop.workspace.local"


@pytest.fixture
def grpc_runtime():
    """Run a local plaintext workspace service and capture its received metadata."""
    headers = {}

    class Workspaces(workspaces_pb2_grpc.WorkspaceServiceServicer):
        def GetWorkspace(self, request, context):
            headers.update(context.invocation_metadata())
            if request.workspace_rid != WORKSPACE_RID:
                context.abort(grpc.StatusCode.NOT_FOUND, "missing workspace")
            return workspaces_pb2.GetWorkspaceResponse(workspace=workspaces_pb2.Workspace(rid=WORKSPACE_RID))

    with ThreadPoolExecutor(max_workers=1) as executor:
        server = grpc.server(executor)
        workspaces_pb2_grpc.add_WorkspaceServiceServicer_to_server(Workspaces(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            yield f"http://127.0.0.1:{port}/api", headers
        finally:
            server.stop(0).wait()


def test_plaintext_workspace_resolution_preserves_metadata(grpc_runtime):
    """Workspace calls reach the plaintext server with bearer auth and custom metadata intact."""
    base_url, headers = grpc_runtime
    client = NominalClient.from_token(
        "local-token",
        base_url=base_url,
        extra_headers={"X-Test-Header": "present"},
    )
    assert client.get_workspace(WORKSPACE_RID).rid == WORKSPACE_RID
    assert headers["authorization"] == "Bearer local-token"
    assert headers["x-test-header"] == "present"


def test_plaintext_workspace_errors_use_existing_nominal_translation(grpc_runtime):
    """Plaintext gRPC failures retain the existing Nominal exception and underlying cause."""
    base_url, _ = grpc_runtime
    client = NominalClient.from_token("local-token", base_url=base_url)
    with pytest.raises(NominalNotFoundError, match="missing workspace") as exc:
        client.get_workspace("ri.security.desktop.workspace.missing")
    assert isinstance(exc.value.__cause__, grpc.RpcError)
