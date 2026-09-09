"""Exercise run transport, metadata, and error translation over a local gRPC channel."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest

from nominal.core.client import NominalClient
from nominal.core.exceptions import NominalNotFoundError
from nominal.core.run import Run
from nominal.protos.run.v1 import run_service_pb2 as pb
from nominal.protos.run.v1 import run_service_pb2_grpc
from nominal.protos.workspaces.v1 import workspaces_pb2, workspaces_pb2_grpc


@pytest.fixture
def grpc_runtime():
    headers = {}

    class Runs(run_service_pb2_grpc.RunServiceServicer):
        def GetRun(self, request, context):
            headers.update(context.invocation_metadata())
            if request.rid == "missing":
                context.abort(grpc.StatusCode.NOT_FOUND, "missing run")
            return pb.GetRunResponse(run=pb.Run(rid=request.rid, title="from server"))

        def CreateRun(self, request, context):
            headers.update(context.invocation_metadata())
            return pb.CreateRunResponse(run=pb.Run(rid="created", title=request.title, start_time=request.start_time))

        def UpdateRun(self, request, context):
            context.abort(grpc.StatusCode.NOT_FOUND, "missing run")

        def AddDataSourcesToRun(self, request, context):
            context.abort(grpc.StatusCode.NOT_FOUND, "missing run")

        def UpdateRunAttachment(self, request, context):
            context.abort(grpc.StatusCode.NOT_FOUND, "missing run")

        def ArchiveRun(self, request, context):
            context.abort(grpc.StatusCode.NOT_FOUND, "missing run")

        def UnarchiveRun(self, request, context):
            context.abort(grpc.StatusCode.NOT_FOUND, "missing run")

        def SearchRuns(self, request, context):
            if request.next_page_token:
                context.abort(grpc.StatusCode.NOT_FOUND, "missing run")
            return pb.SearchRunsResponse(results=[pb.Run(rid="first")], next_page_token="next")

        def GetRunsByAsset(self, request, context):
            context.abort(grpc.StatusCode.NOT_FOUND, "missing run")

    class Workspaces(workspaces_pb2_grpc.WorkspaceServiceServicer):
        def GetWorkspace(self, request, context):
            return workspaces_pb2.GetWorkspaceResponse(workspace=workspaces_pb2.Workspace(rid=request.workspace_rid))

    with ThreadPoolExecutor(max_workers=1) as executor:
        server = grpc.server(executor)
        workspaces_pb2_grpc.add_WorkspaceServiceServicer_to_server(Workspaces(), server)
        run_service_pb2_grpc.add_RunServiceServicer_to_server(Runs(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            yield (
                NominalClient.from_token(
                    "local-token",
                    base_url=f"http://127.0.0.1:{port}/api",
                    workspace_rid="workspace",
                    extra_headers={"X-Test-Header": "present"},
                ),
                headers,
            )
        finally:
            server.stop(0).wait()


def test_run_calls_preserve_metadata_and_serialize_nanoseconds(grpc_runtime):
    client, headers = grpc_runtime
    run = client.get_run("run")
    assert isinstance(run, Run)
    assert (run.rid, run.name) == ("run", "from server")
    assert headers["authorization"] == "Bearer local-token"
    assert headers["x-test-header"] == "present"
    headers.clear()
    created = client.create_run("name", 1_000_000_007, None)
    assert (created.rid, created.start) == ("created", 1_000_000_007)
    assert headers["authorization"] == "Bearer local-token"
    assert headers["x-test-header"] == "present"


@pytest.mark.parametrize(
    "operation",
    [
        lambda client, run: client.get_run("missing"),
        lambda client, run: run.update(name="new"),
        lambda client, run: run.add_dataset("ref", "dataset"),
        lambda client, run: run.add_attachments(["attachment"]),
        lambda client, run: run.archive(),
        lambda client, run: run.unarchive(),
        lambda client, run: client.search_runs(),
    ],
)
def test_run_failures_use_nominal_errors(grpc_runtime, operation):
    client, _ = grpc_runtime
    run = client.get_run("run")
    with pytest.raises(NominalNotFoundError, match="missing run") as exc:
        operation(client, run)
    assert isinstance(exc.value.__cause__, grpc.RpcError)
