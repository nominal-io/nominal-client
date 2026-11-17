"""File Store mutation retry behavior over the client's real gRPC transport."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest
from conjure_python_client import ServiceConfiguration

from nominal.core._clientsbunch import ClientsBunch
from nominal.exceptions import NominalError
from nominal.experimental import drives
from nominal.protos.file_store.v1 import file_store_pb2, files_pb2, files_pb2_grpc
from tests.experimental.drives._fixtures import managed_drive, managed_file_proto


@pytest.mark.parametrize("status", [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.RESOURCE_EXHAUSTED])
def test_applied_batch_is_not_replayed_but_reads_still_retry(status: grpc.StatusCode) -> None:
    """A lost mutation response stays ambiguous; reads on the same channel keep their retry policy."""
    mutation_calls = 0
    read_calls = 0

    class Files(files_pb2_grpc.FilesServiceServicer):
        def ApplyFileChanges(self, request, context):
            nonlocal mutation_calls
            mutation_calls += 1
            if mutation_calls == 1:
                # The move was applied, but no response reached the client.
                context.abort(status, "response lost after applying batch")
            return files_pb2.ApplyFileChangesResponse(
                results=[
                    files_pb2.FileChangeResult(
                        failure=files_pb2.FileChangeFailure(
                            common=file_store_pb2.FILE_STORE_COMMON_ERROR_FILE_NOT_FOUND,
                            message="source directory already moved",
                        ),
                    )
                ]
            )

        def GetFile(self, request, context):
            nonlocal read_calls
            read_calls += 1
            if read_calls == 1:
                context.abort(status, "temporary read failure")
            return files_pb2.GetFileResponse(file=managed_file_proto())

    with ThreadPoolExecutor(max_workers=1) as executor:
        server = grpc.server(executor)
        files_pb2_grpc.add_FilesServiceServicer_to_server(Files(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            base_url = f"http://127.0.0.1:{port}/api"
            config = ServiceConfiguration(uris=[base_url], max_num_retries=1, read_timeout=30)
            clients = ClientsBunch.from_config(config, base_url, "test", "local-test", workspace_rid=None)
            drive = managed_drive(clients)
            with pytest.raises(NominalError, match="response lost after applying batch") as error:
                drive.batch().move_directory("a", "b").submit(failure_policy=drives.FailurePolicy.ABORT)
            assert isinstance(error.value.__cause__, grpc.RpcError)
            assert error.value.__cause__.code() == status
            assert mutation_calls == 1
            assert drive.get_file("data/run-001.csv").path == "data/run-001.csv"
            assert read_calls == 2
        finally:
            server.stop(0).wait()
