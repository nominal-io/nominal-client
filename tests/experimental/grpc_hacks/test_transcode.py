"""Unit tests for the HTTP/JSON transcoding channel.

Each test drives a real generated stub bound to an `HttpTranscodeChannel` whose HTTP session is a
MagicMock, so it asserts transcoding behavior (proto -> HTTP request, HTTP response -> proto, error
mapping) with no network.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
import requests
from conjure_python_client import ServiceConfiguration
from google.protobuf import json_format

from nominal.core._utils.networking import HeaderProvider
from nominal.core.exceptions import (
    HeaderConflictError,
    NominalAlreadyExistsError,
    NominalError,
    NominalInvalidArgumentError,
    NominalNotFoundError,
    NominalPermissionDeniedError,
)
from nominal.experimental.grpc_hacks import HttpTranscodeChannel, TranscodeError
from nominal.experimental.grpc_hacks._transcode import _query_params
from nominal.protos.ingest.v2 import containerized_extractor_pb2 as ce_pb2
from nominal.protos.ingest.v2 import containerized_extractor_pb2_grpc
from nominal.protos.ingest.v2.internal import internal_ingest_service_pb2_grpc
from nominal.protos.registry.v2 import registry_pb2, registry_pb2_grpc
from nominal.protos.units.v1 import units_pb2, units_pb2_grpc
from nominal.protos.workspaces.v1 import workspaces_pb2, workspaces_pb2_grpc

_BASE_URL = "https://api.example.test/api"


def _response(*, status: int = 200, text: str = "{}", content_type: str = "application/json") -> MagicMock:
    """Build a MagicMock standing in for a requests.Response."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status
    resp.text = text
    resp.headers = {"content-type": content_type}
    resp.url = f"{_BASE_URL}/x"
    resp.request = MagicMock(method="POST", url=f"{_BASE_URL}/x")
    return resp


def _channel(session: MagicMock) -> HttpTranscodeChannel:
    """Build a channel whose HTTP session is replaced by `session` (the transport seam)."""
    channel = HttpTranscodeChannel(
        api_base_url=_BASE_URL,
        service_config=ServiceConfiguration(uris=[_BASE_URL]),
        user_agent="test",
        auth_header="Bearer test",
    )
    channel._session = session
    return channel


def test_get_binds_path_field_and_maps_other_fields_to_query() -> None:
    """A GET request substitutes path fields into the URL and sends the rest as query params."""
    session = MagicMock(spec=requests.Session, request=MagicMock(return_value=_response()))
    stub = registry_pb2_grpc.RegistryServiceStub(_channel(session))
    stub.GetImage(registry_pb2.GetImageRequest(rid="ri.image.1", workspace_rid="ri.ws.1"))
    verb, url = session.request.call_args.args
    assert verb == "GET"
    assert url == f"{_BASE_URL}/registry/v2/images/ri.image.1"
    assert session.request.call_args.kwargs["params"] == [("workspaceRid", "ri.ws.1")]
    assert session.request.call_args.kwargs["data"] is None


def test_star_body_excludes_path_bound_fields() -> None:
    """A body:'*' request serializes the whole message except fields already bound to the path."""
    session = MagicMock(spec=requests.Session, request=MagicMock(return_value=_response()))
    stub = containerized_extractor_pb2_grpc.ContainerizedExtractorServiceStub(_channel(session))
    stub.UpdateContainerizedExtractor(ce_pb2.UpdateContainerizedExtractorRequest(rid="ri.ext.1", name="n"))
    verb, url = session.request.call_args.args
    assert verb == "PATCH"
    assert url == f"{_BASE_URL}/extractors/v2/ri.ext.1"
    assert json.loads(session.request.call_args.kwargs["data"]) == {"name": "n"}


def test_named_body_sends_only_that_field() -> None:
    """A body:'<field>' request sends only that field as the body (repeated fields included)."""
    session = MagicMock(spec=requests.Session, request=MagicMock(return_value=_response()))
    stub = units_pb2_grpc.UnitsServiceStub(_channel(session))
    stub.GetBatchUnits(units_pb2.GetBatchUnitsRequest(units=["m", "s"]))
    verb, url = session.request.call_args.args
    assert verb == "POST"
    assert url == f"{_BASE_URL}/units/v1/units/get-batch-units"
    assert json.loads(session.request.call_args.kwargs["data"]) == ["m", "s"]


def test_nonscalar_fields_flatten_into_query_params() -> None:
    """Query mapping expands nested messages to dotted keys and repeats keys for repeated fields."""
    request = registry_pb2.CreateImageRequest(
        workspace_rid="ri.ws.1",
        inputs=[
            registry_pb2.FileExtractionInput(name="a", required=True),
            registry_pb2.FileExtractionInput(name="b", required=False),
        ],
    )
    params = _query_params(json_format.MessageToDict(request, preserving_proto_field_name=False), exclude=set())
    assert ("workspaceRid", "ri.ws.1") in params
    assert ("inputs.name", "a") in params
    assert ("inputs.name", "b") in params
    assert ("inputs.required", "true") in params  # bool rendered lowercase, not "True"
    assert ("inputs.required", "false") in params


def test_response_body_wraps_singular_subfield() -> None:
    """A singular response_body field is wrapped back into the full response message."""
    session = MagicMock(spec=requests.Session, request=MagicMock(return_value=_response(text='{"rid": "ri.ws.1"}')))
    stub = workspaces_pb2_grpc.WorkspaceServiceStub(_channel(session))
    resp = stub.GetDefaultWorkspace(workspaces_pb2.GetDefaultWorkspaceRequest())
    assert resp.workspace.rid == "ri.ws.1"


def test_response_body_wraps_repeated_subfield() -> None:
    """A repeated response_body field parses a JSON array back into the response message."""
    body = '[{"rid": "a"}, {"rid": "b"}]'
    session = MagicMock(spec=requests.Session, request=MagicMock(return_value=_response(text=body)))
    stub = workspaces_pb2_grpc.WorkspaceServiceStub(_channel(session))
    resp = stub.GetWorkspaces(workspaces_pb2.GetWorkspacesRequest())
    assert [w.rid for w in resp.workspaces] == ["a", "b"]


@pytest.mark.parametrize(
    ("error_code", "expected"),
    [
        ("NOT_FOUND", NominalNotFoundError),
        ("INVALID_ARGUMENT", NominalInvalidArgumentError),
        ("ALREADY_EXISTS", NominalAlreadyExistsError),
        ("PERMISSION_DENIED", NominalPermissionDeniedError),
    ],
)
def test_json_error_code_selects_nominal_exception(error_code: str, expected: type[NominalError]) -> None:
    """The JSON error body's errorCode selects the exception, independent of the HTTP status."""
    # status 400 would otherwise map to NominalInvalidArgumentError; errorCode must win.
    session = MagicMock(
        spec=requests.Session,
        request=MagicMock(return_value=_response(status=400, text=json.dumps({"errorCode": error_code}))),
    )
    stub = workspaces_pb2_grpc.WorkspaceServiceStub(_channel(session))
    with pytest.raises(expected):
        stub.GetDefaultWorkspace(workspaces_pb2.GetDefaultWorkspaceRequest())


def test_unrecognized_error_code_falls_back_to_base_error() -> None:
    """An unmapped errorCode and status surface as the base NominalError, not a subclass."""
    session = MagicMock(
        spec=requests.Session,
        request=MagicMock(return_value=_response(status=500, text=json.dumps({"errorCode": "INTERNAL"}))),
    )
    stub = workspaces_pb2_grpc.WorkspaceServiceStub(_channel(session))
    with pytest.raises(NominalError) as exc_info:
        stub.GetDefaultWorkspace(workspaces_pb2.GetDefaultWorkspaceRequest())
    assert type(exc_info.value) is NominalError


def test_non_json_404_raises_route_level_transcode_error() -> None:
    """A non-JSON 404 means the transcoder didn't claim the route, so it raises TranscodeError."""
    session = MagicMock(
        spec=requests.Session,
        request=MagicMock(return_value=_response(status=404, text="<html>404</html>", content_type="text/html")),
    )
    stub = workspaces_pb2_grpc.WorkspaceServiceStub(_channel(session))
    with pytest.raises(TranscodeError):
        stub.GetDefaultWorkspace(workspaces_pb2.GetDefaultWorkspaceRequest())


def test_header_provider_cannot_override_reserved_header() -> None:
    """A header_provider that sets authorization is rejected when the channel is built."""
    header_provider = MagicMock(spec=HeaderProvider)
    header_provider.headers.return_value = {"authorization": "Bearer other"}
    with pytest.raises(HeaderConflictError):
        HttpTranscodeChannel(
            api_base_url=_BASE_URL,
            service_config=ServiceConfiguration(uris=[_BASE_URL]),
            user_agent="test",
            auth_header="Bearer test",
            header_provider=header_provider,
        )


def test_binding_unannotated_service_fails_fast() -> None:
    """Binding a service whose methods lack http annotations fails when the stub is built."""
    channel = HttpTranscodeChannel(
        api_base_url=_BASE_URL,
        service_config=ServiceConfiguration(uris=[_BASE_URL]),
        user_agent="test",
        auth_header="Bearer test",
    )
    with pytest.raises(TranscodeError):
        internal_ingest_service_pb2_grpc.InternalIngestServiceStub(channel)


def test_numeric_error_code_does_not_crash_and_falls_back() -> None:
    """A numeric JSON `code` is ignored rather than crashing, falling back to a NominalError."""
    session = MagicMock(
        spec=requests.Session,
        request=MagicMock(return_value=_response(status=500, text=json.dumps({"code": 13, "message": "boom"}))),
    )
    stub = workspaces_pb2_grpc.WorkspaceServiceStub(_channel(session))
    with pytest.raises(NominalError):
        stub.GetDefaultWorkspace(workspaces_pb2.GetDefaultWorkspaceRequest())


def test_empty_repeated_named_body_sends_json_array() -> None:
    """An empty repeated named-body field is sent as [], not {}, matching its JSON type."""
    session = MagicMock(spec=requests.Session, request=MagicMock(return_value=_response()))
    stub = units_pb2_grpc.UnitsServiceStub(_channel(session))
    stub.GetBatchUnits(units_pb2.GetBatchUnitsRequest())  # units omitted -> empty repeated
    assert json.loads(session.request.call_args.kwargs["data"]) == []
