from __future__ import annotations

from typing import Any, Callable

from nominal.core.nominal_hosted_extractor import (
    ContainerImage,
    FileExtractionInput,
    FileOutputFormat,
    NominalHostedExtractor,
    TimestampMetadata,
)
from nominal.protos.ingest.v2 import containerized_extractor_pb2 as _extractor_pb2
from nominal.protos.registry.v2 import registry_pb2 as _registry_pb2
from nominal.protos.types.time import timestamp_parsers_pb2

_WORKSPACE = "ri.security.test.workspace.0"
_EXTRACTOR = "ri.scout.test.containerized-extractor.0"
_IMAGE = "ri.scout.test.container-image.0"


class _RecordingStub:
    """Fake gRPC stub: any RPC method records its request and returns the next queued response."""

    def __init__(self, responses: list[Any] | None = None) -> None:
        self.requests: list[Any] = []
        self._responses = list(responses or [])

    def __getattr__(self, _method: str) -> Callable[[Any], Any]:
        def call(request: Any) -> Any:
            self.requests.append(request)
            return self._responses.pop(0)

        return call


class _FakeClients:
    """Minimal `_Clients`: both v2 stubs share one recorder, plus a default workspace."""

    def __init__(self, stub: _RecordingStub, workspace: str = _WORKSPACE) -> None:
        self._stub = stub
        self._workspace = workspace

    @property
    def nominal_hosted_extractors(self) -> _RecordingStub:
        return self._stub

    @property
    def registry(self) -> _RecordingStub:
        return self._stub

    def resolve_default_workspace_rid(self) -> str:
        return self._workspace


def _extractor_proto(**overrides: Any) -> _extractor_pb2.ContainerizedExtractor:
    fields: dict[str, Any] = {"rid": _EXTRACTOR, "workspace_rid": _WORKSPACE, "name": "ulog-parser"}
    fields.update(overrides)
    return _extractor_pb2.ContainerizedExtractor(**fields)


def _image_proto(**overrides: Any) -> _registry_pb2.ContainerImage:
    fields: dict[str, Any] = {"rid": _IMAGE, "tag": "v1", "extractor_rid": _EXTRACTOR}
    fields.update(overrides)
    return _registry_pb2.ContainerImage(**fields)


def _iso_metadata() -> TimestampMetadata:
    return TimestampMetadata(
        series_name="ts",
        timestamp_type=timestamp_parsers_pb2.TimestampType(
            absolute=timestamp_parsers_pb2.AbsoluteTimestamp(iso8601=timestamp_parsers_pb2.Iso8601Timestamp())
        ),
    )


def test_create_defaults_workspace_and_omits_description() -> None:
    stub = _RecordingStub([_extractor_pb2.CreateContainerizedExtractorResponse(extractor=_extractor_proto())])
    extractor = NominalHostedExtractor._create(_FakeClients(stub), "ulog-parser", description=None, workspace_rid=None)

    assert extractor.rid == _EXTRACTOR
    assert extractor.name == "ulog-parser"
    assert extractor.description is None
    request = stub.requests[0]
    assert request.workspace_rid == _WORKSPACE
    assert request.name == "ulog-parser"
    assert request.description == ""  # proto default: never set


def test_create_uses_explicit_workspace_and_description() -> None:
    stub = _RecordingStub(
        [_extractor_pb2.CreateContainerizedExtractorResponse(extractor=_extractor_proto(description="parses ulog"))]
    )
    NominalHostedExtractor._create(
        _FakeClients(stub),
        "ulog-parser",
        description="parses ulog",
        workspace_rid="ri.security.test.workspace.other",
    )

    request = stub.requests[0]
    assert request.workspace_rid == "ri.security.test.workspace.other"
    assert request.description == "parses ulog"


def test_from_proto_maps_optional_fields() -> None:
    proto = _extractor_proto(
        description="parses ulog",
        is_archived=True,
        active_container_image=_registry_pb2.ContainerImage(rid=_IMAGE, tag="v1"),
    )
    extractor = NominalHostedExtractor._from_proto(_FakeClients(_RecordingStub()), proto)

    assert extractor.description == "parses ulog"
    assert extractor.is_archived is True
    assert extractor.active_container_image_rid == _IMAGE


def test_archive_sets_is_archived_flag() -> None:
    stub = _RecordingStub(
        [_extractor_pb2.UpdateContainerizedExtractorResponse(extractor=_extractor_proto(is_archived=True))]
    )
    extractor = NominalHostedExtractor._from_proto(_FakeClients(stub), _extractor_proto())
    extractor.archive()

    request = stub.requests[0]
    assert request.rid == _EXTRACTOR
    assert request.workspace_rid == _WORKSPACE
    assert request.is_archived is True
    assert extractor.is_archived is True  # instance refreshed from the response


def test_set_active_image_accepts_rid_or_image() -> None:
    stub = _RecordingStub(
        [
            _extractor_pb2.UpdateContainerizedExtractorResponse(extractor=_extractor_proto()),
            _extractor_pb2.UpdateContainerizedExtractorResponse(extractor=_extractor_proto()),
        ]
    )
    clients = _FakeClients(stub)
    extractor = NominalHostedExtractor._from_proto(clients, _extractor_proto())

    extractor.set_active_image("ri.scout.test.container-image.7")
    assert stub.requests[0].active_container_image_rid == "ri.scout.test.container-image.7"

    image = ContainerImage._from_proto(clients, _WORKSPACE, _image_proto(rid="ri.scout.test.container-image.8"))
    extractor.set_active_image(image)
    assert stub.requests[1].active_container_image_rid == "ri.scout.test.container-image.8"


def test_search_follows_pagination_cursor() -> None:
    page1 = _extractor_pb2.SearchContainerizedExtractorsResponse(
        extractors=[_extractor_proto(rid="a"), _extractor_proto(rid="b")],
        next_page_token="cursor-2",
    )
    page2 = _extractor_pb2.SearchContainerizedExtractorsResponse(extractors=[_extractor_proto(rid="c")])
    stub = _RecordingStub([page1, page2])

    results = NominalHostedExtractor._search(
        _FakeClients(stub), include_archived=False, file_extension=None, workspace_rid=None
    )

    assert [extractor.rid for extractor in results] == ["a", "b", "c"]
    assert stub.requests[0].next_page_token == ""  # first page sends no cursor
    assert stub.requests[1].next_page_token == "cursor-2"


def test_register_image_builds_full_request() -> None:
    stub = _RecordingStub([_registry_pb2.CreateImageResponse(image=_image_proto())])
    extractor = NominalHostedExtractor._from_proto(_FakeClients(stub), _extractor_proto())

    image = extractor.register_image(
        tag="v1",
        object_path="s3://bucket/image.tar",
        inputs=[FileExtractionInput(environment_variable="INPUT_FILE", name="Input file", required=True)],
        file_output_format=FileOutputFormat.FILE_OUTPUT_FORMAT_PARQUET,
        default_timestamp_metadata=_iso_metadata(),
    )

    assert image.rid == _IMAGE
    assert image.tag == "v1"
    request = stub.requests[0]
    assert request.workspace_rid == _WORKSPACE
    assert request.extractor_rid == _EXTRACTOR
    assert request.tag == "v1"
    assert request.object_path == "s3://bucket/image.tar"
    assert [i.environment_variable for i in request.inputs] == ["INPUT_FILE"]
    assert request.file_output_format == FileOutputFormat.FILE_OUTPUT_FORMAT_PARQUET
    assert request.default_timestamp_metadata.series_name == "ts"


def test_get_image_uses_extractor_workspace() -> None:
    stub = _RecordingStub([_registry_pb2.GetImageResponse(image=_image_proto(rid=_IMAGE))])
    extractor = NominalHostedExtractor._from_proto(_FakeClients(stub), _extractor_proto())

    image = extractor.get_image(_IMAGE)

    assert image.rid == _IMAGE
    request = stub.requests[0]
    assert request.rid == _IMAGE
    assert request.workspace_rid == _WORKSPACE


def test_image_delete_passes_rid_and_workspace() -> None:
    stub = _RecordingStub([_registry_pb2.DeleteImageResponse()])
    image = ContainerImage._from_proto(_FakeClients(stub), "ri.security.test.workspace.z", _image_proto(rid=_IMAGE))

    image.delete()

    request = stub.requests[0]
    assert request.rid == _IMAGE
    assert request.workspace_rid == "ri.security.test.workspace.z"


def test_search_images_follows_pagination_cursor() -> None:
    page1 = _registry_pb2.SearchImagesResponse(
        images=[_image_proto(rid="img-a"), _image_proto(rid="img-b")], next_page_token="cursor-2"
    )
    page2 = _registry_pb2.SearchImagesResponse(images=[_image_proto(rid="img-c")])
    stub = _RecordingStub([page1, page2])

    results = ContainerImage._search(_FakeClients(stub))

    assert [image.rid for image in results] == ["img-a", "img-b", "img-c"]
    assert stub.requests[1].next_page_token == "cursor-2"
