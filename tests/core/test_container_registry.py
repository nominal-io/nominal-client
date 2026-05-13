from __future__ import annotations

import io
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from nominal_api_protos.nominal.registry.v1 import registry_pb2

from nominal.core.client import NominalClient, WorkspaceSearchType
from nominal.core.container_image import ContainerImage, ContainerImageStatus
from nominal.core.filetype import FileTypes


class _FakeRegistry:
    def __init__(self) -> None:
        self.create_response = _pb_image()
        self.get_response = _pb_image()
        self.search_responses: list[registry_pb2.SearchImagesResponse] = []
        self.create_calls: list[tuple[str, registry_pb2.CreateImageRequest]] = []
        self.get_calls: list[tuple[str, str, str]] = []
        self.delete_calls: list[tuple[str, str, str]] = []
        self.search_calls: list[tuple[str, registry_pb2.SearchImagesRequest]] = []

    def create_image(self, auth_header: str, request: registry_pb2.CreateImageRequest) -> registry_pb2.ContainerImage:
        self.create_calls.append((auth_header, request))
        return self.create_response

    def get_image(self, auth_header: str, rid: str, *, workspace_rid: str) -> registry_pb2.ContainerImage:
        self.get_calls.append((auth_header, rid, workspace_rid))
        return self.get_response

    def delete_image(self, auth_header: str, rid: str, *, workspace_rid: str) -> None:
        self.delete_calls.append((auth_header, rid, workspace_rid))

    def search_images(
        self, auth_header: str, request: registry_pb2.SearchImagesRequest
    ) -> registry_pb2.SearchImagesResponse:
        self.search_calls.append((auth_header, request))
        return self.search_responses.pop(0)


def _pb_image(
    *,
    rid: str = "ri.container-image.test.1",
    name: str = "extractor",
    tag: str = "abc123",
    status: int = registry_pb2.CONTAINER_IMAGE_STATUS_READY,
    size_bytes: int | None = 42,
) -> registry_pb2.ContainerImage:
    image = registry_pb2.ContainerImage(
        rid=rid,
        name=name,
        tag=tag,
        status=status,
        created_at=Timestamp(seconds=2, nanos=3),
    )
    if size_bytes is not None:
        image.size_bytes = size_bytes
    return image


def _clients(*, workspace_rid: str | None = "ri.workspace.default") -> SimpleNamespace:
    return SimpleNamespace(
        auth_header="Bearer token",
        workspace_rid=workspace_rid,
        upload=MagicMock(name="upload"),
        registry=_FakeRegistry(),
        resolve_default_workspace_rid=MagicMock(return_value="ri.workspace.default"),
        resolve_workspace=MagicMock(side_effect=lambda rid: SimpleNamespace(rid=rid)),
    )


def test_upload_container_image_from_io_uploads_tarball_and_returns_container_image() -> None:
    clients = _clients()
    tarball = io.BytesIO(b"tarball")

    with patch("nominal.core.client.upload_multipart_io", return_value="s3://object-path") as upload:
        image = NominalClient(_clients=clients).upload_container_image_from_io(tarball, "extractor", "abc123")

    assert image.rid == "ri.container-image.test.1"
    assert image.name == "extractor"
    assert image.tag == "abc123"
    assert image.status is ContainerImageStatus.READY
    assert image.size_bytes == 42
    upload.assert_called_once_with(
        "Bearer token",
        "ri.workspace.default",
        tarball,
        "extractor-abc123",
        FileTypes.TAR,
        clients.upload,
    )
    auth_header, request = clients.registry.create_calls[0]
    assert auth_header == "Bearer token"
    assert request.workspace_rid == "ri.workspace.default"
    assert request.name == "extractor"
    assert request.tag == "abc123"
    assert request.object_path == "s3://object-path"


def test_upload_container_image_from_io_rejects_text_streams() -> None:
    with pytest.raises(TypeError, match="binary mode"):
        NominalClient(_clients=_clients()).upload_container_image_from_io(io.StringIO("tarball"), "extractor", "tag")


def test_upload_container_image_from_io_requires_configured_workspace() -> None:
    clients = _clients(workspace_rid=None)

    with patch("nominal.core.client.upload_multipart_io") as upload:
        with pytest.raises(ValueError, match="workspace_rid"):
            NominalClient(_clients=clients).upload_container_image_from_io(io.BytesIO(b"tarball"), "extractor", "tag")

    upload.assert_not_called()
    assert clients.registry.create_calls == []


def test_search_container_images_returns_all_pages_and_applies_filters() -> None:
    clients = _clients(workspace_rid=None)
    clients.registry.search_responses = [
        registry_pb2.SearchImagesResponse(
            images=[_pb_image(rid="ri.container-image.test.1")],
            next_page_token="next-page",
        ),
        registry_pb2.SearchImagesResponse(images=[_pb_image(rid="ri.container-image.test.2", tag="def456")]),
    ]

    images = NominalClient(_clients=clients).search_container_images(
        name="extractor",
        tag="abc123",
        status=ContainerImageStatus.READY,
    )

    assert [image.rid for image in images] == ["ri.container-image.test.1", "ri.container-image.test.2"]
    assert [call[0] for call in clients.registry.search_calls] == ["Bearer token", "Bearer token"]
    first_request = clients.registry.search_calls[0][1]
    second_request = clients.registry.search_calls[1][1]
    assert first_request.workspace_rid == "ri.workspace.default"
    assert first_request.filter.WhichOneof("filter") == "and"
    clauses = getattr(first_request.filter, "and").clauses
    assert clauses[0].name.name == "extractor"
    assert clauses[1].tag.tag == "abc123"
    assert clauses[2].status.status == registry_pb2.CONTAINER_IMAGE_STATUS_READY
    assert second_request.next_page_token == "next-page"


def test_search_container_images_rejects_all_workspace_selector() -> None:
    with pytest.raises(ValueError, match="WorkspaceSearchType.ALL"):
        NominalClient(_clients=_clients()).search_container_images(workspace=WorkspaceSearchType.ALL)


def test_get_and_delete_container_image_use_workspace_boundary() -> None:
    clients = _clients()
    client = NominalClient(_clients=clients)

    image = client.get_container_image("ri.container-image.test.1", workspace_rid="ri.workspace.explicit")
    client.delete_container_image("ri.container-image.test.1", workspace_rid="ri.workspace.explicit")

    assert image.workspace_rid == "ri.workspace.explicit"
    assert clients.registry.get_calls == [
        ("Bearer token", "ri.container-image.test.1", "ri.workspace.explicit"),
    ]
    assert clients.registry.delete_calls == [
        ("Bearer token", "ri.container-image.test.1", "ri.workspace.explicit"),
    ]


def test_container_image_delete_uses_own_workspace() -> None:
    clients = _clients()
    image = ContainerImage(
        rid="ri.container-image.test.1",
        name="extractor",
        tag="abc123",
        status=ContainerImageStatus.READY,
        created_at=2_000_000_003,
        size_bytes=None,
        workspace_rid="ri.workspace.image",
        _clients=clients,
    )

    image.delete()

    assert clients.registry.delete_calls == [
        ("Bearer token", "ri.container-image.test.1", "ri.workspace.image"),
    ]
