from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core.container_image import FileOutputFormat
from nominal.core.containerized_extractor import (
    ContainerizedExtractor,
    _create_containerized_extractor,
    _search_containerized_extractors,
)
from nominal.core.exceptions import NominalContainerImageError
from nominal.protos.ingest.v2 import containerized_extractor_pb2
from nominal.protos.registry.v2 import registry_pb2


def _clients() -> MagicMock:
    clients = MagicMock()
    clients.auth_header = "Bearer test-token"
    clients.resolve_default_workspace_rid.return_value = "ri.workspace.default"
    clients.resolve_workspace.return_value.rid = "ri.workspace.default"
    return clients


def _ext(rid: str) -> containerized_extractor_pb2.ContainerizedExtractor:
    return containerized_extractor_pb2.ContainerizedExtractor(
        rid=rid, workspace_rid="ri.workspace.default", name=rid, is_archived=False
    )


def _img(rid: str, status: registry_pb2.ContainerImageStatus.ValueType) -> registry_pb2.ContainerImage:
    return registry_pb2.ContainerImage(rid=rid, tag="v1", extractor_rid="ri.ext", status=status)


def test_search_extractors_follows_pagination_cursors() -> None:
    """Search accumulates results across pages until the server returns an empty next_page_token."""
    clients = _clients()
    page1 = containerized_extractor_pb2.SearchContainerizedExtractorsResponse(
        extractors=[_ext("a")], next_page_token="tok"
    )
    page2 = containerized_extractor_pb2.SearchContainerizedExtractorsResponse(
        extractors=[_ext("b")], next_page_token=""
    )
    clients.containerized_extractor.SearchContainerizedExtractors.side_effect = [page1, page2]

    results = _search_containerized_extractors(clients, include_archived=False, file_extension=None, workspace_rid=None)

    assert [e.rid for e in results] == ["a", "b"]
    assert clients.containerized_extractor.SearchContainerizedExtractors.call_count == 2
    second_call_request = clients.containerized_extractor.SearchContainerizedExtractors.call_args_list[1].args[0]
    assert second_call_request.next_page_token == "tok"


def test_create_defaults_workspace_to_client_default() -> None:
    """When workspace_rid is omitted, the request uses the client's resolved default workspace."""
    clients = _clients()
    clients.containerized_extractor.CreateContainerizedExtractor.return_value = (
        containerized_extractor_pb2.CreateContainerizedExtractorResponse(extractor=_ext("a"))
    )

    _create_containerized_extractor(clients, "a", description=None)

    request = clients.containerized_extractor.CreateContainerizedExtractor.call_args.args[0]
    assert request.workspace_rid == "ri.workspace.default"


def test_extractor_refresh_updates_fields_in_place() -> None:
    """refresh() re-fetches via GetContainerizedExtractor and updates the same instance."""
    clients = _clients()
    extractor = ContainerizedExtractor._from_proto(clients, _ext("ri.ext"))
    refreshed = containerized_extractor_pb2.ContainerizedExtractor(
        rid="ri.ext", workspace_rid="ri.workspace.default", name="renamed", is_archived=True
    )
    clients.containerized_extractor.GetContainerizedExtractor.return_value = (
        containerized_extractor_pb2.GetContainerizedExtractorResponse(extractor=refreshed)
    )

    returned = extractor.refresh()

    assert returned is extractor
    assert extractor.name == "renamed"
    assert extractor.is_archived is True
    assert clients.containerized_extractor.GetContainerizedExtractor.call_args.args[0].rid == "ri.ext"


def test_set_active_image_raises_when_image_not_ready() -> None:
    """With polling disabled, activating a non-READY image raises up-front without sending an update."""
    clients = _clients()
    extractor = ContainerizedExtractor._from_proto(clients, _ext("ri.ext"))
    clients.registry.GetImage.return_value = registry_pb2.GetImageResponse(
        image=_img("ri.img.1", registry_pb2.CONTAINER_IMAGE_STATUS_PENDING)
    )

    with pytest.raises(NominalContainerImageError, match="PENDING"):
        extractor.set_active_image("ri.img.1", poll_until_ready=False)

    clients.containerized_extractor.UpdateContainerizedExtractor.assert_not_called()


@pytest.mark.parametrize("output_format", [FileOutputFormat.PARQUET_TAR, FileOutputFormat.UNSPECIFIED])
def test_register_image_rejects_retired_output_formats_before_uploading(
    output_format: FileOutputFormat,
) -> None:
    """Formats being retired (or unset) fail up-front, before the tarball upload starts."""
    clients = _clients()
    extractor = ContainerizedExtractor._from_proto(clients, _ext("ri.ext"))

    with pytest.raises(ValueError, match=output_format.name):
        extractor.register_image(
            "extractor.tar",
            tag="v1",
            inputs=[],
            default_timestamp_column="ts",
            default_timestamp_type="iso_8601",
            output_format=output_format,
        )

    clients.upload.initiate_multipart_upload.assert_not_called()
    clients.registry.CreateImage.assert_not_called()


def test_set_active_image_polls_then_activates() -> None:
    """By default, a rid is fetched in the extractor's workspace, awaited until READY, then activated."""
    clients = _clients()
    extractor = ContainerizedExtractor._from_proto(clients, _ext("ri.ext"))
    clients.registry.GetImage.side_effect = [
        registry_pb2.GetImageResponse(image=_img("ri.img.1", registry_pb2.CONTAINER_IMAGE_STATUS_PENDING)),
        registry_pb2.GetImageResponse(image=_img("ri.img.1", registry_pb2.CONTAINER_IMAGE_STATUS_READY)),
    ]
    clients.containerized_extractor.UpdateContainerizedExtractor.return_value = (
        containerized_extractor_pb2.UpdateContainerizedExtractorResponse(extractor=_ext("ri.ext"))
    )

    extractor.set_active_image("ri.img.1")

    assert clients.registry.GetImage.call_args_list[0].args[0].workspace_rid == "ri.workspace.default"
    update_request = clients.containerized_extractor.UpdateContainerizedExtractor.call_args.args[0]
    assert update_request.active_container_image_rid == "ri.img.1"
