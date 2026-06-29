from __future__ import annotations

from unittest.mock import MagicMock

from nominal.core.containerized_extractor import (
    ContainerImage,
    ContainerImageStatus,
    ContainerizedExtractor,
    FileOutputFormat,
    _build_search_filter,
    _create_containerized_extractor,
    _get_image,
    _search_containerized_extractors,
    _search_images,
)
from nominal.protos.ingest.v2 import containerized_extractor_pb2 as ext_pb2
from nominal.protos.registry.v2 import registry_pb2 as reg_pb2


def _clients() -> MagicMock:
    clients = MagicMock()
    clients.auth_header = "Bearer test-token"
    clients.resolve_default_workspace_rid.return_value = "ri.workspace.default"
    return clients


def _ext(rid: str) -> ext_pb2.ContainerizedExtractor:
    return ext_pb2.ContainerizedExtractor(rid=rid, workspace_rid="ri.workspace.default", name=rid, is_archived=False)


def test_search_extractors_follows_pagination_cursors() -> None:
    """Search accumulates results across pages until the server returns an empty next_page_token."""
    clients = _clients()
    page1 = ext_pb2.SearchContainerizedExtractorsResponse(extractors=[_ext("a")], next_page_token="tok")
    page2 = ext_pb2.SearchContainerizedExtractorsResponse(extractors=[_ext("b")], next_page_token="")
    clients.containerized_extractor.SearchContainerizedExtractors.side_effect = [page1, page2]

    results = _search_containerized_extractors(clients, include_archived=False, file_extension=None, workspace_rid=None)

    assert [e.rid for e in results] == ["a", "b"]
    assert clients.containerized_extractor.SearchContainerizedExtractors.call_count == 2
    second_call_request = clients.containerized_extractor.SearchContainerizedExtractors.call_args_list[1].args[0]
    assert second_call_request.next_page_token == "tok"


def _img(r: str) -> reg_pb2.ContainerImage:
    from nominal.protos.types.time import timestamp_parsers_pb2 as time_pb2

    return reg_pb2.ContainerImage(
        rid=r,
        tag="v1",
        extractor_rid="ri.ext",
        file_output_format=1,
        default_timestamp_metadata=reg_pb2.TimestampMetadata(
            series_name="ts",
            timestamp_type=time_pb2.TimestampType(
                absolute=time_pb2.AbsoluteTimestamp(iso8601=time_pb2.Iso8601Timestamp())
            ),
        ),
    )


def test_container_image_refresh_uses_its_own_workspace() -> None:
    """refresh() re-fetches via GetImage with the image's own workspace (not the client default), in place."""
    clients = _clients()
    image = ContainerImage._from_proto(clients, "ri.ws.explicit", _img("ri.img.1"))
    updated = _img("ri.img.1")
    updated.tag = "v2"
    updated.status = reg_pb2.CONTAINER_IMAGE_STATUS_READY
    clients.registry.GetImage.return_value = reg_pb2.GetImageResponse(image=updated)

    returned = image.refresh()

    assert returned is image
    assert image.tag == "v2"
    assert image.status is ContainerImageStatus.READY
    request = clients.registry.GetImage.call_args.args[0]
    assert request.rid == "ri.img.1"
    assert request.workspace_rid == "ri.ws.explicit"


def test_image_from_proto_handles_minimal_proto() -> None:
    """A minimal image (optional metadata unset, enum at its 0/UNSPECIFIED default) parses without crashing."""
    msg = reg_pb2.ContainerImage(rid="ri.img.1", tag="v1", extractor_rid="ri.ext")

    image = ContainerImage._from_proto(_clients(), "ri.ws", msg)

    assert image.default_timestamp_metadata is None
    assert image.file_output_format is FileOutputFormat.UNSPECIFIED


def test_create_defaults_workspace_to_client_default() -> None:
    """When workspace_rid is omitted, the request uses the client's resolved default workspace."""
    clients = _clients()
    clients.containerized_extractor.CreateContainerizedExtractor.return_value = (
        ext_pb2.CreateContainerizedExtractorResponse(extractor=_ext("a"))
    )

    _create_containerized_extractor(clients, "a", description=None, workspace_rid=None)

    request = clients.containerized_extractor.CreateContainerizedExtractor.call_args.args[0]
    assert request.workspace_rid == "ri.workspace.default"


def test_get_image_defaults_workspace_and_returns_image() -> None:
    """get_container_image resolves the default workspace and returns the parsed image."""
    clients = _clients()
    clients.registry.GetImage.return_value = reg_pb2.GetImageResponse(image=_img("ri.img.1"))

    image = _get_image(clients, "ri.img.1", workspace_rid=None)

    assert image.rid == "ri.img.1"
    request = clients.registry.GetImage.call_args.args[0]
    assert request.rid == "ri.img.1"
    assert request.workspace_rid == "ri.workspace.default"


def test_extractor_refresh_updates_fields_in_place() -> None:
    """refresh() re-fetches via GetContainerizedExtractor and updates the same instance."""
    clients = _clients()
    extractor = ContainerizedExtractor._from_proto(clients, _ext("ri.ext"))
    refreshed = ext_pb2.ContainerizedExtractor(
        rid="ri.ext", workspace_rid="ri.workspace.default", name="renamed", is_archived=True
    )
    clients.containerized_extractor.GetContainerizedExtractor.return_value = ext_pb2.GetContainerizedExtractorResponse(
        extractor=refreshed
    )

    returned = extractor.refresh()

    assert returned is extractor
    assert extractor.name == "renamed"
    assert extractor.is_archived is True
    assert clients.containerized_extractor.GetContainerizedExtractor.call_args.args[0].rid == "ri.ext"


def test_search_images_follows_pagination_cursors() -> None:
    """Image search accumulates across pages and stops on an empty next_page_token."""
    clients = _clients()
    clients.registry.SearchImages.side_effect = [
        reg_pb2.SearchImagesResponse(images=[_img("i1")], next_page_token="n"),
        reg_pb2.SearchImagesResponse(images=[_img("i2")], next_page_token=""),
    ]

    results = _search_images(clients, workspace_rid=None)

    assert [i.rid for i in results] == ["i1", "i2"]
    assert clients.registry.SearchImages.call_count == 2
    assert clients.registry.SearchImages.call_args_list[1].args[0].next_page_token == "n"


def test_build_search_filter_selects_the_right_oneof() -> None:
    """tag/status map to their filters; both compose into the proto's `and` oneof; neither yields no filter."""
    assert _build_search_filter(None, None) is None
    assert _build_search_filter("v1", None).WhichOneof("filter") == "tag"
    assert _build_search_filter(None, ContainerImageStatus.READY).WhichOneof("filter") == "status"
    assert _build_search_filter("v1", ContainerImageStatus.READY).WhichOneof("filter") == "and"
