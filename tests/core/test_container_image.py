from __future__ import annotations

from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from nominal.core._utils.query_tools import create_search_container_images_query
from nominal.core.container_image import (
    ContainerImage,
    ContainerImageStatus,
    FileOutputFormat,
    _get_container_image,
    _search_container_images,
)
from nominal.core.exceptions import NominalContainerImageError
from nominal.protos.registry.v2 import registry_pb2
from nominal.protos.types.time import timestamp_parsers_pb2


def _clients() -> MagicMock:
    clients = MagicMock()
    clients.auth_header = "Bearer test-token"
    clients.resolve_default_workspace_rid.return_value = "ri.workspace.default"
    clients.resolve_workspace.return_value.rid = "ri.workspace.default"
    return clients


def _img(r: str) -> registry_pb2.ContainerImage:
    return registry_pb2.ContainerImage(
        rid=r,
        tag="v1",
        extractor_rid="ri.ext",
        file_output_format=registry_pb2.FILE_OUTPUT_FORMAT_PARQUET,
        default_timestamp_metadata=registry_pb2.TimestampMetadata(
            series_name="ts",
            timestamp_type=timestamp_parsers_pb2.TimestampType(
                absolute=timestamp_parsers_pb2.AbsoluteTimestamp(iso8601=timestamp_parsers_pb2.Iso8601Timestamp())
            ),
        ),
    )


def _img_with_status(rid: str, status: registry_pb2.ContainerImageStatus.ValueType) -> registry_pb2.ContainerImage:
    img = _img(rid)
    img.status = status
    return img


def test_container_image_refresh_uses_its_own_workspace() -> None:
    """refresh() re-fetches via GetImage with the image's own workspace (not the client default), in place."""
    clients = _clients()
    image = ContainerImage._from_proto(clients, "ri.ws.explicit", _img("ri.img.1"))
    updated = _img("ri.img.1")
    updated.tag = "v2"
    updated.status = registry_pb2.CONTAINER_IMAGE_STATUS_READY
    clients.registry.GetImage.return_value = registry_pb2.GetImageResponse(image=updated)

    returned = image.refresh()

    assert returned is image
    assert image.tag == "v2"
    assert image.status is ContainerImageStatus.READY
    request = clients.registry.GetImage.call_args.args[0]
    assert request.rid == "ri.img.1"
    assert request.workspace_rid == "ri.ws.explicit"


def test_image_from_proto_handles_minimal_proto() -> None:
    """A minimal image (optional metadata unset, enum at its 0/UNSPECIFIED default) parses without crashing."""
    msg = registry_pb2.ContainerImage(rid="ri.img.1", tag="v1", extractor_rid="ri.ext")

    image = ContainerImage._from_proto(_clients(), "ri.ws", msg)

    assert image.default_timestamp_metadata is None
    assert image.file_output_format is FileOutputFormat.UNSPECIFIED


def test_get_container_image_defaults_workspace_and_returns_image() -> None:
    """get_container_image resolves the default workspace and returns the parsed image."""
    clients = _clients()
    clients.registry.GetImage.return_value = registry_pb2.GetImageResponse(image=_img("ri.img.1"))

    image = _get_container_image(clients, "ri.img.1")

    assert image.rid == "ri.img.1"
    request = clients.registry.GetImage.call_args.args[0]
    assert request.rid == "ri.img.1"
    assert request.workspace_rid == "ri.workspace.default"


def test_search_container_images_follows_pagination_cursors() -> None:
    """Image search accumulates across pages and stops on an empty next_page_token."""
    clients = _clients()
    clients.registry.SearchImages.side_effect = [
        registry_pb2.SearchImagesResponse(images=[_img("i1")], next_page_token="n"),
        registry_pb2.SearchImagesResponse(images=[_img("i2")], next_page_token=""),
    ]

    results = _search_container_images(clients, workspace_rid=None)

    assert [i.rid for i in results] == ["i1", "i2"]
    assert clients.registry.SearchImages.call_count == 2
    assert clients.registry.SearchImages.call_args_list[1].args[0].next_page_token == "n"


def test_create_search_container_images_query_selects_the_right_oneof() -> None:
    """tag/status map to their filters; both compose into the proto's `and` oneof; neither yields no filter."""
    assert create_search_container_images_query(None, None) is None
    assert create_search_container_images_query("v1", None).WhichOneof("filter") == "tag"
    assert create_search_container_images_query(None, ContainerImageStatus.READY).WhichOneof("filter") == "status"
    assert create_search_container_images_query("v1", ContainerImageStatus.READY).WhichOneof("filter") == "and"


def test_poll_until_ready_polls_until_ready() -> None:
    """poll_until_ready refreshes on an interval until the server reports READY, in place."""
    clients = _clients()
    image = ContainerImage._from_proto(
        clients, "ri.ws", _img_with_status("ri.img.1", registry_pb2.CONTAINER_IMAGE_STATUS_PENDING)
    )
    clients.registry.GetImage.side_effect = [
        registry_pb2.GetImageResponse(image=_img_with_status("ri.img.1", registry_pb2.CONTAINER_IMAGE_STATUS_PENDING)),
        registry_pb2.GetImageResponse(image=_img_with_status("ri.img.1", registry_pb2.CONTAINER_IMAGE_STATUS_READY)),
    ]

    returned = image.poll_until_ready(interval=timedelta(seconds=0))

    assert returned is image
    assert image.status is ContainerImageStatus.READY
    assert clients.registry.GetImage.call_count == 2


def test_poll_until_ready_raises_when_image_failed() -> None:
    """A FAILED image raises instead of polling forever."""
    clients = _clients()
    image = ContainerImage._from_proto(clients, "ri.ws", _img("ri.img.1"))
    clients.registry.GetImage.return_value = registry_pb2.GetImageResponse(
        image=_img_with_status("ri.img.1", registry_pb2.CONTAINER_IMAGE_STATUS_FAILED)
    )

    with pytest.raises(NominalContainerImageError, match="FAILED"):
        image.poll_until_ready(interval=timedelta(seconds=0))


def test_enum_from_proto_falls_back_to_unspecified_on_unknown_value() -> None:
    """A value a newer server might return (unknown to this SDK) decodes to UNSPECIFIED, not a crash."""
    assert FileOutputFormat._from_proto(999) is FileOutputFormat.UNSPECIFIED
    assert ContainerImageStatus._from_proto(999) is ContainerImageStatus.UNSPECIFIED
