from __future__ import annotations

from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from nominal import core
from nominal.core._utils.query_tools import create_search_container_images_query
from nominal.core.container_image import (
    ContainerImage,
    ContainerImageStatus,
    FileExtractionInput,
    FileOutputFormat,
    TimestampMetadata,
    _get_container_image,
    _search_container_images,
)
from nominal.exceptions import NominalContainerImageError
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
    assert image.exit_code_mappings == ()
    assert image.resources == core.ContainerResources()


def test_image_refresh_replaces_and_clears_resources() -> None:
    """Refresh replaces resource overrides without carrying over previously configured fields."""
    clients = _clients()
    original = _img("ri.img")
    original.resources.CopyFrom(registry_pb2.ContainerResources(cpu_cores=4, memory_gib=16, disk_gib=64))
    image = ContainerImage._from_proto(clients, "ri.ws", original)
    assert image.resources == core.ContainerResources(cpu_cores=4, memory_gib=16, disk_gib=64)

    updated = _img("ri.img")
    updated.resources.memory_gib = 32
    clients.registry.GetImage.return_value = registry_pb2.GetImageResponse(image=updated)

    refreshed = image.refresh()

    assert refreshed is image
    assert image.resources == core.ContainerResources(memory_gib=32)

    clients.registry.GetImage.return_value = registry_pb2.GetImageResponse(image=_img("ri.img"))
    image.refresh()
    assert image.resources == core.ContainerResources()


def test_image_refresh_replaces_and_clears_exit_code_mappings() -> None:
    """refresh() adopts the server's exit code mappings, including clearing them when the server drops them."""
    clients = _clients()
    image = ContainerImage._from_proto(clients, "ri.ws", _img("ri.img"))
    updated = _img("ri.img")
    updated.exit_code_mappings.add(exit_code=2, code="INVALID_INPUT", message="Invalid input", retryable=True)
    clients.registry.GetImage.return_value = registry_pb2.GetImageResponse(image=updated)

    assert image.refresh() is image
    assert image.exit_code_mappings == (
        core.ExitCodeMapping(exit_code=2, code="INVALID_INPUT", message="Invalid input", retryable=True),
    )

    clients.registry.GetImage.return_value = registry_pb2.GetImageResponse(image=_img("ri.img"))
    image.refresh()
    assert image.exit_code_mappings == ()


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


def _source_image(clients: MagicMock, *, with_timestamp_metadata: bool = True) -> ContainerImage:
    msg = registry_pb2.ContainerImage(
        rid="ri.img.source",
        tag="v1",
        status=registry_pb2.CONTAINER_IMAGE_STATUS_READY,
        extractor_rid="ri.ext",
        inputs=[registry_pb2.FileExtractionInput(name="data", environment_variable="INPUT_PATH")],
        parameters=[registry_pb2.FileExtractionParameter(name="scale", environment_variable="SCALE")],
        exit_code_mappings=[registry_pb2.ExitCodeMapping(exit_code=2, code="INVALID_INPUT", message="Invalid file")],
        resources=registry_pb2.ContainerResources(cpu_cores=4, memory_gib=16),
        file_output_format=registry_pb2.FILE_OUTPUT_FORMAT_PARQUET,
        default_timestamp_metadata=(
            TimestampMetadata(series_name="time", timestamp_type="iso_8601")._to_proto()
            if with_timestamp_metadata
            else None
        ),
    )
    clients.registry.GetImage.return_value = registry_pb2.GetImageResponse(image=msg)
    return ContainerImage._from_proto(clients, "ri.workspace.source", msg)


def test_clone_inherits_contract_and_reuses_source_binary() -> None:
    """Registering from an image creates a new tag without uploading or changing its execution contract."""
    clients = _clients()
    source = _source_image(clients)
    clients.registry.CreateImage.return_value = registry_pb2.CreateImageResponse(
        image=_img_with_status("ri.img.new", registry_pb2.CONTAINER_IMAGE_STATUS_READY)
    )

    image = source.clone(tag="v2")

    request = clients.registry.CreateImage.call_args.args[0]
    assert request.source_image_rid == "ri.img.source"
    assert request.object_path == ""
    assert request.tag == "v2"
    assert request.inputs[0].name == "data"
    assert request.parameters[0].name == "scale"
    assert request.file_output_format == registry_pb2.FILE_OUTPUT_FORMAT_PARQUET
    assert request.default_timestamp_metadata.series_name == "time"
    assert list(request.exit_code_mappings) == [
        registry_pb2.ExitCodeMapping(exit_code=2, code="INVALID_INPUT", message="Invalid file")
    ]
    assert request.resources == registry_pb2.ContainerResources(cpu_cores=4, memory_gib=16)
    assert image.rid == "ri.img.new"
    assert image is not source
    assert source.rid == "ri.img.source"
    assert source.tag == "v1"
    assert request.extractor_rid == source.extractor_rid
    assert request.workspace_rid == "ri.workspace.source"
    assert image._workspace_rid == "ri.workspace.source"
    clients.upload.initiate_multipart_upload.assert_not_called()
    clients.containerized_extractor.UpdateContainerizedExtractor.assert_not_called()


def test_clone_allows_execution_contract_overrides() -> None:
    """Callers can change the new image's contract while reusing the source image binary."""
    clients = _clients()
    source = _source_image(clients)
    clients.registry.CreateImage.return_value = registry_pb2.CreateImageResponse(
        image=_img_with_status("ri.img.new", registry_pb2.CONTAINER_IMAGE_STATUS_READY)
    )

    source.clone(
        tag="v2",
        inputs=(FileExtractionInput(name="new", environment_variable="NEW_PATH"),),
        parameters=(),
        exit_code_mappings=(),
        resources=core.ContainerResources(disk_gib=64),
        output_format=FileOutputFormat.CSV,
        default_timestamp_column="new_time",
        default_timestamp_type="iso_8601",
    )

    request = clients.registry.CreateImage.call_args.args[0]
    assert request.inputs[0].name == "new"
    assert list(request.parameters) == []
    assert request.file_output_format == registry_pb2.FILE_OUTPUT_FORMAT_CSV
    assert request.default_timestamp_metadata.series_name == "new_time"
    assert list(request.exit_code_mappings) == []
    assert request.resources == registry_pb2.ContainerResources(disk_gib=64)


def test_clone_can_reset_resources_to_deployment_defaults() -> None:
    """An explicit empty resource object clears inherited overrides."""
    clients = _clients()
    clients.registry.CreateImage.return_value = registry_pb2.CreateImageResponse(
        image=_img_with_status("ri.img.new", registry_pb2.CONTAINER_IMAGE_STATUS_READY)
    )

    _source_image(clients).clone(tag="v2", resources=core.ContainerResources())

    assert clients.registry.CreateImage.call_args.args[0].resources == registry_pb2.ContainerResources()


@pytest.mark.parametrize(
    "timestamp_overrides",
    [
        {"default_timestamp_column": "new_time"},
        {"default_timestamp_type": "iso_8601"},
    ],
)
def test_clone_requires_timestamp_overrides_together(timestamp_overrides: dict[str, object]) -> None:
    """A partial timestamp override is rejected rather than silently mixing old and new metadata."""
    clients = _clients()
    clients.registry.CreateImage.return_value = registry_pb2.CreateImageResponse(
        image=_img_with_status("ri.img.new", registry_pb2.CONTAINER_IMAGE_STATUS_READY)
    )

    with pytest.raises(ValueError, match="provided together"):
        _source_image(clients).clone(tag="v2", **timestamp_overrides)

    clients.registry.CreateImage.assert_not_called()


def test_clone_requires_timestamp_metadata_when_the_source_has_none() -> None:
    """Legacy images without timestamp metadata require an explicit replacement contract."""
    clients = _clients()

    with pytest.raises(ValueError, match="no default timestamp metadata"):
        _source_image(clients, with_timestamp_metadata=False).clone(tag="v2")

    clients.registry.CreateImage.assert_not_called()


@pytest.mark.parametrize("output_format", [FileOutputFormat.JSON_L, FileOutputFormat.PARQUET_TAR])
def test_clone_rejects_non_ingestible_output_formats(output_format: FileOutputFormat) -> None:
    """Derived images apply the same output-format guard as tarball registrations."""
    clients = _clients()
    clients.registry.CreateImage.return_value = registry_pb2.CreateImageResponse(
        image=_img_with_status("ri.img.new", registry_pb2.CONTAINER_IMAGE_STATUS_READY)
    )

    with pytest.raises(ValueError, match=output_format.name):
        _source_image(clients).clone(tag="v2", output_format=output_format)

    clients.registry.CreateImage.assert_not_called()


def test_clone_inherits_fresh_metadata_without_mutating_source() -> None:
    """Cloning uses the latest server contract while preserving the caller's source snapshot."""
    clients = _clients()
    source = _source_image(clients)
    latest = clients.registry.GetImage.return_value.image
    latest.resources.memory_gib = 32
    latest.default_timestamp_metadata.series_name = "latest_time"
    clients.registry.CreateImage.return_value = registry_pb2.CreateImageResponse(image=_img("ri.img.copy"))

    source.clone(tag="v2")

    request = clients.registry.CreateImage.call_args.args[0]
    assert request.resources.memory_gib == 32
    assert request.default_timestamp_metadata.series_name == "latest_time"
    assert source.resources.memory_gib == 16
    assert source.default_timestamp_metadata.series_name == "time"
    clients.registry.GetImage.assert_called_once_with(
        registry_pb2.GetImageRequest(rid=source.rid, workspace_rid="ri.workspace.source")
    )
