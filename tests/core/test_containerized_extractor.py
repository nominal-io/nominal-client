from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core.container_image import (
    ContainerImage,
    ContainerImageStatus,
    FileExtractionInput,
    FileExtractionParameter,
    FileOutputFormat,
    TimestampMetadata,
)
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


def _source_image(clients: MagicMock, *, with_timestamp_metadata: bool = True) -> ContainerImage:
    return ContainerImage(
        rid="ri.img.source",
        tag="v1",
        status=ContainerImageStatus.READY,
        size_bytes=123,
        created_at=0,
        extractor_rid="ri.ext",
        inputs=(FileExtractionInput(name="data", environment_variable="INPUT_PATH", file_suffixes=("csv",)),),
        parameters=(FileExtractionParameter(name="scale", environment_variable="SCALE"),),
        file_output_format=FileOutputFormat.PARQUET,
        default_timestamp_metadata=(
            TimestampMetadata(series_name="time", timestamp_type="iso_8601") if with_timestamp_metadata else None
        ),
        _workspace_rid="ri.workspace.default",
        _clients=clients,
    )


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


@pytest.mark.parametrize("output_format", [FileOutputFormat.JSON_L, FileOutputFormat.PARQUET_TAR])
def test_register_image_rejects_non_ingestible_output_formats_before_uploading(
    output_format: FileOutputFormat,
) -> None:
    """Formats the backend can't ingest fail up-front, before the tarball upload starts."""
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


def test_register_image_from_inherits_contract_and_reuses_source_binary() -> None:
    """Registering from an image creates a new tag without uploading or changing its execution contract."""
    clients = _clients()
    extractor = ContainerizedExtractor._from_proto(clients, _ext("ri.ext"))
    source = _source_image(clients)
    clients.registry.CreateImage.return_value = registry_pb2.CreateImageResponse(
        image=_img("ri.img.new", registry_pb2.CONTAINER_IMAGE_STATUS_READY)
    )

    image = extractor.register_image_from(source, tag="v2")

    request = clients.registry.CreateImage.call_args.args[0]
    assert request.source_image_rid == "ri.img.source"
    assert request.object_path == ""
    assert request.tag == "v2"
    assert request.inputs[0].name == "data"
    assert request.parameters[0].name == "scale"
    assert request.file_output_format == registry_pb2.FILE_OUTPUT_FORMAT_PARQUET
    assert request.default_timestamp_metadata.series_name == "time"
    assert image.rid == "ri.img.new"
    clients.upload.initiate_multipart_upload.assert_not_called()


def test_register_image_from_allows_execution_contract_overrides() -> None:
    """Callers can change the new image's contract while reusing the source image binary."""
    clients = _clients()
    extractor = ContainerizedExtractor._from_proto(clients, _ext("ri.ext"))
    source = _source_image(clients)
    clients.registry.CreateImage.return_value = registry_pb2.CreateImageResponse(
        image=_img("ri.img.new", registry_pb2.CONTAINER_IMAGE_STATUS_READY)
    )

    extractor.register_image_from(
        source,
        tag="v2",
        inputs=(FileExtractionInput(name="new", environment_variable="NEW_PATH"),),
        parameters=(),
        output_format=FileOutputFormat.CSV,
        default_timestamp_column="new_time",
        default_timestamp_type="iso_8601",
    )

    request = clients.registry.CreateImage.call_args.args[0]
    assert request.inputs[0].name == "new"
    assert list(request.parameters) == []
    assert request.file_output_format == registry_pb2.FILE_OUTPUT_FORMAT_CSV
    assert request.default_timestamp_metadata.series_name == "new_time"


def test_register_image_from_resolves_source_rid_in_the_extractors_workspace() -> None:
    """A source RID is fetched in the extractor's workspace before its contract is inherited."""
    clients = _clients()
    extractor = ContainerizedExtractor._from_proto(clients, _ext("ri.ext"))
    clients.registry.GetImage.return_value = registry_pb2.GetImageResponse(
        image=registry_pb2.ContainerImage(
            rid="ri.img.source",
            tag="v1",
            extractor_rid="ri.ext",
            status=registry_pb2.CONTAINER_IMAGE_STATUS_READY,
            inputs=[registry_pb2.FileExtractionInput(name="data", environment_variable="INPUT_PATH")],
            file_output_format=registry_pb2.FILE_OUTPUT_FORMAT_PARQUET,
            default_timestamp_metadata=TimestampMetadata(series_name="time", timestamp_type="iso_8601")._to_proto(),
        )
    )
    clients.registry.CreateImage.return_value = registry_pb2.CreateImageResponse(
        image=_img("ri.img.new", registry_pb2.CONTAINER_IMAGE_STATUS_READY)
    )

    extractor.register_image_from("ri.img.source", tag="v2")

    get_request = clients.registry.GetImage.call_args.args[0]
    assert get_request.rid == "ri.img.source"
    assert get_request.workspace_rid == "ri.workspace.default"
    create_request = clients.registry.CreateImage.call_args.args[0]
    assert create_request.source_image_rid == "ri.img.source"
    assert create_request.inputs[0].name == "data"


@pytest.mark.parametrize(
    "timestamp_overrides",
    [
        {"default_timestamp_column": "new_time"},
        {"default_timestamp_type": "iso_8601"},
    ],
)
def test_register_image_from_requires_timestamp_overrides_together(timestamp_overrides: dict[str, object]) -> None:
    """A partial timestamp override is rejected rather than silently mixing old and new metadata."""
    clients = _clients()
    extractor = ContainerizedExtractor._from_proto(clients, _ext("ri.ext"))
    clients.registry.CreateImage.return_value = registry_pb2.CreateImageResponse(
        image=_img("ri.img.new", registry_pb2.CONTAINER_IMAGE_STATUS_READY)
    )

    with pytest.raises(ValueError, match="provided together"):
        extractor.register_image_from(_source_image(clients), tag="v2", **timestamp_overrides)

    clients.registry.CreateImage.assert_not_called()


def test_register_image_from_requires_timestamp_metadata_when_the_source_has_none() -> None:
    """Legacy images without timestamp metadata require an explicit replacement contract."""
    clients = _clients()
    extractor = ContainerizedExtractor._from_proto(clients, _ext("ri.ext"))

    with pytest.raises(ValueError, match="no default timestamp metadata"):
        extractor.register_image_from(_source_image(clients, with_timestamp_metadata=False), tag="v2")

    clients.registry.CreateImage.assert_not_called()


@pytest.mark.parametrize("output_format", [FileOutputFormat.JSON_L, FileOutputFormat.PARQUET_TAR])
def test_register_image_from_rejects_non_ingestible_output_formats(output_format: FileOutputFormat) -> None:
    """Derived images apply the same output-format guard as tarball registrations."""
    clients = _clients()
    extractor = ContainerizedExtractor._from_proto(clients, _ext("ri.ext"))
    clients.registry.CreateImage.return_value = registry_pb2.CreateImageResponse(
        image=_img("ri.img.new", registry_pb2.CONTAINER_IMAGE_STATUS_READY)
    )

    with pytest.raises(ValueError, match=output_format.name):
        extractor.register_image_from(_source_image(clients), tag="v2", output_format=output_format)

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
