from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal import ts
from nominal.core.client import NominalClient
from nominal.core.container_image import ContainerImageStatus, FileOutputFormat
from nominal.core.containerized_extractor import (
    ContainerizedExtractor,
    _create_containerized_extractor,
    _parse_docker_load_image_ref,
    _search_containerized_extractors,
)
from nominal.core.exceptions import NominalContainerImageError
from nominal.protos.ingest.v2 import containerized_extractor_pb2
from nominal.protos.registry.v2 import registry_pb2
from nominal.protos.types.time import timestamp_parsers_pb2


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


def _img(
    rid: str,
    status: registry_pb2.ContainerImageStatus.ValueType,
    *,
    tag: str = "v1",
    extractor_rid: str = "ri.ext",
) -> registry_pb2.ContainerImage:
    return registry_pb2.ContainerImage(
        rid=rid,
        tag=tag,
        extractor_rid=extractor_rid,
        status=status,
        file_output_format=registry_pb2.FILE_OUTPUT_FORMAT_PARQUET,
        default_timestamp_metadata=registry_pb2.TimestampMetadata(
            series_name="ts",
            timestamp_type=timestamp_parsers_pb2.TimestampType(
                absolute=timestamp_parsers_pb2.AbsoluteTimestamp(iso8601=timestamp_parsers_pb2.Iso8601Timestamp())
            ),
        ),
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


def test_client_upsert_containerized_extractor_updates_existing_match() -> None:
    """CI can update an existing extractor by exact name without knowing its RID."""
    clients = _clients()
    existing = containerized_extractor_pb2.ContainerizedExtractor(
        rid="ri.ext",
        workspace_rid="ri.workspace.default",
        name="parser",
        description="old",
        is_archived=True,
    )
    updated = containerized_extractor_pb2.ContainerizedExtractor(
        rid="ri.ext",
        workspace_rid="ri.workspace.default",
        name="parser",
        description="new",
        is_archived=False,
    )
    clients.containerized_extractor.SearchContainerizedExtractors.return_value = (
        containerized_extractor_pb2.SearchContainerizedExtractorsResponse(extractors=[existing])
    )
    clients.containerized_extractor.UpdateContainerizedExtractor.return_value = (
        containerized_extractor_pb2.UpdateContainerizedExtractorResponse(extractor=updated)
    )
    client = NominalClient(_clients=clients)

    extractor = client.upsert_containerized_extractor("parser", description="new", workspace="ri.workspace.default")

    assert extractor.description == "new"
    assert extractor.is_archived is False
    request = clients.containerized_extractor.UpdateContainerizedExtractor.call_args.args[0]
    assert request.description == "new"
    assert request.is_archived is False


def test_extractor_get_image_by_tag_scopes_to_own_images() -> None:
    """Tag lookup ignores images with the same tag registered to other extractors."""
    clients = _clients()
    clients.registry.SearchImages.return_value = registry_pb2.SearchImagesResponse(
        images=[
            _img("i1", registry_pb2.CONTAINER_IMAGE_STATUS_READY, tag="same", extractor_rid="ri.other"),
            _img("i2", registry_pb2.CONTAINER_IMAGE_STATUS_READY, tag="same", extractor_rid="ri.ext"),
        ],
        next_page_token="",
    )
    extractor = ContainerizedExtractor._from_proto(clients, _ext("ri.ext"))

    image = extractor.get_image_by_tag("same")

    assert image is not None
    assert image.rid == "i2"


def test_register_image_reuses_existing_matching_contract_and_activates() -> None:
    """CI can rerun safely when an immutable image tag already exists."""
    clients = _clients()
    existing = _img("ri.img.1", registry_pb2.CONTAINER_IMAGE_STATUS_READY, tag="v1", extractor_rid="ri.ext")
    clients.registry.SearchImages.return_value = registry_pb2.SearchImagesResponse(
        images=[existing], next_page_token=""
    )
    updated = _ext("ri.ext")
    updated.active_container_image.CopyFrom(existing)
    clients.containerized_extractor.UpdateContainerizedExtractor.return_value = (
        containerized_extractor_pb2.UpdateContainerizedExtractorResponse(extractor=updated)
    )
    extractor = ContainerizedExtractor._from_proto(clients, _ext("ri.ext"))

    image = extractor.register_image(
        "unused.tar",
        tag="v1",
        inputs=(),
        default_timestamp_column="ts",
        default_timestamp_type=ts.Iso8601(),
        reuse_existing=True,
        activate=True,
        wait_until_ready=True,
    )

    assert image.rid == "ri.img.1"
    clients.registry.CreateImage.assert_not_called()
    request = clients.containerized_extractor.UpdateContainerizedExtractor.call_args.args[0]
    assert request.active_container_image_rid == "ri.img.1"


def test_register_image_uploads_waits_and_activates_new_image(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """New image registration uploads the tarball, waits for READY, and activates the image."""
    from nominal.core import containerized_extractor as module

    tarball = tmp_path / "image.tar"
    tarball.write_bytes(b"tar")
    uploaded_paths = []

    def fake_upload(*args, **kwargs) -> str:
        uploaded_paths.append(args[2])
        return "s3://bucket/image.tar"

    monkeypatch.setattr(module, "upload_multipart_file", fake_upload)
    clients = _clients()
    created = _img("ri.img.1", registry_pb2.CONTAINER_IMAGE_STATUS_PENDING, tag="v1", extractor_rid="ri.ext")
    ready = _img("ri.img.1", registry_pb2.CONTAINER_IMAGE_STATUS_READY, tag="v1", extractor_rid="ri.ext")
    clients.registry.SearchImages.return_value = registry_pb2.SearchImagesResponse(images=[], next_page_token="")
    clients.registry.CreateImage.return_value = registry_pb2.CreateImageResponse(image=created)
    clients.registry.GetImage.return_value = registry_pb2.GetImageResponse(image=ready)
    updated = _ext("ri.ext")
    updated.active_container_image.CopyFrom(ready)
    clients.containerized_extractor.UpdateContainerizedExtractor.return_value = (
        containerized_extractor_pb2.UpdateContainerizedExtractorResponse(extractor=updated)
    )
    extractor = ContainerizedExtractor._from_proto(clients, _ext("ri.ext"))

    image = extractor.register_image(
        tarball,
        tag="v1",
        inputs=(),
        default_timestamp_column="ts",
        default_timestamp_type=ts.Iso8601(),
        wait_until_ready=True,
        poll_interval_seconds=0.001,
        activate=True,
    )

    assert image.status is ContainerImageStatus.READY
    assert uploaded_paths == [tarball]
    create_request = clients.registry.CreateImage.call_args.args[0]
    assert create_request.object_path == "s3://bucket/image.tar"
    assert clients.containerized_extractor.UpdateContainerizedExtractor.called


def test_parse_docker_load_image_ref_requires_one_loaded_ref() -> None:
    """Squash helper accepts Docker's image-ref and image-id load output variants."""
    assert _parse_docker_load_image_ref("Loaded image: repo/image:tag\n") == "repo/image:tag"
    assert _parse_docker_load_image_ref("Loaded image ID: sha256:abc\n") == "sha256:abc"
    with pytest.raises(RuntimeError, match="exactly one image reference"):
        _parse_docker_load_image_ref("Loaded image: a\nLoaded image: b\n")
