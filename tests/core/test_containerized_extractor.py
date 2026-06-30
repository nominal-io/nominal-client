from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal import ts
from nominal.core.client import NominalClient
from nominal.core.containerized_extractor import (
    ContainerImage,
    ContainerImageStatus,
    ContainerizedExtractor,
    FileOutputFormat,
    TimestampMetadata,
    _build_search_filter,
    _create_containerized_extractor,
    _get_image,
    _parse_docker_load_image_ref,
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


def _img(
    r: str,
    *,
    tag: str = "v1",
    extractor_rid: str = "ri.ext",
    status: reg_pb2.ContainerImageStatus.ValueType = reg_pb2.CONTAINER_IMAGE_STATUS_UNSPECIFIED,
) -> reg_pb2.ContainerImage:
    from nominal.protos.types.time import timestamp_parsers_pb2 as time_pb2

    return reg_pb2.ContainerImage(
        rid=r,
        tag=tag,
        extractor_rid=extractor_rid,
        status=status,
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


def test_client_upsert_containerized_extractor_updates_existing_match() -> None:
    """CI can update an existing extractor by exact name without knowing its RID."""
    clients = _clients()
    existing = ext_pb2.ContainerizedExtractor(
        rid="ri.ext",
        workspace_rid="ri.workspace.default",
        name="parser",
        description="old",
        is_archived=True,
    )
    updated = ext_pb2.ContainerizedExtractor(
        rid="ri.ext",
        workspace_rid="ri.workspace.default",
        name="parser",
        description="new",
        is_archived=False,
    )
    clients.containerized_extractor.SearchContainerizedExtractors.return_value = (
        ext_pb2.SearchContainerizedExtractorsResponse(extractors=[existing])
    )
    clients.containerized_extractor.UpdateContainerizedExtractor.return_value = (
        ext_pb2.UpdateContainerizedExtractorResponse(extractor=updated)
    )
    client = NominalClient(_clients=clients)

    extractor = client.upsert_containerized_extractor("parser", description="new", workspace_rid="ri.workspace.default")

    assert extractor.description == "new"
    assert extractor.is_archived is False
    request = clients.containerized_extractor.UpdateContainerizedExtractor.call_args.args[0]
    assert request.description == "new"
    assert request.is_archived is False


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


def test_search_images_can_filter_by_extractor_rid() -> None:
    """Extractor scoping filters registry search results client-side."""
    clients = _clients()
    clients.registry.SearchImages.return_value = reg_pb2.SearchImagesResponse(
        images=[
            _img("i1", extractor_rid="ri.ext.1"),
            _img("i2", extractor_rid="ri.ext.2"),
        ],
        next_page_token="",
    )

    results = _search_images(clients, extractor_rid="ri.ext.2", workspace_rid=None)

    assert [i.rid for i in results] == ["i2"]


def test_extractor_get_image_by_tag_scopes_to_own_images() -> None:
    """Tag lookup ignores images with the same tag registered to other extractors."""
    clients = _clients()
    clients.registry.SearchImages.return_value = reg_pb2.SearchImagesResponse(
        images=[
            _img("i1", tag="same", extractor_rid="ri.other"),
            _img("i2", tag="same", extractor_rid="ri.ext"),
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
    existing = _img("ri.img.1", tag="v1", extractor_rid="ri.ext", status=reg_pb2.CONTAINER_IMAGE_STATUS_READY)
    clients.registry.SearchImages.return_value = reg_pb2.SearchImagesResponse(images=[existing], next_page_token="")
    updated = _ext("ri.ext")
    updated.active_container_image.CopyFrom(existing)
    clients.containerized_extractor.UpdateContainerizedExtractor.return_value = (
        ext_pb2.UpdateContainerizedExtractorResponse(extractor=updated)
    )
    extractor = ContainerizedExtractor._from_proto(clients, _ext("ri.ext"))

    image = extractor.register_image(
        "unused.tar",
        tag="v1",
        inputs=(),
        timestamp=TimestampMetadata(series_name="ts", timestamp_type=ts.Iso8601()),
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
    created = _img("ri.img.1", tag="v1", extractor_rid="ri.ext", status=reg_pb2.CONTAINER_IMAGE_STATUS_PENDING)
    ready = _img("ri.img.1", tag="v1", extractor_rid="ri.ext", status=reg_pb2.CONTAINER_IMAGE_STATUS_READY)
    clients.registry.SearchImages.return_value = reg_pb2.SearchImagesResponse(images=[], next_page_token="")
    clients.registry.CreateImage.return_value = reg_pb2.CreateImageResponse(image=created)
    clients.registry.GetImage.return_value = reg_pb2.GetImageResponse(image=ready)
    updated = _ext("ri.ext")
    updated.active_container_image.CopyFrom(ready)
    clients.containerized_extractor.UpdateContainerizedExtractor.return_value = (
        ext_pb2.UpdateContainerizedExtractorResponse(extractor=updated)
    )
    extractor = ContainerizedExtractor._from_proto(clients, _ext("ri.ext"))

    image = extractor.register_image(
        tarball,
        tag="v1",
        inputs=(),
        timestamp=TimestampMetadata(series_name="ts", timestamp_type=ts.Iso8601()),
        wait_until_ready=True,
        poll_interval_seconds=0.001,
        activate=True,
    )

    assert image.status is ContainerImageStatus.READY
    assert uploaded_paths == [tarball]
    create_request = clients.registry.CreateImage.call_args.args[0]
    assert create_request.object_path == "s3://bucket/image.tar"
    assert clients.containerized_extractor.UpdateContainerizedExtractor.called


def test_build_search_filter_selects_the_right_oneof() -> None:
    """tag/status map to their filters; both compose into the proto's `and` oneof; neither yields no filter."""
    assert _build_search_filter(None, None) is None
    assert _build_search_filter("v1", None).WhichOneof("filter") == "tag"
    assert _build_search_filter(None, ContainerImageStatus.READY).WhichOneof("filter") == "status"
    assert _build_search_filter("v1", ContainerImageStatus.READY).WhichOneof("filter") == "and"


def test_parse_docker_load_image_ref_requires_one_loaded_ref() -> None:
    """Squash helper accepts Docker's image-ref and image-id load output variants."""
    assert _parse_docker_load_image_ref("Loaded image: repo/image:tag\n") == "repo/image:tag"
    assert _parse_docker_load_image_ref("Loaded image ID: sha256:abc\n") == "sha256:abc"
    with pytest.raises(RuntimeError, match="exactly one image reference"):
        _parse_docker_load_image_ref("Loaded image: a\nLoaded image: b\n")
