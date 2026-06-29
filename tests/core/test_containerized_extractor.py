from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import grpc
import pytest

from nominal.core.client import NominalClient
from nominal.core.containerized_extractor import (
    ContainerImage,
    ContainerizedExtractor,
    FileOutputFormat,
    TimestampMetadata,
    _image_config_to_import_changes,
    _parse_docker_load_image_ref,
    _search_images,
)
from nominal.protos.ingest.v2 import containerized_extractor_pb2 as ext_pb2
from nominal.protos.registry.v2 import registry_pb2 as reg_pb2


def _clients() -> MagicMock:
    clients = MagicMock()
    clients.auth_header = "Bearer test-token"
    clients.resolve_default_workspace_rid.return_value = "ri.workspace.default"
    return clients


def _ext(rid: str, *, active_image: reg_pb2.ContainerImage | None = None) -> ext_pb2.ContainerizedExtractor:
    msg = ext_pb2.ContainerizedExtractor(rid=rid, workspace_rid="ri.workspace.default", name=rid, is_archived=False)
    if active_image is not None:
        msg.active_container_image.CopyFrom(active_image)
    return msg


def _archived_ext(rid: str, *, name: str, description: str | None) -> ext_pb2.ContainerizedExtractor:
    msg = ext_pb2.ContainerizedExtractor(
        rid=rid,
        workspace_rid="ri.workspace.default",
        name=name,
        is_archived=True,
    )
    if description is not None:
        msg.description = description
    return msg


def test_search_extractors_follows_pagination_cursors() -> None:
    """Search accumulates results across pages until the server returns an empty next_page_token."""
    clients = _clients()
    page1 = ext_pb2.SearchContainerizedExtractorsResponse(extractors=[_ext("a")], next_page_token="tok")
    page2 = ext_pb2.SearchContainerizedExtractorsResponse(extractors=[_ext("b")], next_page_token="")
    clients.containerized_extractor.SearchContainerizedExtractors.side_effect = [page1, page2]

    results = ContainerizedExtractor._search(clients, include_archived=False, file_extension=None, workspace_rid=None)

    assert [e.rid for e in results] == ["a", "b"]
    assert clients.containerized_extractor.SearchContainerizedExtractors.call_count == 2
    second_call_request = clients.containerized_extractor.SearchContainerizedExtractors.call_args_list[1].args[0]
    assert second_call_request.next_page_token == "tok"


def _img(
    r: str,
    *,
    tag: str = "v1",
    extractor_rid: str = "ri.ext",
    status: int = reg_pb2.CONTAINER_IMAGE_STATUS_READY,
) -> reg_pb2.ContainerImage:
    from nominal.protos.types.time import timestamp_parsers_pb2 as time_pb2

    return reg_pb2.ContainerImage(
        rid=r,
        tag=tag,
        status=status,
        extractor_rid=extractor_rid,
        file_output_format=1,
        default_timestamp_metadata=reg_pb2.TimestampMetadata(
            series_name="ts",
            timestamp_type=time_pb2.TimestampType(
                absolute=time_pb2.AbsoluteTimestamp(iso8601=time_pb2.Iso8601Timestamp())
            ),
        ),
    )


def test_create_defaults_workspace_to_client_default() -> None:
    """When workspace_rid is omitted, the request uses the client's resolved default workspace."""
    clients = _clients()
    clients.containerized_extractor.CreateContainerizedExtractor.return_value = (
        ext_pb2.CreateContainerizedExtractorResponse(extractor=_ext("a"))
    )

    ContainerizedExtractor._create(clients, "a", description=None, workspace_rid=None)

    request = clients.containerized_extractor.CreateContainerizedExtractor.call_args.args[0]
    assert request.workspace_rid == "ri.workspace.default"


def test_upsert_containerized_extractor_creates_when_missing() -> None:
    """Upsert creates an extractor when exact-name search finds none."""
    clients = _clients()
    clients.containerized_extractor.SearchContainerizedExtractors.return_value = (
        ext_pb2.SearchContainerizedExtractorsResponse(extractors=[], next_page_token="")
    )
    clients.containerized_extractor.CreateContainerizedExtractor.return_value = (
        ext_pb2.CreateContainerizedExtractorResponse(extractor=_ext("ri.ext"))
    )

    extractor = NominalClient(_clients=clients).upsert_containerized_extractor("parser", description="desc")

    assert extractor.rid == "ri.ext"
    request = clients.containerized_extractor.CreateContainerizedExtractor.call_args.args[0]
    assert request.name == "parser"
    assert request.description == "desc"


def test_upsert_containerized_extractor_updates_existing() -> None:
    """Upsert unarchives and updates description for an existing exact-name match."""
    clients = _clients()
    clients.containerized_extractor.SearchContainerizedExtractors.return_value = (
        ext_pb2.SearchContainerizedExtractorsResponse(
            extractors=[_archived_ext("ri.ext", name="parser", description="old")],
            next_page_token="",
        )
    )
    clients.containerized_extractor.UpdateContainerizedExtractor.return_value = (
        ext_pb2.UpdateContainerizedExtractorResponse(extractor=_ext("ri.ext"))
    )

    extractor = NominalClient(_clients=clients).upsert_containerized_extractor("parser", description="new")

    assert extractor.rid == "ri.ext"
    request = clients.containerized_extractor.UpdateContainerizedExtractor.call_args.args[0]
    assert request.rid == "ri.ext"
    assert request.description == "new"
    assert request.is_archived is False


def test_upsert_containerized_extractor_rejects_duplicate_names() -> None:
    """Exact-name upsert requires one matching extractor at most."""
    clients = _clients()
    clients.containerized_extractor.SearchContainerizedExtractors.return_value = (
        ext_pb2.SearchContainerizedExtractorsResponse(
            extractors=[
                ext_pb2.ContainerizedExtractor(
                    rid="ri.ext.1",
                    workspace_rid="ri.workspace.default",
                    name="parser",
                    is_archived=False,
                ),
                ext_pb2.ContainerizedExtractor(
                    rid="ri.ext.2",
                    workspace_rid="ri.workspace.default",
                    name="parser",
                    is_archived=False,
                ),
            ],
            next_page_token="",
        )
    )

    with pytest.raises(ValueError, match="Multiple containerized extractors"):
        NominalClient(_clients=clients).upsert_containerized_extractor("parser")


def test_upsert_containerized_extractor_recovers_from_create_race(fake_rpc_error) -> None:
    """Concurrent creation can race; upsert re-searches after ALREADY_EXISTS."""
    clients = _clients()
    clients.containerized_extractor.SearchContainerizedExtractors.side_effect = [
        ext_pb2.SearchContainerizedExtractorsResponse(extractors=[], next_page_token=""),
        ext_pb2.SearchContainerizedExtractorsResponse(
            extractors=[
                ext_pb2.ContainerizedExtractor(
                    rid="ri.ext",
                    workspace_rid="ri.workspace.default",
                    name="parser",
                    is_archived=False,
                )
            ],
            next_page_token="",
        ),
    ]
    clients.containerized_extractor.CreateContainerizedExtractor.side_effect = fake_rpc_error(
        grpc.StatusCode.ALREADY_EXISTS, "already exists"
    )

    extractor = NominalClient(_clients=clients).upsert_containerized_extractor("parser")

    assert extractor.rid == "ri.ext"
    assert clients.containerized_extractor.SearchContainerizedExtractors.call_count == 2


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


def test_search_images_filters_to_extractor_rid() -> None:
    """Client-side extractor filtering avoids tag collisions across extractors."""
    clients = _clients()
    clients.registry.SearchImages.return_value = reg_pb2.SearchImagesResponse(
        images=[
            _img("i1", tag="release", extractor_rid="ri.ext.keep"),
            _img("i2", tag="release", extractor_rid="ri.ext.drop"),
        ],
        next_page_token="",
    )

    results = _search_images(clients, tag="release", extractor_rid="ri.ext.keep", workspace_rid=None)

    assert [i.rid for i in results] == ["i1"]


def test_from_proto_keeps_active_container_image_rid() -> None:
    """Fetched extractors retain the active image RID."""
    clients = _clients()

    extractor = ContainerizedExtractor._from_proto(clients, _ext("ri.ext", active_image=_img("ri.image")))

    assert extractor.active_container_image_rid == "ri.image"


def _extractor(clients: MagicMock) -> ContainerizedExtractor:
    return ContainerizedExtractor(
        rid="ri.ext",
        name="parser",
        description=None,
        is_archived=False,
        active_container_image_rid=None,
        _workspace_rid="ri.workspace.default",
        _clients=clients,
    )


def _timestamp() -> TimestampMetadata:
    return TimestampMetadata._from_proto(_img("timestamp-prototype").default_timestamp_metadata)


def test_register_image_reuses_existing_tag_before_upload(monkeypatch) -> None:
    """Immutable tags are reused without uploading duplicate tarballs."""
    clients = _clients()
    clients.registry.SearchImages.return_value = reg_pb2.SearchImagesResponse(
        images=[_img("ri.image", tag="release", extractor_rid="ri.ext")],
        next_page_token="",
    )
    upload = MagicMock(return_value="uploaded-object-unused")
    monkeypatch.setattr("nominal.core.containerized_extractor.upload_multipart_file", upload)

    image = _extractor(clients).register_image("image.tar", tag="release", inputs=(), timestamp=_timestamp())

    assert image.rid == "ri.image"
    upload.assert_not_called()
    clients.registry.CreateImage.assert_not_called()


def test_register_image_squashes_tarball_before_upload(monkeypatch) -> None:
    """The opt-in squash flag swaps the uploaded tarball for the SDK-flattened archive."""
    clients = _clients()
    clients.registry.SearchImages.return_value = reg_pb2.SearchImagesResponse(images=[], next_page_token="")
    clients.registry.CreateImage.return_value = reg_pb2.CreateImageResponse(
        image=_img("ri.image", tag="release", extractor_rid="ri.ext")
    )
    squash = MagicMock(return_value=Path("squashed.tar"))
    upload = MagicMock(return_value="uploaded-object")
    monkeypatch.setattr("nominal.core.containerized_extractor._squash_image_tarball", squash)
    monkeypatch.setattr("nominal.core.containerized_extractor.upload_multipart_file", upload)

    image = _extractor(clients).register_image(
        "image.tar",
        tag="release",
        inputs=(),
        timestamp=_timestamp(),
        squash_before_registering=True,
    )

    assert image.rid == "ri.image"
    squash.assert_called_once()
    assert squash.call_args.args[0] == Path("image.tar")
    assert upload.call_args.args[2] == Path("squashed.tar")


def test_register_image_reuses_existing_after_registry_push_collision(monkeypatch, fake_rpc_error) -> None:
    """A duplicate registry-layer push error can still resolve to the already-created image record."""
    clients = _clients()
    clients.registry.SearchImages.side_effect = [
        reg_pb2.SearchImagesResponse(images=[], next_page_token=""),
        reg_pb2.SearchImagesResponse(images=[_img("ri.image", tag="release", extractor_rid="ri.ext")]),
    ]
    clients.registry.CreateImage.side_effect = fake_rpc_error(
        grpc.StatusCode.INTERNAL, "Failed to push image to registry: BlobAlreadyExists"
    )
    upload = MagicMock(return_value="uploaded-object")
    monkeypatch.setattr("nominal.core.containerized_extractor.upload_multipart_file", upload)

    image = _extractor(clients).register_image("image.tar", tag="release", inputs=(), timestamp=_timestamp())

    assert image.rid == "ri.image"
    upload.assert_called_once()
    clients.registry.CreateImage.assert_called_once()


def test_register_image_does_not_reuse_generic_internal_error(monkeypatch, fake_rpc_error) -> None:
    """Only duplicate blob registry failures can reuse an existing immutable tag."""
    clients = _clients()
    clients.registry.SearchImages.return_value = reg_pb2.SearchImagesResponse(images=[], next_page_token="")
    clients.registry.CreateImage.side_effect = fake_rpc_error(grpc.StatusCode.INTERNAL, "registry unavailable")
    upload = MagicMock(return_value="uploaded-object")
    monkeypatch.setattr("nominal.core.containerized_extractor.upload_multipart_file", upload)

    with pytest.raises(Exception, match="registry unavailable"):
        _extractor(clients).register_image("image.tar", tag="release", inputs=(), timestamp=_timestamp())

    upload.assert_called_once()
    clients.registry.CreateImage.assert_called_once()


def test_register_image_reuse_rejects_contract_mismatch(monkeypatch) -> None:
    """A reused immutable tag must match the requested registration contract."""
    clients = _clients()
    clients.registry.SearchImages.return_value = reg_pb2.SearchImagesResponse(
        images=[_img("ri.image", tag="release", extractor_rid="ri.ext")],
        next_page_token="",
    )
    upload = MagicMock(return_value="uploaded-object-unused")
    monkeypatch.setattr("nominal.core.containerized_extractor.upload_multipart_file", upload)

    with pytest.raises(ValueError, match="output_format"):
        _extractor(clients).register_image(
            "image.tar",
            tag="release",
            inputs=(),
            timestamp=_timestamp(),
            output_format=FileOutputFormat.MANIFEST,
        )

    upload.assert_not_called()


def test_container_image_wait_until_ready_refreshes_pending_image(monkeypatch) -> None:
    """Polling mutates the image once registry status becomes READY."""
    clients = _clients()
    image = ContainerImage._from_proto(
        clients,
        "ri.workspace.default",
        _img("ri.image", status=reg_pb2.CONTAINER_IMAGE_STATUS_PENDING),
    )
    clients.registry.GetImage.return_value = reg_pb2.GetImageResponse(image=_img("ri.image"))
    monkeypatch.setattr("nominal.core.containerized_extractor.time.sleep", lambda _: None)

    returned = image.wait_until_ready(timeout_seconds=1, poll_interval_seconds=0.01)

    assert returned is image
    assert image.status.name == "READY"
    clients.registry.GetImage.assert_called_once()


def test_parse_docker_load_image_ref_accepts_single_loaded_ref() -> None:
    assert _parse_docker_load_image_ref("Loaded image: example:tag\n") == "example:tag"
    assert _parse_docker_load_image_ref("Loaded image ID: sha256:abc\n") == "sha256:abc"


def test_parse_docker_load_image_ref_rejects_multiple_refs() -> None:
    with pytest.raises(RuntimeError, match="exactly one image reference"):
        _parse_docker_load_image_ref("Loaded image: one:tag\nLoaded image: two:tag\n")


def test_image_config_to_import_changes_preserves_runtime_config() -> None:
    changes = _image_config_to_import_changes(
        {
            "Entrypoint": ["/bin/app"],
            "Cmd": ["--serve"],
            "Env": ["A=B"],
            "WorkingDir": "/work",
            "User": "1000",
            "StopSignal": "SIGTERM",
            "ExposedPorts": {"8080/tcp": {}},
            "Volumes": {"/data": {}},
            "Labels": {"label": "value"},
            "OnBuild": ["RUN echo ok"],
        }
    )

    assert changes == (
        'ENTRYPOINT ["/bin/app"]',
        'CMD ["--serve"]',
        "ENV A=B",
        "WORKDIR /work",
        "USER 1000",
        "STOPSIGNAL SIGTERM",
        "EXPOSE 8080/tcp",
        'VOLUME ["/data"]',
        'LABEL label="value"',
        "ONBUILD RUN echo ok",
    )
