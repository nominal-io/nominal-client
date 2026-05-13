from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from nominal.cli.container_registry import container_registry_cmd
from nominal.core.container_image import ContainerImage, ContainerImageStatus


def _image(*, rid: str = "ri.container-image.test.1", size_bytes: int | None = 42) -> ContainerImage:
    return ContainerImage(
        rid=rid,
        name="extractor",
        tag="abc123",
        status=ContainerImageStatus.READY,
        created_at=2_000_000_003,
        size_bytes=size_bytes,
        workspace_rid="ri.workspace.default",
        _clients=SimpleNamespace(auth_header="Bearer token", registry=MagicMock()),
    )


def _invoke(command: list[str], *, client: SimpleNamespace):
    with patch("nominal.cli.util.global_decorators.NominalClient.from_profile", return_value=client):
        return CliRunner().invoke(container_registry_cmd, command)


def test_upload_reads_tarball_and_prints_image_rid(tmp_path) -> None:
    tarball = tmp_path / "image.tar"
    tarball.write_bytes(b"tarball")

    def upload_container_image_from_io(file_obj, name: str, tag: str) -> ContainerImage:
        assert file_obj.read() == b"tarball"
        assert name == "extractor"
        assert tag == "abc123"
        return _image()

    client = SimpleNamespace(upload_container_image_from_io=MagicMock(side_effect=upload_container_image_from_io))

    result = _invoke(
        [
            "upload",
            "--profile",
            "test",
            "--name",
            "extractor",
            "--tag",
            "abc123",
            "--file",
            str(tarball),
        ],
        client=client,
    )

    assert result.exit_code == 0
    assert result.output == "ri.container-image.test.1\n"
    client.upload_container_image_from_io.assert_called_once()


def test_search_emits_json_and_forwards_status_filter() -> None:
    client = SimpleNamespace(search_container_images=MagicMock(return_value=[_image()]))

    result = _invoke(
        [
            "search",
            "--profile",
            "test",
            "--name",
            "extractor",
            "--tag",
            "abc123",
            "--status",
            "ready",
            "--format",
            "json",
        ],
        client=client,
    )

    assert result.exit_code == 0
    assert json.loads(result.output) == {
        "rid": "ri.container-image.test.1",
        "name": "extractor",
        "tag": "abc123",
        "status": "CONTAINER_IMAGE_STATUS_READY",
        "createdAt": "1970-01-01T00:00:02.000000003Z",
        "sizeBytes": 42,
    }
    client.search_container_images.assert_called_once_with(
        name="extractor",
        tag="abc123",
        status=ContainerImageStatus.READY,
    )


def test_get_emits_json_for_single_image() -> None:
    client = SimpleNamespace(get_container_image=MagicMock(return_value=_image(size_bytes=None)))

    result = _invoke(
        ["get", "--profile", "test", "--rid", "ri.container-image.test.1", "--format", "json"],
        client=client,
    )

    assert result.exit_code == 0
    assert json.loads(result.output)["sizeBytes"] is None
    client.get_container_image.assert_called_once_with("ri.container-image.test.1")


def test_delete_requires_confirmation_unless_yes_is_passed() -> None:
    client = SimpleNamespace(delete_container_image=MagicMock())

    result = _invoke(
        ["delete", "--profile", "test", "--rid", "ri.container-image.test.1", "--yes"],
        client=client,
    )

    assert result.exit_code == 0
    assert result.output == "deleted ri.container-image.test.1\n"
    client.delete_container_image.assert_called_once_with("ri.container-image.test.1")
