from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from nominal.cli.container_registry import container_registry_cmd


def test_upload_help_omits_workspace_rid_option() -> None:
    result = CliRunner().invoke(container_registry_cmd, ["upload", "--help"])

    assert result.exit_code == 0
    assert "--workspace-rid" not in result.output


def test_upload_uses_profile_client_workspace(tmp_path) -> None:
    tarball = tmp_path / "image.tar"
    tarball.write_bytes(b"image")
    client = MagicMock()
    client.upload_container_image_from_io.return_value = SimpleNamespace(rid="ri.container-image.main.image.1")

    with patch("nominal.cli.util.global_decorators.NominalClient.from_profile", return_value=client):
        result = CliRunner().invoke(
            container_registry_cmd,
            [
                "upload",
                "-n",
                "extractor",
                "-t",
                "v1",
                "-f",
                str(tarball),
                "--profile",
                "default",
            ],
        )

    assert result.exit_code == 0, result.output
    assert result.output == "ri.container-image.main.image.1\n"
    client.upload_container_image_from_io.assert_called_once()
    call_args = client.upload_container_image_from_io.call_args
    assert call_args.kwargs == {}
    assert call_args.args[1:] == ("extractor", "v1")
