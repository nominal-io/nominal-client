from __future__ import annotations

from pathlib import Path

import click

from nominal.cli.util.global_decorators import client_options, global_options
from nominal.core.client import NominalClient


@click.group(name="container-image")
def container_image_cmd() -> None:
    pass


@container_image_cmd.command("upload")
@click.option("-n", "--name", required=True, help="Image name (typically the package name).")
@click.option("-t", "--tag", required=True, help="Image tag, typically a git short SHA.")
@click.option(
    "-f",
    "--file",
    "file_path",
    type=click.Path(exists=True, dir_okay=False, resolve_path=True, path_type=Path),
    required=True,
    help="Path to the uncompressed `docker save`/OCI tarball.",
)
@click.option(
    "--workspace",
    "workspace_rid",
    help="Workspace RID to upload into. Defaults to the active profile's workspace.",
)
@client_options
@global_options
def upload(
    name: str,
    tag: str,
    file_path: Path,
    workspace_rid: str | None,
    client: NominalClient,
) -> None:
    """Upload a docker image tarball to Nominal's self-hosted container registry.

    Prints the resulting container image RID on stdout, suitable for capturing in CI:

        IMAGE_RID=$(nom container-image upload -n my-extractor -t $(git rev-parse --short HEAD) -f image.tar)
        nom containerized-extractor register -c config.json -n my-extractor -r "$IMAGE_RID"
    """
    image = client.upload_container_image(
        name=name,
        tag=tag,
        file=file_path,
        workspace_rid=workspace_rid,
    )
    click.echo(image.rid)
