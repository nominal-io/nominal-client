from __future__ import annotations

from pathlib import Path

import click

from nominal.cli.util.global_decorators import client_options, global_options
from nominal.core.client import NominalClient
from nominal.core.filetype import FileType


@click.group(name="container-registry")
def container_registry_cmd() -> None:
    pass


@container_registry_cmd.command("upload")
@click.option("-n", "--name", required=True, help="image name (e.g. 'my-extractor')")
@click.option("-t", "--tag", required=True, help="image tag (e.g. 'v1.2.3')")
@click.option("-f", "--file", required=True, help="path to the container image tarball to upload")
@client_options
@global_options
def upload(name: str, tag: str, file: str, client: NominalClient) -> None:
    """Upload a container image tarball to Nominal's registry."""
    path = Path(file)
    file_type = FileType.from_path(path)
    with open(path, "rb") as f:
        image = client.upload_container_image_from_io(f, name, tag, file_type=file_type)
    click.echo(image)
