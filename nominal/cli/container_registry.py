from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

import click
from rich.box import ASCII
from rich.console import Console
from rich.filesize import decimal as format_filesize
from rich.markup import escape
from rich.style import Style
from rich.table import Column, Table

from nominal.cli.util.format import emit_records
from nominal.cli.util.global_decorators import client_options, global_options, output_fmt_options
from nominal.core.client import NominalClient
from nominal.core.container_image import ContainerImage, ContainerImageStatus
from nominal.ts import _SecondsNanos


@click.group(name="container-registry")
def container_registry_cmd() -> None:
    pass


@container_registry_cmd.command("upload")
@click.option("-n", "--name", required=True, help="Image name (typically the package name).")
@click.option("-t", "--tag", required=True, help="Image tag, typically a git short SHA.")
@click.option(
    "-f",
    "--file",
    "file_path",
    type=click.Path(exists=True, dir_okay=False, readable=True, resolve_path=True, path_type=Path),
    required=True,
    help="Path to the uncompressed `docker save`/OCI tarball.",
)
@client_options
@global_options
def upload(name: str, tag: str, file_path: Path, client: NominalClient) -> None:
    """Upload a container image tarball to Nominal's self-hosted registry.

    Prints the resulting container image RID on stdout, suitable for capturing in CI:

        IMAGE_RID=$(nom container-registry upload -n my-extractor -t $(git rev-parse --short HEAD) -f image.tar)
        nom containerized-extractor register -c config.json -n my-extractor -r "$IMAGE_RID"
    """
    with file_path.open("rb") as f:
        image = client.upload_container_image_from_io(f, name, tag)
    click.echo(image.rid)


@container_registry_cmd.command("get")
@click.option("-r", "--rid", required=True)
@output_fmt_options
@client_options
@global_options
def get(rid: str, output_format: str, client: NominalClient) -> None:
    """Fetch a container image by its RID."""
    image = client.get_container_image(rid)
    _emit_images([image], output_format)


_STATUS_CHOICES = tuple(s.name.lower() for s in ContainerImageStatus if s != ContainerImageStatus.UNSPECIFIED)


@container_registry_cmd.command("search")
@click.option("-n", "--name", help="Filter by exact image name.")
@click.option("-t", "--tag", help="Filter by exact image tag.")
@click.option(
    "--status",
    type=click.Choice(_STATUS_CHOICES, case_sensitive=False),
    help="Filter by lifecycle status.",
)
@output_fmt_options
@client_options
@global_options
def search(
    name: str | None,
    tag: str | None,
    status: str | None,
    output_format: str,
    client: NominalClient,
) -> None:
    """Search for container images. Filters are ANDed together."""
    status_enum = ContainerImageStatus[status.upper()] if status is not None else None
    images = client.search_container_images(
        name=name,
        tag=tag,
        status=status_enum,
    )
    _emit_images(images, output_format)


@container_registry_cmd.command("delete")
@click.option("-r", "--rid", required=True)
@click.option("--yes", is_flag=True, help="Skip the confirmation prompt.")
@client_options
@global_options
def delete(rid: str, yes: bool, client: NominalClient) -> None:
    """Delete a container image. Extractors that reference this image's RID will fail on
    subsequent ingests.
    """
    if not yes:
        click.confirm(f"Delete container image {rid}?", abort=True)
    client.delete_container_image(rid)
    click.echo(f"deleted {rid}")


def _emit_images(images: Sequence[ContainerImage], output_format: str) -> None:
    emit_records(
        images,
        output_format,
        to_dict=_image_to_wire_dict,
        render_table=_print_image_table,
        render_detail=_print_image_detail,
    )


def _image_to_wire_dict(image: ContainerImage) -> Mapping[str, Any]:
    return {
        "rid": image.rid,
        "name": image.name,
        "tag": image.tag,
        "status": image.status.value,
        "createdAt": _ns_to_iso(image.created_at),
        "sizeBytes": image.size_bytes,
    }


def _print_image_table(images: Sequence[ContainerImage]) -> None:
    console = Console()
    if not images:
        console.print("No container images found.", style=Style(color="yellow"))
        return
    table = Table(
        Column("RID", style=Style(italic=True, dim=True), ratio=4, overflow="fold"),
        Column("Name", style=Style(color="white", bold=True), ratio=2, overflow="fold"),
        Column("Tag", style=Style(color="cyan"), ratio=2, overflow="fold"),
        Column("Status", style=Style(color="green"), ratio=2, overflow="fold"),
        Column("Size", style=Style(color="magenta"), ratio=1, overflow="fold"),
        Column("Created", style=Style(dim=True), ratio=3, overflow="fold"),
        title=f"Container Images ({len(images)})",
        expand=True,
        box=ASCII,
    )
    for image in images:
        table.add_row(
            escape(image.rid),
            escape(image.name),
            escape(image.tag),
            escape(image.status.value),
            _format_size(image.size_bytes),
            escape(_ns_to_iso(image.created_at)),
        )
    console.print(table)


def _format_size(size_bytes: int | None) -> str:
    return "-" if size_bytes is None else format_filesize(size_bytes)


def _print_image_detail(image: ContainerImage) -> None:
    console = Console()
    table = Table(
        Column("Field", style=Style(color="white", bold=True), ratio=1, overflow="fold"),
        Column("Value", style=Style(color="cyan"), ratio=4, overflow="fold"),
        title=f"{escape(image.name)}:{escape(image.tag)} ({escape(image.rid)})",
        expand=True,
        box=ASCII,
        show_header=False,
    )
    table.add_row("Name", escape(image.name))
    table.add_row("Tag", escape(image.tag))
    table.add_row("Status", escape(image.status.value))
    table.add_row("Size", _format_size(image.size_bytes))
    table.add_row("Created", escape(_ns_to_iso(image.created_at)))
    console.print(table)


def _ns_to_iso(nanoseconds: int) -> str:
    return _SecondsNanos.from_nanoseconds(nanoseconds).to_iso8601()
