from __future__ import annotations

import json
import pathlib
import sys
from typing import Sequence

import click
from conjure_python_client import ConjureDecoder, ConjureEncoder
from nominal_api import ingest_api
from rich.box import ASCII
from rich.console import Console
from rich.markup import escape
from rich.style import Style
from rich.table import Column, Table

from nominal.cli.util.click_types import parse_key_value
from nominal.cli.util.format import emit_records, render_labels, render_properties
from nominal.cli.util.global_decorators import client_options, global_options, output_fmt_options
from nominal.core.client import NominalClient, WorkspaceSearchType
from nominal.core.containerized_extractors import ContainerizedExtractor


@click.group(name="containerized-extractor")
def containerized_extractor_cmd() -> None:
    pass


@containerized_extractor_cmd.command("register")
@click.option(
    "-c",
    "--config",
    "config_file",
    type=click.Path(exists=True, dir_okay=False, resolve_path=True, path_type=pathlib.Path),
    help=(
        "Path to a JSON file containing a RegisterContainerizedExtractorRequest body in conjure "
        "wire format (camelCase, with `type` discriminators on union fields). If omitted, the "
        "JSON is read from stdin."
    ),
)
@click.option(
    "-n",
    "--name",
    help="Override the `name` field in the JSON. Useful when reusing one config across packages.",
)
@click.option(
    "-r",
    "--container-image-rid",
    help=(
        "Override the `containerImageRid` field in the JSON. Useful in CI where the image is "
        "uploaded as a separate step and its RID is unknown when the config is checked in."
    ),
)
@client_options
@global_options
def register(
    config_file: pathlib.Path | None,
    name: str | None,
    container_image_rid: str | None,
    client: NominalClient,
) -> None:
    """Register a containerized extractor from a JSON request payload.

    The JSON body is the conjure wire format of a RegisterContainerizedExtractorRequest --
    typically a per-package config checked into the repo. Values supplied via --name or
    --container-image-rid override fields in the JSON. The `workspace` field must NOT be
    present in the JSON; the workspace is always taken from the active config profile.
    """
    try:
        raw = config_file.read_text() if config_file is not None else sys.stdin.read()
    except (OSError, UnicodeDecodeError) as ex:
        raise click.BadParameter(f"failed to read input: {ex}") from ex
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as ex:
        raise click.BadParameter(f"invalid JSON: {ex.msg}") from ex
    if not isinstance(payload, dict):
        raise click.BadParameter("top-level JSON must be an object")

    if "workspace" in payload:
        raise click.BadParameter("`workspace` must not be set in the JSON; it comes from the active config profile.")
    if name is not None:
        payload["name"] = name
    if container_image_rid is not None:
        payload["containerImageRid"] = container_image_rid
    payload["workspace"] = client._clients.resolve_default_workspace_rid()

    request = ConjureDecoder().decode(payload, ingest_api.RegisterContainerizedExtractorRequest)
    click.echo(ContainerizedExtractor.register_from_request(client._clients, request))


@containerized_extractor_cmd.command("get")
@click.option("-r", "--rid", required=True)
@output_fmt_options
@client_options
@global_options
def get(rid: str, output_format: str, client: NominalClient) -> None:
    """Fetch a containerized extractor by its RID."""
    extractor = client.get_containerized_extractor(rid)
    _emit_extractors([extractor], output_format)


@containerized_extractor_cmd.command("search")
@click.option("-s", "--search-text", help="Case-insensitive fuzzy match against extractor metadata.")
@click.option("labels", "--label", type=str, multiple=True, help="Label that must be present (repeat for multiple).")
@click.option(
    "properties",
    "--property",
    type=str,
    multiple=True,
    callback=parse_key_value,
    help="Property KEY=VALUE that must be present (repeat for multiple).",
)
@click.option(
    "--workspace",
    "workspace_rid",
    help=(
        "Workspace RID to filter to. If omitted, searches across all workspaces the user can "
        "access. Pass `default` to use the active profile's default workspace."
    ),
)
@output_fmt_options
@client_options
@global_options
def search(
    search_text: str | None,
    labels: Sequence[str],
    properties: Sequence[tuple[str, str]],
    workspace_rid: str | None,
    output_format: str,
    client: NominalClient,
) -> None:
    """Search for containerized extractors. Filters are ANDed together."""
    if workspace_rid is None:
        workspace: WorkspaceSearchType | str = WorkspaceSearchType.ALL
    elif workspace_rid == "default":
        workspace = WorkspaceSearchType.DEFAULT
    else:
        workspace = workspace_rid

    extractors = client.search_containerized_extractors(
        search_text=search_text,
        labels=list(labels) or None,
        properties=dict(properties) or None,
        workspace=workspace,
    )
    _emit_extractors(extractors, output_format)


@containerized_extractor_cmd.command("update")
@click.option("-r", "--rid", required=True)
@click.option("-n", "--name", help="Replace the extractor's name.")
@click.option("-d", "--description", help="Replace the extractor's description.")
@click.option(
    "labels",
    "--label",
    type=str,
    multiple=True,
    help="Replace the extractor's labels (repeat for multiple). Pass --clear-labels to set to empty.",
)
@click.option("--clear-labels", is_flag=True, help="Set labels to the empty list.")
@click.option(
    "properties",
    "--property",
    type=str,
    multiple=True,
    callback=parse_key_value,
    help="Replace the extractor's properties as KEY=VALUE (repeat for multiple).",
)
@click.option("--clear-properties", is_flag=True, help="Set properties to the empty mapping.")
@click.option(
    "tags",
    "--tag",
    type=str,
    multiple=True,
    help="Replace the docker image tag list (repeat for multiple).",
)
@click.option("--default-tag", help="Default docker image tag to use when running the extractor.")
@output_fmt_options
@client_options
@global_options
def update(
    rid: str,
    name: str | None,
    description: str | None,
    labels: Sequence[str],
    clear_labels: bool,
    properties: Sequence[tuple[str, str]],
    clear_properties: bool,
    tags: Sequence[str],
    default_tag: str | None,
    output_format: str,
    client: NominalClient,
) -> None:
    """Update mutable fields of a containerized extractor.

    Only the flags you pass are sent. Repeated flags (--label, --property, --tag) replace the
    full set on the server side; pass --clear-labels or --clear-properties to set them empty.
    """
    extractor = client.get_containerized_extractor(rid)
    extractor.update(
        name=name,
        description=description,
        labels=list(labels) if labels else ([] if clear_labels else None),
        properties=dict(properties) if properties else ({} if clear_properties else None),
        tags=list(tags) if tags else None,
        default_tag=default_tag,
    )
    _emit_extractors([extractor], output_format)


@containerized_extractor_cmd.command("archive")
@click.option("-r", "--rid", required=True)
@client_options
@global_options
def archive(rid: str, client: NominalClient) -> None:
    """Archive a containerized extractor."""
    client.get_containerized_extractor(rid).archive()
    click.echo(f"archived {rid}")


@containerized_extractor_cmd.command("unarchive")
@click.option("-r", "--rid", required=True)
@client_options
@global_options
def unarchive(rid: str, client: NominalClient) -> None:
    """Unarchive a containerized extractor."""
    client.get_containerized_extractor(rid).unarchive()
    click.echo(f"unarchived {rid}")


def _emit_extractors(extractors: Sequence[ContainerizedExtractor], output_format: str) -> None:
    emit_records(
        extractors,
        output_format,
        to_dict=_extractor_to_wire_dict,
        render_table=_print_extractor_table,
        render_detail=_print_extractor_detail,
    )


def _extractor_to_wire_dict(extractor: ContainerizedExtractor) -> dict[str, object]:
    """Serialize a ContainerizedExtractor to a conjure-wire-format dict (camelCase)."""
    timestamp_metadata = extractor.default_timestamp_metadata
    return {
        "rid": extractor.rid,
        "name": extractor.name,
        "description": extractor.description,
        "image": ConjureEncoder.do_encode(extractor.image._to_conjure()),
        "containerImageRid": extractor.container_image_rid,
        "inputs": [ConjureEncoder.do_encode(inp._to_conjure()) for inp in extractor.inputs],
        "labels": list(extractor.labels),
        "properties": dict(extractor.properties),
        "timestampMetadata": (
            None if timestamp_metadata is None else ConjureEncoder.do_encode(timestamp_metadata._to_conjure())
        ),
    }


def _format_image_ref(extractor: ContainerizedExtractor) -> str:
    """Render the column-friendly image reference for an extractor.

    Self-hosted extractors carry their image as a `containerImageRid` and leave the
    `image` field as placeholder values (which would otherwise show as `/:latest`); for
    those, surface the RID directly. Legacy self-hosted rows that have neither a real
    image nor a populated `containerImageRid` render as `-` instead of the placeholder.
    """
    if extractor.container_image_rid:
        return extractor.container_image_rid
    image = extractor.image
    if not image.registry and not image.repository:
        return "-"
    return f"{image.registry}/{image.repository}:{image.tag_details.default_tag}"


def _print_extractor_table(extractors: Sequence[ContainerizedExtractor]) -> None:
    console = Console()
    if not extractors:
        console.print("No containerized extractors matched.", style=Style(color="yellow"))
        return
    table = Table(
        Column("RID", style=Style(italic=True, dim=True), ratio=3, overflow="fold"),
        Column("Name", style=Style(color="white", bold=True), ratio=2, overflow="fold"),
        Column("Image", style=Style(color="cyan"), ratio=4, overflow="fold"),
        Column("Labels", style=Style(color="green"), ratio=3, overflow="fold"),
        Column("Properties", style=Style(color="magenta"), ratio=4, overflow="fold"),
        title=f"Containerized Extractors ({len(extractors)})",
        expand=True,
        box=ASCII,
    )
    for extractor in extractors:
        table.add_row(
            escape(extractor.rid),
            escape(extractor.name),
            escape(_format_image_ref(extractor)),
            escape(render_labels(extractor.labels)),
            escape(render_properties(extractor.properties)),
        )
    console.print(table)


def _print_extractor_detail(extractor: ContainerizedExtractor) -> None:
    console = Console()
    image = extractor.image
    table = Table(
        Column("Field", style=Style(color="white", bold=True), ratio=1, overflow="fold"),
        Column("Value", style=Style(color="cyan"), ratio=4, overflow="fold"),
        title=f"{escape(extractor.name)} ({escape(extractor.rid)})",
        expand=True,
        box=ASCII,
        show_header=False,
    )
    table.add_row("Description", escape(extractor.description or "-"))
    if extractor.container_image_rid:
        table.add_row("Container Image", escape(extractor.container_image_rid))
    elif not image.registry and not image.repository:
        table.add_row("Container Image", "-")
    else:
        table.add_row("Image", escape(f"{image.registry}/{image.repository}"))
        table.add_row("Tags", escape(", ".join(image.tag_details.tags) or "-"))
        table.add_row("Default tag", escape(image.tag_details.default_tag))
        if image.command:
            table.add_row("Command", escape(image.command))
    table.add_row("Labels", escape(render_labels(extractor.labels)))
    table.add_row("Properties", escape(render_properties(extractor.properties)))
    if extractor.inputs:
        formatted_inputs = "\n".join(
            f"- {inp.name} (env={inp.environment_variable}, "
            f"suffixes=[{', '.join(inp.file_suffixes) or 'any'}], "
            f"{'required' if inp.required else 'optional'})"
            for inp in extractor.inputs
        )
        table.add_row("Inputs", escape(formatted_inputs))
    else:
        table.add_row("Inputs", "-")
    if extractor.default_timestamp_metadata is not None:
        ts_meta = extractor.default_timestamp_metadata
        table.add_row("Timestamp", escape(f"{ts_meta.series_name} ({ts_meta.timestamp_type})"))
    console.print(table)
