from __future__ import annotations

import json
import pathlib
import sys
from typing import Sequence

import click
from conjure_python_client import ConjureDecoder
from nominal_api import ingest_api

from nominal.cli.util.global_decorators import client_options, global_options
from nominal.core.client import NominalClient, WorkspaceSearchType


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
@click.option(
    "--workspace",
    "workspace_rid",
    help="Override the `workspace` field in the JSON. Defaults to the active profile's workspace.",
)
@client_options
@global_options
def register(
    config_file: pathlib.Path | None,
    name: str | None,
    container_image_rid: str | None,
    workspace_rid: str | None,
    client: NominalClient,
) -> None:
    """Register a containerized extractor from a JSON request payload.

    The JSON body is the conjure wire format of a RegisterContainerizedExtractorRequest --
    typically a per-package config checked into the repo. Values supplied via --name,
    --container-image-rid, or --workspace override fields in the JSON; --workspace also acts
    as a default if the JSON omits it.
    """
    raw = config_file.read_text() if config_file is not None else sys.stdin.read()
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise click.BadParameter("top-level JSON must be an object")

    if name is not None:
        payload["name"] = name
    if container_image_rid is not None:
        payload["containerImageRid"] = container_image_rid
    if workspace_rid is not None:
        payload["workspace"] = workspace_rid
    if not payload.get("workspace"):
        payload["workspace"] = client._clients.resolve_default_workspace_rid()

    request = ConjureDecoder().decode(payload, ingest_api.RegisterContainerizedExtractorRequest)
    resp = client._clients.containerized_extractors.register_containerized_extractor(
        client._clients.auth_header, request
    )
    click.echo(resp.extractor_rid)


@containerized_extractor_cmd.command("get")
@click.option("-r", "--rid", required=True)
@client_options
@global_options
def get(rid: str, client: NominalClient) -> None:
    """Fetch a containerized extractor by its RID."""
    click.echo(client.get_containerized_extractor(rid))


@containerized_extractor_cmd.command("search")
@click.option("-s", "--search-text", help="Case-insensitive fuzzy match against extractor metadata.")
@click.option("labels", "--label", type=str, multiple=True, help="Label that must be present (repeat for multiple).")
@click.option(
    "properties",
    "--property",
    type=(str, str),
    multiple=True,
    help="Property KEY VALUE that must be present (repeat for multiple).",
)
@click.option(
    "--workspace",
    "workspace_rid",
    help=(
        "Workspace RID to filter to. If omitted, searches across all workspaces the user can "
        "access. Pass `default` to use the active profile's default workspace."
    ),
)
@client_options
@global_options
def search(
    search_text: str | None,
    labels: Sequence[str],
    properties: Sequence[tuple[str, str]],
    workspace_rid: str | None,
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
    for extractor in extractors:
        click.echo(extractor)


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
    type=(str, str),
    multiple=True,
    help="Replace the extractor's properties as KEY VALUE (repeat for multiple).",
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
    click.echo(extractor)


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
