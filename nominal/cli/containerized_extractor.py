from __future__ import annotations

import json
import pathlib
import sys

import click
from conjure_python_client import ConjureDecoder
from nominal_api import ingest_api

from nominal.cli.util.global_decorators import client_options, global_options
from nominal.core.client import NominalClient


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
