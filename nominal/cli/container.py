from __future__ import annotations

import collections
import json
import pathlib
from typing import Any, Mapping, NamedTuple, Protocol, Sequence, TypeVar, cast, get_args

import click

from nominal import ts
from nominal.cli.util.format import emit_table, table_data_to_string
from nominal.cli.util.global_decorators import client_options, global_options
from nominal.core.client import NominalClient
from nominal.core.container_image import (
    REGISTERABLE_OUTPUT_FORMATS,
    ContainerImage,
    ContainerImageStatus,
    FileExtractionInput,
    FileExtractionParameter,
    FileOutputFormat,
)
from nominal.core.containerized_extractor import ContainerizedExtractor
from nominal.ts import _SecondsNanos


@click.group(name="container")
def container_cmd() -> None:
    """Work with containerized extractors and the container images they run.

    An extractor carries identity; its execution contract (inputs, parameters, output format,
    timestamp defaults) lives on the container images registered against it, exactly one of
    which is active.
    """


@container_cmd.group(name="extractor")
def extractor_cmd() -> None:
    """Create and manage containerized extractors and their image lifecycle"""


@container_cmd.group(name="image")
def image_cmd() -> None:
    """Inspect and manage container images in Nominal's registry"""


# --------------------------------------------------------------------------------------
# nom container extractor ...
# --------------------------------------------------------------------------------------


@extractor_cmd.command()
@click.option("-n", "--name", required=True)
@click.option("-d", "--description", help="description of the extractor")
@client_options
@global_options
def create(name: str, description: str | None, client: NominalClient) -> None:
    """Create a containerized extractor.

    A newly created extractor has no container image: register one with `register-image` and
    activate it with `set-active-image` before ingesting.
    """
    extractor = client.create_containerized_extractor(name, description=description)
    click.echo(extractor)


@extractor_cmd.command("get")
@click.option("-r", "--rid", required=True)
@client_options
@global_options
def get_extractor(rid: str, client: NominalClient) -> None:
    """Get a containerized extractor by its RID"""
    extractor = client.get_containerized_extractor(rid)
    click.echo(extractor)


@extractor_cmd.command("search")
@click.option("--include-archived", is_flag=True, help="include archived extractors in the results")
@click.option(
    "--file-extension",
    help='only include extractors whose active image accepts files with this suffix (e.g. "csv" -- no leading dot)',
)
@click.option("--workspace", "workspace_rid", help="workspace RID to search  [default: the profile's workspace]")
@click.option(
    "-o",
    "--output",
    type=click.Path(dir_okay=False, resolve_path=True, path_type=pathlib.Path),
    help="If provided, a path to write the output to as a file",
)
@click.option(
    "-f",
    "--format",
    type=click.Choice(["csv", "table"], case_sensitive=True),
    default="table",
    show_default=True,
    help="Output data format to represent the data as",
)
@client_options
@global_options
def search_extractors(
    include_archived: bool,
    file_extension: str | None,
    workspace_rid: str | None,
    output: pathlib.Path | None,
    format: str,
    client: NominalClient,
) -> None:
    """Search for containerized extractors (filters are ANDed together)"""
    extractors = client.search_containerized_extractors(
        include_archived=include_archived,
        file_extension=file_extension,
        workspace=workspace_rid,
    )
    emit_table(_extractors_to_string(extractors, format), output, "containerized extractor(s)")


@extractor_cmd.command()
@click.option("-r", "--rid", required=True)
@click.option("-n", "--name", help="replace the extractor's name")
@click.option("-d", "--description", help="replace the extractor's description")
@client_options
@global_options
def update(rid: str, name: str | None, description: str | None, client: NominalClient) -> None:
    """Update mutable fields of a containerized extractor (only the flags you pass are sent)"""
    extractor = client.get_containerized_extractor(rid)
    extractor.update(name=name, description=description)
    click.echo(extractor)


@extractor_cmd.command()
@click.option("-r", "--rid", required=True)
@client_options
@global_options
def archive(rid: str, client: NominalClient) -> None:
    """Archive a containerized extractor"""
    client.get_containerized_extractor(rid).archive()
    click.secho(f"Archived containerized extractor {rid}", fg="green")


@extractor_cmd.command()
@click.option("-r", "--rid", required=True)
@client_options
@global_options
def unarchive(rid: str, client: NominalClient) -> None:
    """Unarchive a containerized extractor"""
    client.get_containerized_extractor(rid).unarchive()
    click.secho(f"Unarchived containerized extractor {rid}", fg="green")


_TIMESTAMP_TYPE_CHOICES = get_args(ts._LiteralAbsolute)
_OUTPUT_FORMAT_CHOICES = tuple(sorted(fmt.name.lower() for fmt in REGISTERABLE_OUTPUT_FORMATS))


@extractor_cmd.command("register-image")
@click.option("-r", "--rid", required=True, help="RID of the extractor to register the image against")
@click.option(
    "-f",
    "--file",
    "tarball",
    type=click.Path(exists=True, dir_okay=False, readable=True, resolve_path=True, path_type=pathlib.Path),
    required=True,
    help="path to the uncompressed `docker save` tarball to upload",
)
@click.option("-t", "--tag", help="tag to register the image under, typically a git short SHA")
@click.option(
    "-c",
    "--config",
    "config_file",
    type=click.Path(exists=True, dir_okay=False, resolve_path=True, path_type=pathlib.Path),
    help=(
        "path to a JSON file describing the image's execution contract: `inputs` and `parameters` "
        "(lists of objects with snake_case FileExtractionInput / FileExtractionParameter fields), "
        "and optionally `tag`, `default_timestamp_column`, `default_timestamp_type`, and "
        "`output_format`. Typically a per-package config checked into the repo. Values supplied "
        "via flags override fields in the JSON."
    ),
)
@click.option("--timestamp-column", help="the column containing timestamp data in the extractor's output files")
@click.option(
    "--timestamp-type",
    type=click.Choice(_TIMESTAMP_TYPE_CHOICES, case_sensitive=False),
    help="interpretation of the timestamp column in the extractor's output files",
)
@click.option(
    "--output-format",
    type=click.Choice(_OUTPUT_FORMAT_CHOICES, case_sensitive=False),
    help="file format the extractor writes  [default: parquet]",
)
@click.option("--wait/--no-wait", default=True, show_default=True, help="wait until the image is READY")
@click.option(
    "--activate",
    is_flag=True,
    help="activate the image on the extractor after registering (polls it to readiness first)",
)
@client_options
@global_options
def register_image(
    rid: str,
    tarball: pathlib.Path,
    tag: str | None,
    config_file: pathlib.Path | None,
    timestamp_column: str | None,
    timestamp_type: str | None,
    output_format: str | None,
    wait: bool,
    activate: bool,
    client: NominalClient,
) -> None:
    r"""Upload a `docker save` tarball and register it as a container image for an extractor.

    Prints the resulting container image RID on stdout (status messages go to stderr), suitable
    for capturing in CI. Registering does not change which image the extractor runs: activate the
    image with `set-active-image` (release-gated pipelines), or pass --activate to register and
    deploy in one step (continuous-deploy pipelines):

        nom container extractor register-image -r "$EXTRACTOR_RID" \
            -f image.tar -t $(git rev-parse --short HEAD) -c extractor-config.json --activate
    """
    parsed = _parse_config(_load_config(config_file))
    tag = tag if tag is not None else parsed.tag
    if tag is None:
        raise click.BadParameter("a tag is required: pass --tag or set `tag` in the config JSON.")
    timestamp_column = timestamp_column if timestamp_column is not None else parsed.timestamp_column
    timestamp_type = timestamp_type if timestamp_type is not None else parsed.timestamp_type
    if timestamp_column is None or timestamp_type is None:
        raise click.BadParameter(
            "default timestamp metadata is required: pass --timestamp-column and --timestamp-type, "
            "or set `default_timestamp_column` and `default_timestamp_type` in the config JSON."
        )
    format_enum = _parse_output_format(output_format) if output_format is not None else parsed.output_format

    extractor = client.get_containerized_extractor(rid)
    click.secho(f"Uploading {tarball.name} and registering it against {extractor.name}...", fg="cyan", err=True)
    image = extractor.register_image(
        tarball,
        tag=tag,
        inputs=parsed.inputs,
        parameters=parsed.parameters,
        default_timestamp_column=timestamp_column,
        default_timestamp_type=cast(ts._AnyTimestampType, timestamp_type),
        output_format=format_enum if format_enum is not None else FileOutputFormat.PARQUET,
        activate=activate,
    )
    if activate:
        click.secho(f"Activated image {image.rid} ({image.tag}) on {extractor.name}", fg="green", err=True)
    elif wait:
        click.secho(f"Waiting for image {image.rid} ({image.tag}) to become READY...", fg="cyan", err=True)
        image.poll_until_ready()
    click.echo(image.rid)


@extractor_cmd.command("validate-config")
@click.argument(
    "config_path",
    type=click.Path(exists=True, dir_okay=False, readable=True, resolve_path=True, path_type=pathlib.Path),
)
@global_options
def validate_config(config_path: pathlib.Path) -> None:
    """Validate a register-image config JSON without contacting Nominal.

    Runs the exact validation `register-image` performs before uploading, so it can lint a
    checked-in config in CI without credentials. Values that must be supplied via flags at
    register time (e.g. a missing `tag`) are reported as notes, not errors.
    """
    parsed = _parse_config(_load_config(config_path))
    click.secho(f"{config_path.name} is a valid register-image config", fg="green")
    click.echo(f"  inputs: {len(parsed.inputs)}, parameters: {len(parsed.parameters)}")
    if parsed.tag is None:
        click.echo("  note: no `tag` -- pass --tag at register time")
    if parsed.timestamp_column is None or parsed.timestamp_type is None:
        click.echo(
            "  note: incomplete default timestamp metadata -- pass --timestamp-column and "
            "--timestamp-type at register time"
        )
    if parsed.output_format is None:
        click.echo("  note: no `output_format` -- parquet will be used unless --output-format is passed")


@extractor_cmd.command("set-active-image")
@click.option("-r", "--rid", required=True, help="RID of the extractor")
@click.option("-i", "--image-rid", required=True, help="RID of the registered image to activate")
@click.option(
    "--wait/--no-wait",
    default=True,
    show_default=True,
    help="wait until the image is READY before activating (with --no-wait, a non-READY image errors)",
)
@client_options
@global_options
def set_active_image(rid: str, image_rid: str, wait: bool, client: NominalClient) -> None:
    """Select the container image an extractor runs when ingesting"""
    extractor = client.get_containerized_extractor(rid)
    extractor.set_active_image(image_rid, poll_until_ready=wait)
    click.echo(extractor)


# --------------------------------------------------------------------------------------
# nom container image ...
# --------------------------------------------------------------------------------------


@image_cmd.command("get")
@click.option("-r", "--rid", required=True)
@client_options
@global_options
def get_image(rid: str, client: NominalClient) -> None:
    """Get a container image by its RID"""
    image = client.get_container_image(rid)
    click.echo(image)


_STATUS_CHOICES = tuple(s.name.lower() for s in ContainerImageStatus if s is not ContainerImageStatus.UNSPECIFIED)


@image_cmd.command("search")
@click.option("-t", "--tag", help="filter to images with this exact tag")
@click.option(
    "--status",
    type=click.Choice(_STATUS_CHOICES, case_sensitive=False),
    help="filter to images with this lifecycle status",
)
@click.option("-e", "--extractor", "extractor_rid", help="filter to images registered against this extractor RID")
@click.option("--workspace", "workspace_rid", help="workspace RID to search  [default: the profile's workspace]")
@click.option(
    "-o",
    "--output",
    type=click.Path(dir_okay=False, resolve_path=True, path_type=pathlib.Path),
    help="If provided, a path to write the output to as a file",
)
@click.option(
    "-f",
    "--format",
    type=click.Choice(["csv", "table"], case_sensitive=True),
    default="table",
    show_default=True,
    help="Output data format to represent the data as",
)
@client_options
@global_options
def search_images(
    tag: str | None,
    status: str | None,
    extractor_rid: str | None,
    workspace_rid: str | None,
    output: pathlib.Path | None,
    format: str,
    client: NominalClient,
) -> None:
    """Search for container images, filtering by tag, status, and/or extractor (filters are ANDed together)"""
    status_enum = ContainerImageStatus[status.upper()] if status is not None else None
    images = client.search_container_images(
        tag=tag, status=status_enum, extractor=extractor_rid, workspace=workspace_rid
    )
    emit_table(_images_to_string(images, format), output, "container image(s)")


@image_cmd.command()
@click.option("-r", "--rid", required=True)
@click.option("--yes", is_flag=True, help="skip the confirmation prompt")
@client_options
@global_options
def delete(rid: str, yes: bool, client: NominalClient) -> None:
    """Delete a container image (fails if an extractor still has it as its active image)"""
    if not yes:
        click.confirm(f"Delete container image {rid}?", abort=True)
    client.get_container_image(rid).delete()
    click.secho(f"Deleted container image {rid}", fg="green")


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------


class _RegisterImageConfig(NamedTuple):
    """A parsed and validated register-image config JSON; None fields must come from CLI flags."""

    inputs: list[FileExtractionInput]
    parameters: list[FileExtractionParameter]
    tag: str | None
    timestamp_column: str | None
    timestamp_type: str | None
    output_format: FileOutputFormat | None


_KNOWN_CONFIG_KEYS = frozenset(
    {"inputs", "parameters", "tag", "default_timestamp_column", "default_timestamp_type", "output_format"}
)


def _load_config(config_file: pathlib.Path | None) -> dict[str, Any]:
    """Load the register-image config JSON, defaulting to empty when no file is given."""
    if config_file is None:
        return {}
    try:
        raw = config_file.read_text()
    except (OSError, UnicodeDecodeError) as ex:
        raise click.BadParameter(f"failed to read config: {ex}") from ex
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as ex:
        raise click.BadParameter(f"invalid config JSON: {ex.msg}") from ex
    if not isinstance(payload, dict):
        raise click.BadParameter("top-level config JSON must be an object")
    return payload


def _parse_config(config: dict[str, Any]) -> _RegisterImageConfig:
    """Validate a register-image config JSON and parse it into SDK types.

    This is the shared pre-upload validation behind both `register-image` and `validate-config`:
    unknown keys (typos) are rejected rather than silently ignored, contract entries must parse
    into their SDK dataclasses with non-empty names and unique environment variables, and
    timestamp type / output format values must be known.
    """
    unknown_keys = set(config) - _KNOWN_CONFIG_KEYS
    if unknown_keys:
        raise click.BadParameter(
            f"unknown config keys {sorted(unknown_keys)}; expected a subset of {sorted(_KNOWN_CONFIG_KEYS)}"
        )
    inputs = _parse_contract_items(config.get("inputs", []), FileExtractionInput, "inputs")
    parameters = _parse_contract_items(config.get("parameters", []), FileExtractionParameter, "parameters")

    contract_items: list[FileExtractionInput | FileExtractionParameter] = [*inputs, *parameters]
    env_var_counts = collections.Counter(item.environment_variable for item in contract_items)
    duplicates = sorted(env_var for env_var, count in env_var_counts.items() if count > 1)
    if duplicates:
        raise click.BadParameter(
            f"duplicate environment variables across inputs and parameters: {duplicates}; "
            "each input and parameter must use a distinct environment variable."
        )

    timestamp_type = _non_empty_string_or_none(config, "default_timestamp_type")
    if timestamp_type is not None:
        # Match the --timestamp-type flag's case-insensitive click.Choice.
        timestamp_type = timestamp_type.lower()
    if timestamp_type is not None and timestamp_type not in _TIMESTAMP_TYPE_CHOICES:
        raise click.BadParameter(
            f"unknown `default_timestamp_type` {timestamp_type!r}; expected one of {_TIMESTAMP_TYPE_CHOICES}."
        )
    output_format = _non_empty_string_or_none(config, "output_format")
    return _RegisterImageConfig(
        inputs=inputs,
        parameters=parameters,
        tag=_non_empty_string_or_none(config, "tag"),
        timestamp_column=_non_empty_string_or_none(config, "default_timestamp_column"),
        timestamp_type=timestamp_type,
        output_format=None if output_format is None else _parse_output_format(output_format),
    )


def _non_empty_string_or_none(config: Mapping[str, Any], key: str) -> str | None:
    """Read an optional config value that must be a non-empty string when present."""
    value = config.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise click.BadParameter(f"`{key}` in the config JSON must be a non-empty string, got {value!r}")
    return value


class _ContractItem(Protocol):
    @property
    def name(self) -> str: ...
    @property
    def environment_variable(self) -> str: ...


_ContractItemT = TypeVar("_ContractItemT", bound=_ContractItem)


def _parse_contract_items(raw: Any, item_type: type[_ContractItemT], field_name: str) -> list[_ContractItemT]:
    """Build FileExtractionInput / FileExtractionParameter entries from config JSON objects."""
    if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
        raise click.BadParameter(f"`{field_name}` in the config JSON must be a list of objects")
    items = []
    for item in raw:
        try:
            parsed = item_type(**item)
        except TypeError as ex:
            raise click.BadParameter(f"invalid `{field_name}` entry {item!r}: {ex}") from ex
        if not parsed.name or not parsed.environment_variable:
            raise click.BadParameter(
                f"invalid `{field_name}` entry {item!r}: `name` and `environment_variable` must be non-empty."
            )
        items.append(parsed)
    return items


def _parse_output_format(value: str) -> FileOutputFormat:
    try:
        output_format = FileOutputFormat[value.upper()]
    except KeyError:
        raise click.BadParameter(f"unknown `output_format` {value!r}; expected one of {_OUTPUT_FORMAT_CHOICES}.")
    if output_format not in REGISTERABLE_OUTPUT_FORMATS:
        raise click.BadParameter(f"`output_format` {value!r} is not registerable; use one of {_OUTPUT_FORMAT_CHOICES}.")
    return output_format


def _extractors_to_string(extractors: Sequence[ContainerizedExtractor], format: str) -> str:
    data = collections.defaultdict(list)
    for extractor in extractors:
        image = extractor.active_image
        data["rid"].append(extractor.rid)
        data["name"].append(extractor.name)
        data["description"].append(extractor.description or "")
        data["archived"].append("yes" if extractor.is_archived else "no")
        data["active image"].append("" if image is None else f"{image.tag} ({image.status.value})")
        data["created"].append(_SecondsNanos.from_nanoseconds(extractor.created_at).to_iso8601())
    return table_data_to_string(data, format)


def _images_to_string(images: Sequence[ContainerImage], format: str) -> str:
    data = collections.defaultdict(list)
    for image in images:
        data["rid"].append(image.rid)
        data["tag"].append(image.tag)
        data["status"].append(image.status.value)
        data["extractor rid"].append(image.extractor_rid)
        data["size (bytes)"].append(str(image.size_bytes))
        data["created"].append(_SecondsNanos.from_nanoseconds(image.created_at).to_iso8601())
    return table_data_to_string(data, format)
