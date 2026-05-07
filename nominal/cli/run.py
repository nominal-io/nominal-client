from __future__ import annotations

from typing import Sequence

import click

from nominal.cli.util.global_decorators import client_options, global_options
from nominal.core.client import NominalClient
from nominal.ts import _SecondsNanos


@click.group(name="run")
def run_cmd() -> None:
    pass


@run_cmd.command()
@click.option("-n", "--name", required=True)
@click.option("-s", "--start", required=True)
@click.option("-e", "--end", required=True)
@click.option("-d", "--description", help="description of the run")
@click.option("properties", "--property", type=(str, str), multiple=True)
@click.option("labels", "--label", type=str, multiple=True)
@client_options
@global_options
def create(
    name: str,
    start: str,
    end: str,
    description: str | None,
    properties: Sequence[tuple[str, str]],
    labels: Sequence[str],
    client: NominalClient,
) -> None:
    """Create a new run"""
    run = client.create_run(
        name,
        _SecondsNanos.from_flexible(start).to_nanoseconds(),
        _SecondsNanos.from_flexible(end).to_nanoseconds(),
        description,
        properties=dict(properties),
        labels=labels,
    )
    click.echo(run)


@run_cmd.command()
@click.option("-r", "--rid", required=True)
@client_options
@global_options
def get(
    rid: str,
    client: NominalClient,
) -> None:
    """Get a run by its RID"""
    run = client.get_run(rid)
    click.echo(run)
