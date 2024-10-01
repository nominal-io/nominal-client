from __future__ import annotations

from typing import Sequence

import click

from ..ts import _SecondsNanos
from ._utils import BASE_URL_OPTION, TOKEN_OPTION, get_client


@click.group(name="run")
def run_cmd() -> None:
    pass


@run_cmd.command()
@click.option("-n", "--name", required=True)
@click.option("-s", "--start", required=True)
@click.option("-e", "--end", required=True)
@click.option("-d", "--desc")
@click.option("properties", "--property", type=(str, str), multiple=True)
@click.option("labels", "--label", type=str, multiple=True)
@BASE_URL_OPTION
@TOKEN_OPTION
def create(
    name: str,
    start: str,
    end: str,
    desc: str | None,
    properties: Sequence[tuple[str, str]],
    labels: Sequence[str],
    base_url: str,
    token: str | None,
) -> None:
    """create a new run"""
    client = get_client(base_url, token)
    run = client.create_run(
        name,
        _SecondsNanos.from_flexible(start).to_nanoseconds(),
        _SecondsNanos.from_flexible(end).to_nanoseconds(),
        desc,
        properties=dict(properties),
        labels=labels,
    )
    print(run)


@run_cmd.command()
@click.option("-r", "--rid", required=True)
@BASE_URL_OPTION
@TOKEN_OPTION
def get(
    rid: str,
    base_url: str,
    token: str | None,
) -> None:
    """get a run by its RID"""
    client = get_client(base_url, token)
    run = client.get_run(rid)
    print(run)
