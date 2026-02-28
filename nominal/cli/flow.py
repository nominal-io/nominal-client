from __future__ import annotations

import click

from nominal.cli.util.global_decorators import client_options, global_options
from nominal.core.client import NominalClient


@click.group(name="flow")
def flow_cmd() -> None:
    pass


@flow_cmd.command("run")
@click.option("-r", "--rid", required=True, help="RID of the ingest flow to run interactively")
@client_options
@global_options
def run_flow(rid: str, client: NominalClient) -> None:
    """Interactively walk through an ingest flow.

    Prompts for selections and form values at each step,
    then uploads a data file at the end.
    """
    from nominal.core.flow import run_interactive_flow

    result = run_interactive_flow(rid, client)
    if result.run:
        click.echo(f"\n{result.run.nominal_url}")
    else:
        click.echo(f"\n{client.get_dataset(result.dataset_file.dataset_rid).nominal_url}")
