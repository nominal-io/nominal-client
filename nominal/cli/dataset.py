from __future__ import annotations

import collections
import logging
import pathlib
from typing import Mapping, Sequence

import click
import tabulate

from nominal.cli.util.global_decorators import client_options, global_options
from nominal.core.client import NominalClient
from nominal.ts import _LiteralAbsolute

logger = logging.getLogger(__name__)


@click.group(name="dataset")
def dataset_cmd() -> None:
    pass


@dataset_cmd.command()
@click.option("-n", "--name", required=True)
@click.option("-f", "--file", required=True, help="path to the CSV file to upload")
@click.option("-t", "--timestamp-column", required=True, help="the primary timestamp column name")
@click.option(
    "-T",
    "--timestamp-type",
    required=True,
    type=click.Choice(
        [
            "iso_8601",
            "epoch_days",
            "epoch_hours",
            "epoch_minutes",
            "epoch_seconds",
            "epoch_milliseconds",
            "epoch_microseconds",
            "epoch_nanoseconds",
        ]
    ),
    help="interpretation the primary timestamp column",
)
@click.option("-d", "--description", help="description of the dataset")
@click.option(
    "--channel-name-delimiter",
    default=".",
    show_default=True,
    help="the character used to delimit the hierarchy in the channel name",
)
@click.option("--wait/--no-wait", default=True, help="wait until the upload is complete")
@client_options
@global_options
def upload_csv(
    name: str,
    file: str,
    timestamp_column: str,
    timestamp_type: _LiteralAbsolute,
    description: str | None,
    channel_name_delimiter: str | None,
    wait: bool,
    client: NominalClient,
) -> None:
    """Upload a local CSV file to Nominal, create and ingest the data into a dataset, and print the details of
    the newly created dataset to the user.
    """
    dataset = client.create_dataset(
        name=name,
        description=description,
        prefix_tree_delimiter=channel_name_delimiter,
    )
    dataset.add_tabular_data(
        file,
        timestamp_column=timestamp_column,
        timestamp_type=timestamp_type,
    )

    # block until ingestion completed, if requested
    if wait:
        dataset.poll_until_ingestion_completed()

    click.echo(dataset)


@dataset_cmd.command("get")
@click.option("-r", "--rid", required=True)
@client_options
@global_options
def get(rid: str, client: NominalClient) -> None:
    """Fetch a dataset by its RID"""
    dataset = client.get_dataset(rid)
    click.echo(dataset)


@dataset_cmd.command("summarize")
@click.option("-r", "--rid", required=True, multiple=True, help="RID(s) of the dataset(s) to summarize")
@click.option(
    "--show-rids/--no-show-rids",
    default=False,
    show_default=True,
    help="If provided, show channel / dataset RIDs as part of tabulated output",
)
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
def summarize(
    client: NominalClient, rid: Sequence[str], show_rids: bool, output: pathlib.Path | None, format: str
) -> None:
    """Summarize the dataset(s) by their schema (column names, types, and RIDs)"""
    data = collections.defaultdict(list)
    for dataset in client.get_datasets(rid):
        dataset_metadata = dataset.get_channels()
        for metadata in dataset_metadata:
            data["channel name"].append(metadata.name)
            data["channel unit"].append(metadata.unit if metadata.unit else "")
            data["dataset_name"].append(dataset.name)

            if show_rids:
                data["dataset rid"].append(dataset.rid)

    output_str = _dataset_data_to_string(data, format)

    if output is None:
        click.echo(output_str)
    else:
        click.secho(f"Writing dataset(s) metadata to {output}", fg="cyan")
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(output_str)


def _dataset_data_to_string(table_data: Mapping[str, list[str]], format: str) -> str:
    import pandas as pd

    if format == "csv":
        return pd.DataFrame(table_data).to_csv(index=False)
    elif format == "table":
        return tabulate.tabulate(table_data, headers=list(table_data.keys()))
    else:
        raise ValueError(f"Expected format to be one of csv or table, received {format}")
