from __future__ import annotations

import collections
import logging
import pathlib
from typing import Sequence

import click
import tabulate

from ..core.client import NominalClient
from ..ts import _LiteralAbsolute
from .util import client_options, global_options

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
@click.option("-d", "--desc")
@click.option("--wait/--no-wait", default=True, help="wait until the upload is complete")
@client_options
@global_options
def upload_csv(
    name: str,
    file: str,
    timestamp_column: str,
    timestamp_type: _LiteralAbsolute,
    desc: str | None,
    wait: bool,
    client: NominalClient,
) -> None:
    """Upload a local CSV file to Nominal, create and ingest the data into a dataset, and print the details of the newly created dataset to the user."""
    dataset = client.create_csv_dataset(
        file,
        name,
        timestamp_column=timestamp_column,
        timestamp_type=timestamp_type,
        description=desc,
        poll_until_completed=wait,
    )
    click.echo(dataset)


@dataset_cmd.command("get")
@click.option("-r", "--rid", required=True)
@client_options
@global_options
def get(rid: str, client: NominalClient) -> None:
    """fetch a dataset by its RID"""
    dataset = client.get_dataset(rid)
    click.echo(dataset)


@dataset_cmd.command("summarize")
@click.option("-r", "--rid", required=True, multiple=True, help="RID(s) of the dataset(s) to summarize")
@click.option(
    "-c",
    "--csv",
    type=click.Path(exists=False, dir_okay=False, resolve_path=True, path_type=pathlib.Path),
    help="If provided, a path to write the description to as a CSV",
)
@click.option("--show-rids", is_flag=True, help="If provided, show channel / dataset RIDs as part of tabulated output")
@client_options
@global_options
def summarize(rid: Sequence[str], csv: pathlib.Path | None, show_rids: bool, client: NominalClient) -> None:
    """Summarize the dataset(s) by their schema (column names, types, and RIDs)"""
    datasets = {dataset.name: dataset for dataset in client.get_datasets(rid)}
    data = collections.defaultdict(list)
    for dataset_name, dataset in datasets.items():
        dataset_metadata = dataset.get_channels()
        for metadata in dataset_metadata:
            data["channel name"].append(metadata.name)
            data["channel unit"].append(metadata.unit if metadata.unit else "")
            data["dataset_name"].append(dataset_name)

            if show_rids:
                data["channel rid"].append(metadata.rid)
                data["dataset rid"].append(dataset.rid)

    if csv is None:
        click.echo(tabulate.tabulate(data, headers=list(data.keys()), tablefmt="pretty"))
    else:
        # Performing import within method to prevent users from having a long load-up time for other
        # endpoints while importing pandas
        import pandas as pd

        click.secho(f"Writing dataset(s) metadata to {csv}", fg="cyan")
        csv.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(data)
        df.to_csv(csv, index=False)
