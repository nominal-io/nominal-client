from __future__ import annotations

import collections
import pathlib
from typing import Literal, Sequence

import click
import tabulate

from ..nominal import _upload_csv
from ..ts import _LiteralAbsolute
from ._utils import BASE_URL_OPTION, TOKEN_OPTION, get_client


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
@BASE_URL_OPTION
@TOKEN_OPTION
def upload_csv(
    name: str,
    file: str,
    timestamp_column: str,
    timestamp_type: _LiteralAbsolute,
    desc: str | None,
    wait: bool,
    base_url: str,
    token: str | None,
) -> None:
    client = get_client(base_url, token)
    dataset = _upload_csv(client, file, name, timestamp_column, timestamp_type, desc, wait_until_complete=wait)
    print(dataset)


@dataset_cmd.command("get")
@click.option("-r", "--rid", required=True)
@BASE_URL_OPTION
@TOKEN_OPTION
def get(rid: str, base_url: str, token: str | None) -> None:
    """fetch a dataset by its RID"""
    client = get_client(base_url, token)
    dataset = client.get_dataset(rid)
    print(dataset)


@dataset_cmd.command("summarize")
@click.option("-r", "--rid", required=True, multiple=True, help="RID(s) of the dataset(s) to summarize")
@click.option(
    "-c",
    "--csv",
    type=click.Path(exists=False, dir_okay=False, resolve_path=True, path_type=pathlib.Path),
    help="If provided, a path to write the description to as a CSV",
)
@click.option("--show-rids", is_flag=True, help="If provided, show channel / dataset RIDs as part of tabulated output")
@BASE_URL_OPTION
@TOKEN_OPTION
def summarize(rid: Sequence[str], csv: pathlib.Path | None, show_rids: bool, base_url: str, token: str | None) -> None:
    """Summarize the dataset(s) by their schema (column names, types, and RIDs)"""

    client = get_client(base_url, token)
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
