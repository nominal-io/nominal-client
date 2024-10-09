from __future__ import annotations

import collections
import pathlib
from typing import Mapping, Sequence

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
@BASE_URL_OPTION
@TOKEN_OPTION
def summarize(
    base_url: str, token: str | None, rid: Sequence[str], show_rids: bool, output: pathlib.Path | None, format: str
) -> None:
    """Summarize the dataset(s) by their schema (column names, types, and RIDs)"""

    client = get_client(base_url, token)

    data = collections.defaultdict(list)
    for dataset in client.get_datasets(rid):
        dataset_metadata = dataset.get_channels()
        for metadata in dataset_metadata:
            data["channel name"].append(metadata.name)
            data["channel unit"].append(metadata.unit if metadata.unit else "")
            data["dataset_name"].append(dataset.name)

            if show_rids:
                data["channel rid"].append(metadata.rid)
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
