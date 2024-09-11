from typing import Literal
import click

from ._utils import BASE_URL_OPTION, TOKEN_OPTION, get_client
from ..nominal import _upload_csv


@click.group()
def dataset_cmd() -> None:
    pass


@dataset_cmd.command()
@click.option("-n", "--name", required=True)
@click.option("-f", "--file", required=True)
@click.option("-t", "--timestamp-column", required=True)
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
)
@click.option("-d", "--desc")
@click.option("--wait/--no-wait", default=True, help="Wait until the upload is complete.")
@BASE_URL_OPTION
@TOKEN_OPTION
def upload_csv(
    name: str,
    file: str,
    timestamp_column: str,
    timestamp_type: Literal[
        "iso_8601",
        "epoch_days",
        "epoch_hours",
        "epoch_minutes",
        "epoch_seconds",
        "epoch_milliseconds",
        "epoch_microseconds",
        "epoch_nanoseconds",
    ],
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
    client = get_client(base_url, token)
    dataset = client.get_dataset(rid)
    print(dataset)
