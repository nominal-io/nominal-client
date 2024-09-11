from pathlib import Path
from typing import Sequence
import click

from nominal import _config
from .._utils import FileType
from nominal.sdk import NominalClient
from nominal.nominal import _upload_csv

BASE_URL_OPTION = click.option("--base-url", default="https://api.gov.nominal.io/api")
TOKEN_OPTION = click.option("--token", help="[default: looked up in ~/.nominal.yml]")


@click.group(context_settings={"show_default": True})
def nom():
    pass


@nom.group()
def auth():
    pass


@auth.command()
@click.option("-u", "--base-url", default="https://api.gov.nominal.io/api", prompt=True)
@click.option("-t", "--token", required=True, prompt=True)
def set_token(token: str, base_url: str) -> None:
    """Update the token for a given URL in the Nominal config file."""
    path = _config._DEFAULT_NOMINAL_CONFIG_PATH
    _config.set_token(base_url, token)
    print("Successfully set token for", base_url, "in", path)


@nom.group()
def run():
    pass


@run.command()
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
    """Create a new run."""
    client = _get_client(base_url, token)
    run = client.create_run(name, start, end, desc, properties=dict(properties), labels=labels)
    print(run)


@run.command()
@click.option("-r", "--rid", required=True)
@BASE_URL_OPTION
@TOKEN_OPTION
def get(
    rid: str,
    base_url: str,
    token: str | None,
) -> None:
    """Get a run by its RID."""
    client = _get_client(base_url, token)
    run = client.get_run(rid)
    print(run)


@nom.group()
def dataset():
    pass


@dataset.command()
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
    timestamp_type: str,
    desc: str | None,
    wait: bool,
    base_url: str,
    token: str | None,
) -> None:
    client = _get_client(base_url, token)
    dataset = _upload_csv(client, file, name, timestamp_column, timestamp_type, desc, wait_until_complete=wait)
    print(dataset)


@dataset.command()
@click.option("-r", "--rid", required=True)
@BASE_URL_OPTION
@TOKEN_OPTION
def get(rid: str, base_url: str, token: str | None) -> None:
    client = _get_client(base_url, token)
    dataset = client.get_dataset(rid)
    print(dataset)


@nom.group(name="attachment")
def attachment_():
    pass


@attachment_.command()
@click.option("-r", "--rid", required=True)
@BASE_URL_OPTION
@TOKEN_OPTION
def get(rid: str, base_url: str, token: str | None) -> None:
    client = _get_client(base_url, token)
    attachment = client.get_attachment(rid)
    print(attachment)


@attachment_.command()
@click.option("-n", "--name", required=True)
@click.option("-f", "--file", required=True)
@click.option("-d", "--desc")
@BASE_URL_OPTION
@TOKEN_OPTION
def upload(
    name: str,
    file: str,
    desc: str | None,
    base_url: str,
    token: str | None,
) -> None:
    client = _get_client(base_url, token)
    path = Path(file)
    file_type = FileType.from_path(path)
    with open(path, "rb") as f:
        attachment = client.create_attachment_from_io(f, name, file_type, desc)
    print(attachment)


@attachment_.command()
@click.option("-r", "--rid", required=True)
@click.option("-o", "--output", required=True)
@BASE_URL_OPTION
@TOKEN_OPTION
def download(
    rid: str,
    output: str,
    base_url: str,
    token: str | None,
) -> None:
    client = _get_client(base_url, token)
    attachment = client.get_attachment(rid)
    attachment.write(Path(output))


def _get_client(base_url: str, token: str | None) -> NominalClient:
    if token is None:
        token = _config.get_token(base_url)
    return NominalClient.create(base_url, token)
