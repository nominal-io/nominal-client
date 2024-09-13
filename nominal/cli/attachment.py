from __future__ import annotations

from pathlib import Path

import click

from .._utils import FileType
from ._utils import BASE_URL_OPTION, TOKEN_OPTION, get_client


@click.group(name="attachment")
def attachment_cmd() -> None:
    pass


@attachment_cmd.command()
@click.option("-r", "--rid", required=True)
@BASE_URL_OPTION
@TOKEN_OPTION
def get(rid: str, base_url: str, token: str | None) -> None:
    """get an attachment by its RID"""
    client = get_client(base_url, token)
    attachment = client.get_attachment(rid)
    print(attachment)


@attachment_cmd.command("upload")
@click.option("-n", "--name", required=True)
@click.option("-f", "--file", required=True, help="path to the file to upload")
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
    client = get_client(base_url, token)
    path = Path(file)
    file_type = FileType.from_path(path)
    with open(path, "rb") as f:
        attachment = client.create_attachment_from_io(f, name, file_type, desc)
    print(attachment)


@attachment_cmd.command()
@click.option("-r", "--rid", required=True)
@click.option("-o", "--output", required=True, help="full path to write the attachment to (not just the directory)")
@BASE_URL_OPTION
@TOKEN_OPTION
def download(
    rid: str,
    output: str,
    base_url: str,
    token: str | None,
) -> None:
    client = get_client(base_url, token)
    attachment = client.get_attachment(rid)
    attachment.write(Path(output))
