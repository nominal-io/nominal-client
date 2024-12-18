from __future__ import annotations

from pathlib import Path

import click

from nominal.cli.util.global_decorators import client_options, global_options
from nominal.core.client import NominalClient
from nominal.core.filetype import FileType


@click.group(name="attachment")
def attachment_cmd() -> None:
    pass


@attachment_cmd.command()
@click.option("-r", "--rid", required=True)
@client_options
@global_options
def get(rid: str, client: NominalClient) -> None:
    """Get an attachment by its RID"""
    attachment = client.get_attachment(rid)
    click.echo(attachment)


@attachment_cmd.command("upload")
@click.option("-n", "--name", required=True)
@click.option("-f", "--file", required=True, help="path to the file to upload")
@click.option("-d", "--description", help="description of the attachment")
@client_options
@global_options
def upload(name: str, file: str, description: str | None, client: NominalClient) -> None:
    """Upload attachment from a local file with a given name and description and display the details of the newly
    created attachment to the user.
    """
    path = Path(file)
    file_type = FileType.from_path(path)
    with open(path, "rb") as f:
        attachment = client.create_attachment_from_io(f, name, file_type, description)
    click.echo(attachment)


@attachment_cmd.command()
@click.option("-r", "--rid", required=True)
@click.option("-o", "--output", required=True, help="full path to write the attachment to (not just the directory)")
@client_options
@global_options
def download(rid: str, output: str, client: NominalClient) -> None:
    """Download an attachment with the given RID to the specified location on disk."""
    attachment = client.get_attachment(rid)
    attachment.write(Path(output))
