from __future__ import annotations

import click

from nominal.cli.util.global_decorators import global_options
from nominal.core.exceptions import NominalMethodRemovedError


@click.group(name="auth")
def auth_cmd() -> None:
    pass


@auth_cmd.command()
@click.option("-u", "--base-url", default="https://api.gov.nominal.io/api", prompt=True)
@click.option(
    "-t", "--token", required=True, prompt=True, help="access token, can be found in /sandbox on your Nominal instance"
)
@global_options
def set_token(token: str, base_url: str) -> None:
    """deprecated: use `nom config profile add` instead"

    Update the token for a given URL in the (deprecated) Nominal config file
    """
    raise NominalMethodRemovedError("nominal auth set-token", "use 'nominal config profile add' instead")
