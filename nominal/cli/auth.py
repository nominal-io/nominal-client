from __future__ import annotations

import click

from nominal import _config as _deprecated_config
from nominal.cli.util.global_decorators import global_options
from nominal.cli.util.verify_connection import validate_token_url


@click.group(name="auth")
def auth_cmd() -> None:
    pass


@auth_cmd.command(help="deprecated, use `nom config profile add` instead")
@click.option("-u", "--base-url", default="https://api.gov.nominal.io/api", prompt=True)
@click.option(
    "-t", "--token", required=True, prompt=True, help="access token, can be found in /sandbox on your Nominal instance"
)
@global_options
def set_token(token: str, base_url: str) -> None:
    """Update the token for a given URL in the Nominal config file"""
    path = _deprecated_config._DEFAULT_NOMINAL_CONFIG_PATH
    validate_token_url(token, base_url)
    _deprecated_config.set_token(base_url, token)
    click.secho(f"Successfully set token for '{base_url}' in {path}", fg="green")
