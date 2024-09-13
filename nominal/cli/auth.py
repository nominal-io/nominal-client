from __future__ import annotations

import click

from .. import _config


@click.group(name="auth")
def auth_cmd() -> None:
    pass


@auth_cmd.command()
@click.option("-u", "--base-url", default="https://api.gov.nominal.io/api", prompt=True)
@click.option(
    "-t", "--token", required=True, prompt=True, help="access token, can be found in /sandbox on your Nominal instance"
)
def set_token(token: str, base_url: str) -> None:
    """update the token for a given URL in the Nominal config file"""
    path = _config._DEFAULT_NOMINAL_CONFIG_PATH
    _config.set_token(base_url, token)
    print("Successfully set token for", base_url, "in", path)
