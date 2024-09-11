import click

from .._config import _DEFAULT_NOMINAL_CONFIG_PATH, set_token


@click.group()
def auth_cmd() -> None:
    pass


@auth_cmd.command()
@click.option("-u", "--base-url", default="https://api.gov.nominal.io/api", prompt=True)
@click.option("-t", "--token", required=True, prompt=True)
def set_token(token: str, base_url: str) -> None:
    """Update the token for a given URL in the Nominal config file."""
    path = _DEFAULT_NOMINAL_CONFIG_PATH
    set_token(base_url, token)
    print("Successfully set token for", base_url, "in", path)
