import click
import click.core

from nominal import _config


@click.group()
def cli() -> None:
    pass


@cli.group()
def auth() -> None:
    pass


@auth.command()
@click.option("-u", "--base-url", default="https://api.gov.nominal.io/api", prompt=True)
@click.option("-t", "--token", required=True, prompt=True)
def set_token(token: str, base_url: str) -> None:
    """Update the token for a given URL in the Nominal config file."""
    path = _config._DEFAULT_NOMINAL_CONFIG_PATH
    _config.set_token(base_url, token)
    print("Successfully set token for", base_url, "in", path)


if __name__ == "__main__":
    cli()
