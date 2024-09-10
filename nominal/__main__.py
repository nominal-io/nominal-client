import click
import click.core

from ._config import _DEFAULT_NOMINAL_CONFIG_PATH, NominalConfig


def validate_base_url(ctx: click.core.Context, param: str, value: str) -> str:
    if value.startswith("http"):
        raise click.BadParameter(f"base url {value!r} must not include the http:// or https:// scheme")
    return value


@click.group()
def cli() -> None:
    pass


@cli.group()
def auth() -> None:
    pass


@auth.command()
@click.option("-t", "--token", required=True, prompt=True)
@click.option("-u", "--base-url", default="api.gov.nominal.io/api", prompt=True, callback=validate_base_url)
def set_token(token: str, base_url: str) -> None:
    """Update the token for a given URL in the Nominal config file."""
    path = _DEFAULT_NOMINAL_CONFIG_PATH
    cfg = NominalConfig.from_yaml()
    cfg.set_token(base_url, token, save=True)
    print("Successfully set token for", base_url, "in", path)


if __name__ == "__main__":
    cli()
