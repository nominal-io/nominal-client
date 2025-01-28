from __future__ import annotations

import click
from conjure_python_client import ConjureHTTPError

from nominal import NominalClient, config, _config as deprecated_config
from nominal.cli.util.global_decorators import global_options


@click.group(name="auth")
def auth_cmd() -> None:
    pass


def _validate_token_url(token: str, base_url: str) -> None:
    """Ensure the user sets a valid configuration before letting them import the client."""
    token_link = "https://app.gov.nominal.io/settings/user?tab=tokens"
    status_code = 200
    err_msg = ""
    try:
        NominalClient.create(base_url, token).get_user()
    except ConjureHTTPError as err:
        status_code = err.response.status_code
    if status_code == 401:
        err_msg = f"Your authorization token seems to be incorrect. Please recreate one here: {token_link}"
    elif status_code == 404:
        err_msg = "Your base_url is not correct. Ensure it points to the API and not the app."
    elif status_code != 200:
        err_msg = (
            f"There is a misconfiguration between your base_url and token. Ensure you use the API url, "
            f"and create a new token: {token_link} {status_code}"
        )
    if err_msg:
        click.secho(err_msg, err=True, fg="red")
        raise click.ClickException("Failed to authenticate. See above for details")


@auth_cmd.command()
@click.option("-u", "--base-url", default="https://api.gov.nominal.io/api", prompt=True)
@click.option(
    "-t", "--token", required=True, prompt=True, help="access token, can be found in /sandbox on your Nominal instance"
)
@global_options
def set_token(token: str, base_url: str) -> None:
    """Update the token for a given URL in the Nominal config file"""
    path = deprecated_config._DEFAULT_NOMINAL_CONFIG_PATH
    _validate_token_url(token, base_url)
    deprecated_config.set_token(base_url, token)
    click.secho(f"Successfully set token for '{base_url}' in {path}", fg="green")


@auth_cmd.command()
def migrate():
    deprecated_cfg = deprecated_config.NominalConfig.from_yaml()
    profiles = {}
    for url, token in deprecated_cfg.environments.items():
        if click.prompt(f"Add profile for {url}?", default="y", type=bool):
            name = click.prompt("profile name")
            new_url = click.prompt("base url", default=f"https://{url}")
            new_token = click.prompt("token", default=token)
            if click.prompt("Validate connection?", default="y", type=bool):
                _validate_token_url(new_token, new_url)
            profiles[name] = config.Profile(new_url, new_token)
    new_cfg = config.NominalConfig(profiles=profiles)
    new_cfg.to_yaml()


@auth_cmd.group()
def profile():
    pass


@profile.command()
@click.option("-p", "--profile", prompt=True, help="profile name")
@click.option("-u", "--base-url", default="https://api.gov.nominal.io/api", prompt=True)
@click.option("-t", "--token", required=True, prompt=True, help="bearer token or api key")
@global_options
def add(profile: str, base_url: str, token: str) -> None:
    """Add a profile to your Nominal config"""
    cfg = config.NominalConfig.from_yaml()
    _validate_token_url(token, base_url)
    cfg.profiles[profile] = config.Profile(base_url, token)
    cfg.to_yaml()
    click.secho(f"Added profile {profile} to {config._DEFAULT_NOMINAL_CONFIG_PATH}", fg="green")


@profile.command()
@click.option("-p", "--profile", prompt=True)
@global_options
def remove(profile: str) -> None:
    """Remove a profile from your Nominal config"""
    cfg = config.NominalConfig.from_yaml()
    if profile in cfg.profiles:
        del cfg.profiles[profile]
    click.secho(f"Removed profile {profile} from {config._DEFAULT_NOMINAL_CONFIG_PATH}", fg="green")
