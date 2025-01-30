from __future__ import annotations

import click
from conjure_python_client import ConjureHTTPError

from nominal import NominalClient, _config
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


@auth_cmd.command(deprecated=True)
@click.option("-u", "--base-url", default="https://api.gov.nominal.io/api", prompt=True)
@click.option(
    "-t", "--token", required=True, prompt=True, help="access token, can be found in /sandbox on your Nominal instance"
)
@global_options
def set_token(token: str, base_url: str) -> None:
    """[Deprecated] Update the token for a given URL in the Nominal config file.
    
    This command is deprecated. Please use 'auth set-profile' instead, which provides
    better profile management and configuration options.
    """
    click.secho(
        "Warning: 'set-token' is deprecated and will be removed in a future version. "
        "Please use 'auth set-profile' instead.",
        fg="yellow",
        err=True,
    )
    path = _config._DEFAULT_NOMINAL_CONFIG_PATH
    _validate_token_url(token, base_url)
    _config.set_token(base_url, token)
    click.secho(f"Successfully set token for '{base_url}' in {path}", fg="green")


@auth_cmd.command()
@click.argument("profile-name")
@click.option("-u", "--base-url", default="https://api.gov.nominal.io/api", prompt=True)
@click.option(
    "-t", "--token", required=True, prompt=True, help="access token, can be found in /sandbox on your Nominal instance"
)
@global_options
def set_profile(profile_name: str, token: str, base_url: str) -> None:
    """Create or update a named profile in the Nominal config file"""
    path = _config._DEFAULT_NOMINAL_PROFILE_CONFIG_PATH
    _validate_token_url(token, base_url)
    _config.set_profile(profile_name, base_url, token)
    click.secho(f"Successfully set profile '{profile_name}' for '{base_url}' in {path}", fg="green")


@auth_cmd.command(name="list-profiles")
@global_options
def list_profiles() -> None:
    """List all configured profiles"""
    cfg = _config.NominalConfig.from_yaml(path=_config._DEFAULT_NOMINAL_PROFILE_CONFIG_PATH)
    if not cfg.profiles:
        click.secho("No profiles configured", fg="yellow")
        return
    
    click.secho("Configured profiles:", fg="green")
    for name, profile in cfg.profiles.items():
        click.echo(f"  {name}:")
        click.echo(f"    URL: {profile.url}")
        click.echo(f"    Token: {'*' * 8}{profile.token[-4:]}")


@auth_cmd.command()
@click.argument("profile-name")
@global_options
def get_profile(profile_name: str) -> None:
    """Show details for a specific profile"""
    try:
        profile = _config.get_profile(profile_name)
        click.secho(f"Profile: {profile_name}", fg="green")
        click.echo(f"  URL: {profile.url}")
        click.echo(f"  Token: {'*' * 8}{profile.token[-4:]}")
    except ValueError as e:
        click.secho(str(e), fg="red", err=True)
        raise click.ClickException("Profile not found")
