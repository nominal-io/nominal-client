from __future__ import annotations
import dataclasses

import click

from nominal import config, _config as _deprecated_config
from nominal.cli.util.global_decorators import global_options
from nominal.cli.util.verify_connection import validate_token_url


@click.group(name="config")
def config_cmd():
    pass


@config_cmd.command()
@click.option("-p", "--profile", prompt=True, help="profile name")
@click.option("-u", "--base-url", default="https://api.gov.nominal.io/api", prompt=True)
@click.option("-t", "--token", required=True, prompt=True, help="bearer token or api key")
@global_options
def add(profile: str, base_url: str, token: str) -> None:
    """Add or update a profile to your Nominal config"""
    cfg = config.NominalConfig.from_yaml()
    validate_token_url(token, base_url)
    new_cfg = dataclasses.replace(cfg, profiles={**cfg.profiles, profile: config.ConfigProfile(base_url, token)})
    new_cfg.to_yaml()
    click.secho(f"Added profile {profile} to {config._DEFAULT_NOMINAL_CONFIG_PATH}", fg="green")


@config_cmd.command()
@click.option("-p", "--profile", prompt=True)
@global_options
def remove(profile: str) -> None:
    """Remove a profile from your Nominal config"""
    cfg = config.NominalConfig.from_yaml()
    new_cfg = dataclasses.replace(cfg, profiles={k: v for k, v in cfg.profiles.items() if k != profile})
    new_cfg.to_yaml()
    click.secho(f"Removed profile {profile} from {config._DEFAULT_NOMINAL_CONFIG_PATH}", fg="green")


@config_cmd.command()
def migrate():
    """Interactively migrate deprecated environment-based config at ~/.nominal.yml to new profile-based config at ~/.config/nominal/config.yml"""
    deprecated_cfg = _deprecated_config.NominalConfig.from_yaml()
    profiles = {}
    for url, token in deprecated_cfg.environments.items():
        if click.prompt(f"Add profile for {url}?", default="y", type=bool):
            name = click.prompt("profile name")
            new_url = click.prompt("base url", default=f"https://{url}")
            new_token = click.prompt("token", default=token)
            if click.prompt("Validate connection?", default="y", type=bool):
                validate_token_url(new_token, new_url)
            profiles[name] = config.ConfigProfile(new_url, new_token)
    new_cfg = config.NominalConfig(profiles=profiles, version=2)
    new_cfg.to_yaml()
