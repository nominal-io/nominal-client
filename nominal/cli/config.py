from __future__ import annotations

import dataclasses

import click


from nominal.io import config
from nominal.cli.util.global_decorators import global_options
from nominal.cli.util.verify_connection import validate_token_url


@click.group(name="config")
def config_cmd() -> None:
    pass


@config_cmd.group(name="profile")
def profile_cmd() -> None:
    pass


@profile_cmd.command("add")
@click.argument("profile")
@click.option("-u", "--base-url", default="https://api.gov.nominal.io/api", prompt=True)
@click.option("-t", "--token", required=True, prompt=True, help="bearer token or api key")
@global_options
def add_profile(profile: str, base_url: str, token: str) -> None:
    """Add or update a profile to your Nominal config"""
    cfg = config.NominalConfig(profiles={}, version=2)
    try:
        cfg = config.NominalConfig.from_yaml()
    except FileNotFoundError:
        pass
    validate_token_url(token, base_url)
    new_cfg = dataclasses.replace(cfg, profiles={**cfg.profiles, profile: config.ConfigProfile(base_url, token)})
    new_cfg.to_yaml()
    click.secho(f"Added profile {profile} to {config.DEFAULT_NOMINAL_CONFIG_PATH}", fg="green")


@profile_cmd.command("remove")
@click.argument("profile")
@global_options
def remove_profile(profile: str) -> None:
    """Remove a profile from your Nominal config"""
    cfg = config.NominalConfig.from_yaml()
    new_cfg = dataclasses.replace(cfg, profiles={k: v for k, v in cfg.profiles.items() if k != profile})
    new_cfg.to_yaml()
    click.secho(f"Removed profile {profile} from {config.DEFAULT_NOMINAL_CONFIG_PATH}", fg="green")
