from __future__ import annotations

import dataclasses
from pathlib import Path

import click

from nominal import _config as _deprecated_config
from nominal import config
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
@click.option("-t", "--token", required=True, help="bearer token or api key")
@click.option("-u", "--base-url", default="https://api.gov.nominal.io/api")
@click.option("-w", "--workspace-rid", help="workspace RID  [optional]")
@click.option("--validate/--no-validate", default=True, help="Validate authentication parameters")
@global_options
def add_profile(profile: str, base_url: str, token: str, workspace_rid: str | None, validate: bool) -> None:
    """Add or update a profile to your Nominal config"""
    cfg = config.NominalConfig(profiles={}, version=2)
    try:
        cfg = config.NominalConfig.from_yaml()
    except FileNotFoundError:
        pass
    if validate:
        validate_token_url(token, base_url, workspace_rid)
    new_cfg = dataclasses.replace(
        cfg, profiles={**cfg.profiles, profile: config.ConfigProfile(base_url, token, workspace_rid)}
    )
    new_cfg.to_yaml()
    click.secho(f"Wrote profile {profile} to {config.DEFAULT_NOMINAL_CONFIG_PATH}", fg="green")


@profile_cmd.command("remove")
@click.argument("profile")
@global_options
def remove_profile(profile: str) -> None:
    """Remove a profile from your Nominal config"""
    cfg = config.NominalConfig.from_yaml()
    new_cfg = dataclasses.replace(cfg, profiles={k: v for k, v in cfg.profiles.items() if k != profile})
    new_cfg.to_yaml()
    click.secho(f"Removed profile {profile} from {config.DEFAULT_NOMINAL_CONFIG_PATH}", fg="green")


@config_cmd.command()
def migrate() -> None:
    """Interactively migrate deprecated config at ~/.nominal.yml
    to new profile-based config at ~/.config/nominal/config.yml
    """
    deprecated_cfg = _deprecated_config.NominalConfig.from_yaml()
    profiles = {}
    for url, token in deprecated_cfg.environments.items():
        if click.prompt(f"Add profile for {url}?", default="y", type=bool):
            name = click.prompt("Profile name (used to create a client, e.g. NominalClient.from_profile('name'))")
            new_url = click.prompt("API Base url", default=f"https://{url}")
            new_token = click.prompt("Token", default=token)
            new_workspace_rid = None
            if click.prompt("Add workspace?", default="n", type=bool):
                new_workspace_rid = click.prompt("Workspace RID")
            if click.prompt("Validate authentication?", default="y", type=bool):
                validate_token_url(new_token, new_url, new_workspace_rid)
            profiles[name] = config.ConfigProfile(new_url, new_token, new_workspace_rid)
    new_cfg = config.NominalConfig(profiles=profiles, version=2)
    new_cfg.to_yaml()
    click.secho(f"Migrated config to {config.DEFAULT_NOMINAL_CONFIG_PATH}", fg="green")


@profile_cmd.command("list")
@global_options
def list_profiles() -> None:
    """List the profiles in your Nominal config"""
    cfg = config.NominalConfig(profiles={}, version=2)
    try:
        cfg = config.NominalConfig.from_yaml()
    except FileNotFoundError:
        pass

    default_config_path = config.DEFAULT_NOMINAL_CONFIG_PATH
    home = Path.home()
    if home in default_config_path.parents:
        config_path = "~/" + str(default_config_path.relative_to(home))
    else:
        config_path = str(default_config_path)

    if len(cfg.profiles) == 0:
        print(f"No profiles found in `{config_path}`")
        return

    print(f"Profiles from `{config_path}`:\n")

    for profile_name, profile in cfg.profiles.items():
        print(f"- {profile_name} (", end="")
        print(profile.base_url, end="")
        if not profile.token:
            print(", missing token", end="")
        if profile.workspace_rid:
            print(", in workspace", end="")
        print(")")
