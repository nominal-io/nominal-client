"""Choose, list and create profiles, and get a client on the chosen one.

A profile is a named Nominal API key and URL in the config file, ``~/.config/nominal/config.yml``
(`nominal.config.DEFAULT_NOMINAL_CONFIG_PATH`). The *active* profile, which `get_client` and the
``df.nom.to_nominal`` upload use when told no other, is the first of these that is set:

1. `use_profile`, in this Python session
2. the ``NOMINAL_PROFILE`` environment variable (`ENV`), or the older ``PANOMINAL_PROFILE`` (`LEGACY_ENV`)
3. ``"default"`` (`DEFAULT`)

``NOMINAL_WORKSPACE_RID`` (`WORKSPACE_ENV`), if set, picks the workspace instead of the profile's own.

Nothing here changes `nominal.core.NominalClient.from_profile`, which always takes the profile it is given.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import nominal.core

DEFAULT = "default"
"""The profile used when neither `use_profile` nor the environment names one."""

ENV = "NOMINAL_PROFILE"
"""Environment variable naming the profile to use."""

LEGACY_ENV = "PANOMINAL_PROFILE"
"""The environment variable panominal used; honored when ``NOMINAL_PROFILE`` is not set."""

WORKSPACE_ENV = "NOMINAL_WORKSPACE_RID"
"""Environment variable naming the workspace (its RID) to use instead of the profile's own."""

_chosen: str | None = None


def active_profile() -> str:
    """Return the name of the profile that `get_client` uses now.

    The name may not exist yet; check it against `list_profiles`.
    """
    return _chosen or os.environ.get(ENV) or os.environ.get(LEGACY_ENV) or DEFAULT


def use_profile(name: str | None) -> None:
    """Use this profile for the rest of the Python session.

    Args:
        name: A profile in `list_profiles`, or ``None`` to go back to the environment variable, or ``"default"``.

    Raises:
        ValueError: If there's no profile called ``name``.
    """
    global _chosen
    if name is not None and name not in list_profiles():
        raise ValueError(f"no Nominal profile {name!r}; {_available()}")
    _chosen = name


def list_profiles() -> dict[str, str]:
    """Return the profiles in the config file, as names mapped to their API URLs.

    Tokens aren't included. Returns ``{}`` if there's no config file yet.
    """
    from nominal.config import NominalConfig

    path = _config_path()
    if not path.exists():
        return {}
    return {name: profile.base_url for name, profile in NominalConfig.from_yaml(path).profiles.items()}


def get_client(profile: str | None = None) -> nominal.core.NominalClient:
    """Return a `~nominal.core.NominalClient` for a profile.

    It uses the profile's workspace, or the one ``NOMINAL_WORKSPACE_RID`` names.

    Args:
        profile: The profile's name. Defaults to `active_profile`.

    Raises:
        ValueError: If there's no such profile.
    """
    from nominal.config import NominalConfig
    from nominal.core import NominalClient
    from nominal.exceptions import NominalConfigError

    name = profile or active_profile()
    workspace = os.environ.get(WORKSPACE_ENV)
    try:
        if not workspace:
            return NominalClient.from_profile(name)
        # from_profile, with the workspace replaced
        found = NominalConfig.from_yaml(_config_path()).get_profile(name)
        return NominalClient.from_token(found.token, found.base_url, workspace_rid=workspace, _profile=name)
    except (FileNotFoundError, NominalConfigError) as e:
        raise ValueError(f"no Nominal profile {name!r}; {_available()}") from e


def make_profile(
    name: str = DEFAULT,
    *,
    token: str,
    base_url: str | None = None,
    workspace_rid: str | None = None,
) -> nominal.core.NominalClient:
    """Save a Nominal API key as a profile, replacing any profile with the same name.

    The key is checked against Nominal before it's saved, and the file is made readable only by you.
    Create a key in Nominal's settings. This does what ``nom config profile add`` does::

        nominal.config.make_profile(token="nominal_api_key_...")

    Args:
        name: The profile's name. ``"default"`` is the one used unless told otherwise.
        token: A Nominal API key.
        base_url: Your Nominal deployment's API. Defaults to the client's default URL.
        workspace_rid: The workspace to use, if not your default one.

    Returns:
        A `~nominal.core.NominalClient` on the new profile.
    """
    from nominal.config import ConfigProfile, NominalConfig
    from nominal.core import NominalClient
    from nominal.core._constants import DEFAULT_API_BASE_URL

    url = base_url or DEFAULT_API_BASE_URL
    client = NominalClient.from_token(token, url, workspace_rid=workspace_rid, _profile=name)
    client.get_user()  # fails here, before saving, if the key or URL is wrong
    path = _config_path()
    profiles = dict(NominalConfig.from_yaml(path).profiles) if path.exists() else {}
    profiles[name] = ConfigProfile(base_url=url, token=token, workspace_rid=workspace_rid)
    NominalConfig(profiles=profiles, version=2).to_yaml(path)
    path.chmod(0o600)  # it holds API keys
    return client


def _config_path() -> Path:
    """Return the path of the config file."""
    from nominal.config import DEFAULT_NOMINAL_CONFIG_PATH

    return DEFAULT_NOMINAL_CONFIG_PATH


def _available() -> str:
    """Say which profiles exist, and how to make one."""
    names = ", ".join(repr(n) for n in list_profiles())
    have = f"profiles: {names}" if names else "there are no profiles yet"
    return f"{have}. Make one with nominal.config.make_profile(name, token=...) or `nom config profile add`"
