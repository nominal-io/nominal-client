from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal, Mapping

import yaml
from typing_extensions import Self  # typing.Self in 3.11+

from nominal._config import _DEFAULT_NOMINAL_CONFIG_PATH as DEPRECATED_NOMINAL_CONFIG_PATH
from nominal.exceptions import NominalConfigError

DEFAULT_NOMINAL_CONFIG_PATH = Path("~/.config/nominal/config.yml").expanduser().resolve()
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConfigProfile:
    base_url: str
    token: str
    workspace_rid: str | None = None


@dataclass(frozen=True)
class NominalConfig:
    """Nominal configuration. Stores connection profiles.

    By default, the configuration is stored in ~/.config/nominal/config.yml.

    Example config file
    -------------------
    version: 2
    profiles:
      default:
        base_url: https://api.gov.nominal.io/api
        token: nominal_api_key_...
      staging:
        base_url: https://api-staging.gov.nominal.io/api
        token: eyJ...
        workspace_rid: ri.security.gov-staging.workspace.82db1f3a-568e-418e-a2d0-0575396f29a2
      dev:
        base_url: https://api.nominal.test
        token: eyJ...


    For production environments, the typical URL is: https://api.gov.nominal.io/api
    For staging environments, the typical URL is: https://api-staging.gov.nominal.io/api
    For local development, the typical URL is: https://api.nominal.test (note the lack of the /api suffix)
    """

    profiles: Mapping[str, ConfigProfile]
    version: Literal[2]

    @classmethod
    def from_yaml(cls, path: Path = DEFAULT_NOMINAL_CONFIG_PATH) -> Self:
        if not path.exists():
            if DEPRECATED_NOMINAL_CONFIG_PATH.exists():
                raise FileNotFoundError(
                    f"no config file found at {path}: deprecated config file {DEPRECATED_NOMINAL_CONFIG_PATH} found. "
                    f"migrate with `nom config migrate`",
                )
            raise FileNotFoundError(f"no config file found at {path}: create with `nom config profile add`")
        with open(path) as f:
            obj = yaml.safe_load(f)
        if "version" not in obj:
            raise NominalConfigError(f"missing 'version' key in config file: {path}")
        if "profiles" not in obj:
            raise NominalConfigError(f"missing 'profiles' key in config file: {path}")
        version = obj["version"]
        if version != 2:
            raise NominalConfigError(f"unsupported config version: {version}")
        profiles = {name: ConfigProfile(**params) for name, params in obj["profiles"].items()}
        return cls(version=2, profiles=profiles)

    def to_yaml(self, path: Path = DEFAULT_NOMINAL_CONFIG_PATH) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            yaml.dump(asdict(self), f)

    def get_profile(self, name: str) -> ConfigProfile:
        if name in self.profiles:
            return self.profiles[name]
        raise NominalConfigError(f"profile {name!r} not found in config: add with `nom config profile add`")
