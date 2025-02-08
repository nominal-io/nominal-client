from __future__ import annotations

from dataclasses import asdict, dataclass
import logging
from pathlib import Path
from types import MappingProxyType
from typing import Literal, Mapping

import yaml
from typing_extensions import Self  # typing.Self in 3.11+

from nominal._config import (
    NominalConfigV1,
    _DEFAULT_NOMINAL_CONFIG_PATH as DEPRECATED_NOMINAL_CONFIG_PATH,
)
from nominal.exceptions import NominalConfigError


_DEFAULT_NOMINAL_CONFIG_PATH = Path("~/.config/nominal/config.yml").expanduser().resolve()
logger = logging.getLogger(__name__)


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
      dev:
        base_url: http://api.nominal.test
        token: eyJ...


    For production environments, the typical URL is: https://api.gov.nominal.io/api
    For staging environments, the typical URL is: https://api-staging.gov.nominal.io/api
    For local development, the typical URL is: https://api.nominal.test (note the lack of the /api suffix)
    """

    profiles: Mapping[str, ConfigProfile]
    version: Literal[2]

    @classmethod
    def from_yaml(cls, path: Path = _DEFAULT_NOMINAL_CONFIG_PATH) -> Self:
        if not path.exists():
            if DEPRECATED_NOMINAL_CONFIG_PATH.exists():
                _migrate_deprecated_config()
            raise FileNotFoundError(
                f"no config file found at {_DEFAULT_NOMINAL_CONFIG_PATH}: create with `nom config profile add`"
            )
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
        return cls(version=2, profiles=MappingProxyType(profiles))

    def to_yaml(self, path: Path = _DEFAULT_NOMINAL_CONFIG_PATH) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            yaml.dump(asdict(self), f)

    def get_profile(self, name: str) -> ConfigProfile:
        if name in self.profiles:
            return self.profiles[name]
        raise NominalConfigError(f"profile {name!r} not found in config: add with `nom config profile add`")


@dataclass(frozen=True)
class ConfigProfile:
    base_url: str
    token: str


class _NominalConfigMigrationError(NominalConfigError):
    """Unable to automatically migrate v1 config to v2"""


def _migrate_deprecated_config() -> None:
    logger.info("migrating deprecated config to v2")

    prod_url = "api.gov.nominal.io/api"
    staging_url = "api-staging.gov.nominal.io/api"
    dev_url = "api.nominal.test"

    deprecated_cfg = NominalConfigV1.from_yaml()
    logger.debug(f"retrieved deprecated config: {deprecated_cfg}")
    env = deprecated_cfg.environments.copy()
    profiles = {}

    # if there's only one config, make it the default
    if len(env) == 1:
        url, token = env.popitem()
        profiles["default"] = ConfigProfile(base_url=f"https://{url}", token=token)
        logger.debug(f"creating profile 'default' from the single environment: {profiles['default']}")
    # if there's a prod URL, make that the default
    elif prod_url in env:
        profiles["default"] = ConfigProfile(base_url=f"https://{prod_url}", token=env.pop(prod_url))
        logger.debug(f"creating profile 'default' from the {prod_url} environment: {profiles['default']}")
    # otherwise, not obvious how to migrate
    else:
        raise _NominalConfigMigrationError("unable to automatically migrate v1 config to v2: use `nom config migrate`")

    if staging_url in env:
        profiles["staging"] = ConfigProfile(base_url=f"https://{staging_url}", token=env.pop(staging_url))
        logger.debug(f"creating profile 'staging' from the {staging_url} environment: {profiles['staging']}")
    if dev_url in env:
        profiles["dev"] = ConfigProfile(base_url=f"https://{dev_url}", token=env.pop(dev_url))
        logger.debug(f"creating profile 'dev' from the {dev_url} environment: {profiles['dev']}")
    while env:
        url, token = env.popitem()
        profiles[url] = ConfigProfile(base_url=f"https://{url}", token=token)
        logger.debug(f"creating profile '{url}' from the {url} environment: {profiles[url]}")

    cfg = NominalConfig(version=2, profiles=MappingProxyType(profiles))
    logger.info(f"migrating deprecated config to new config: {cfg}")
    cfg.to_yaml()
    logger.warning(
        f"we recommend deleting deprecated config file {DEPRECATED_NOMINAL_CONFIG_PATH} containing {deprecated_cfg}"
    )
    logger.info(f"successfully migrated over to the v2 nominal config at {_DEFAULT_NOMINAL_CONFIG_PATH}")
