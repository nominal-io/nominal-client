from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

import yaml
from typing_extensions import Self  # typing.Self in 3.11+

from nominal.exceptions import NominalConfigError
from nominal._config import NominalConfig as DeprecatedNominalConfig

_DEFAULT_NOMINAL_CONFIG_PATH = Path("~/.nominal.yml").expanduser().resolve()


@dataclass
class NominalConfig:
    """Nominal configuration. Stores connection profiles.
    By default, the configuration is stored in ~/.nominal.yml.

    Example config file
    -------------------
    profiles:
      prod:
        base_url: https://api.gov.nominal.io/api
        token: nominal_api_key_...
      staging:
        base_url: https://api-staging.gov.nominal.io/api
        token: eyJ...
      local:
        base_url: http://api.nominal.test
        token: eyJ...
    """

    profiles: dict[str, Profile]

    @classmethod
    def from_yaml(cls, path: Path = _DEFAULT_NOMINAL_CONFIG_PATH) -> Self:
        if not path.exists():
            cfg = cls(profiles={})
            cfg.to_yaml(path)
            return cfg
        with open(path) as f:
            obj = yaml.safe_load(f)
        return cls(**obj)

    def to_yaml(self, path: Path = _DEFAULT_NOMINAL_CONFIG_PATH, create: bool = True) -> None:
        if create:
            path.touch()
        with open(path, "w") as f:
            yaml.dump(asdict(self), f)

    def get_profile(self, name: str) -> Profile:
        if name in self.profiles:
            return self.profiles[name]
        raise NominalConfigError(f"profile {name!r} not found in config: use `nom auth profile add`")


@dataclass
class Profile:
    base_url: str
    token: str
