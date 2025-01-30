from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

import yaml
from typing_extensions import Self  # typing.Self in 3.11+

from nominal.exceptions import NominalConfigError

_DEFAULT_NOMINAL_CONFIG_PATH = Path("~/.nominal.yml").expanduser().resolve()
_DEFAULT_NOMINAL_PROFILE_CONFIG_PATH = Path("~/.nominal_profile.yml").expanduser().resolve()


@dataclass
class ProfileConfig:
    url: str
    token: str


@dataclass
class NominalConfig:
    environments: dict[str, str] = None
    """environments map base_urls (with no scheme) to auth tokens (legacy support)"""
    profiles: dict[str, ProfileConfig] = None
    """profiles map profile names to profile configurations"""
    
    def __post_init__(self):
        self.environments = self.environments or {}
        self.profiles = self.profiles or {}

    @classmethod
    def from_yaml(cls, path: Path = _DEFAULT_NOMINAL_CONFIG_PATH) -> Self:
        if not path.exists():
            return cls(environments={}, profiles={})
        with open(path) as f:
            obj = yaml.safe_load(f)
        return cls(**obj)

    def to_yaml(self, path: Path = _DEFAULT_NOMINAL_CONFIG_PATH, create: bool = True) -> None:
        if create:
            path.touch()
        with open(path, "w") as f:
            yaml.dump(asdict(self), f)

    def set_profile(self, name: str, url: str, token: str, save: bool = True) -> None:
        """Set a named profile with URL and token"""
        if url.startswith("http"):
            raise ValueError(f"url {url!r} must not include the http:// or https:// scheme")
        self.profiles[name] = ProfileConfig(url=url, token=token)
        if save:
            self.to_yaml(_DEFAULT_NOMINAL_PROFILE_CONFIG_PATH)

    def get_profile(self, name: str) -> ProfileConfig:
        """Get a profile configuration by name"""
        if name in self.profiles:
            return self.profiles[name]
        raise NominalConfigError(f"profile {name!r} not found in config: set a profile with `nom auth set-profile`")

    def set_token(self, url: str, token: str, save: bool = True) -> None:
        """Legacy method for backward compatibility"""
        if url.startswith("http"):
            raise ValueError(f"url {url!r} must not include the http:// or https:// scheme")
        self.environments[url] = token
        if save:
            self.to_yaml()

    def get_token(self, url: str) -> str:
        """Legacy method for backward compatibility"""
        if url.startswith("http"):
            raise ValueError(f"url {url!r} must not include the http:// or https:// scheme")
        if url in self.environments:
            return self.environments[url]
        raise NominalConfigError(f"url {url!r} not found in config: set a token with `nom auth set-token`")


def get_profile(name: str, config_path: Path = _DEFAULT_NOMINAL_PROFILE_CONFIG_PATH) -> ProfileConfig:
    return NominalConfig.from_yaml(path=config_path).get_profile(name)


def set_profile(name: str, url: str, token: str) -> None:
    cfg = NominalConfig.from_yaml(path=_DEFAULT_NOMINAL_PROFILE_CONFIG_PATH)
    cfg.set_profile(name, _strip_scheme(url), token)


def get_token(url: str, config_path: Path = _DEFAULT_NOMINAL_CONFIG_PATH) -> str:
    return NominalConfig.from_yaml(path=config_path).get_token(_strip_scheme(url))


def set_token(url: str, token: str) -> None:
    cfg = NominalConfig.from_yaml()
    cfg.set_token(_strip_scheme(url), token)


def _strip_scheme(url: str) -> str:
    if "://" in url:
        return url.split("://", 1)[-1]
    return url
