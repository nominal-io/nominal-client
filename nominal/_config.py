from __future__ import annotations

from pathlib import Path

import pydantic
import yaml
from typing_extensions import Self  # typing.Self in 3.11+

from nominal.exceptions import NominalConfigError

_DEFAULT_NOMINAL_CONFIG_PATH = Path("~/.nominal.yml").expanduser()


class NominalConfig(pydantic.BaseModel):
    environments: dict[str, str]
    """environments map base_urls (with no scheme) to auth tokens"""

    @classmethod
    def from_yaml(cls, path: Path = _DEFAULT_NOMINAL_CONFIG_PATH) -> Self:
        if not path.exists():
            return cls(environments={})
        with open(path) as f:
            obj = yaml.safe_load(f)
        return cls.model_validate(obj)

    def to_yaml(self, path: Path = _DEFAULT_NOMINAL_CONFIG_PATH, create: bool = True) -> None:
        if create:
            path.touch()
        with open(path, "w") as f:
            yaml.dump(self.model_dump(), f)

    def set_token(self, url: str, token: str, save: bool = True) -> None:
        if url.startswith("http"):
            raise ValueError("url {url!r} must not include the http:// or https:// scheme")
        self.environments[url] = token
        if save:
            self.to_yaml()

    def get_token(self, url: str) -> str:
        if url.startswith("http"):
            raise ValueError("url {url!r} must not include the http:// or https:// scheme")
        if url in self.environments:
            return self.environments[url]
        raise NominalConfigError(f"url {url!r} not found in config: set a token with `nom auth set-token`")


def get_token(url: str) -> str:
    return NominalConfig.from_yaml().get_token(_strip_scheme(url))


def set_token(url: str, token: str) -> None:
    cfg = NominalConfig.from_yaml()
    cfg.set_token(_strip_scheme(url), token)


def _strip_scheme(url: str) -> str:
    if "://" in url:
        return url.split("://", 1)[-1]
    return url
