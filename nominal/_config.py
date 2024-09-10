from __future__ import annotations

from pathlib import Path

import pydantic
import yaml

from nominal.exceptions import NominalConfigError
from typing_extensions import Self  # typing.Self in 3.11+

_DEFAULT_NOMINAL_CONFIG_PATH = Path("~/.nominal.yml").expanduser()
_DEFAULT_BASE_URL = "api.gov.nominal.io/api"


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

    def get_token(self, url: str = _DEFAULT_BASE_URL) -> str:
        if url in self.environments:
            return self.environments[url]
        raise NominalConfigError(f"url {url!r} not found in config")
