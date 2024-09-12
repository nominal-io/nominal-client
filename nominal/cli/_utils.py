from __future__ import annotations

import click

from .._config import get_token
from ..core import NominalClient

BASE_URL_OPTION = click.option("--base-url", default="https://api.gov.nominal.io/api")
TOKEN_OPTION = click.option("--token", help="[default: looked up in ~/.nominal.yml]")


def get_client(base_url: str, token: str | None) -> NominalClient:
    if token is None:
        token = get_token(base_url)
    return NominalClient.create(base_url, token)
