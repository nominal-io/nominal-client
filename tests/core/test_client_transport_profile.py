from __future__ import annotations

import ssl
from unittest.mock import MagicMock, patch, sentinel

from nominal.config import ConfigProfile, NominalConfig
from nominal.core._utils.networking import SslContextProvider
from nominal.core.client import NominalClient


class _FakeSslContextProvider(SslContextProvider):
    def create_ssl_context(self) -> ssl.SSLContext:
        return ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)


def test_from_profile_passes_explicit_ssl_context_provider_to_clients_bunch() -> None:
    profile = ConfigProfile(
        base_url="https://api.example.mil/api",
        token="token",
        workspace_rid="ri.workspace.test.workspace.123",
    )
    config = NominalConfig(version=2, profiles={"cac": profile})
    provider = _FakeSslContextProvider()

    with (
        patch("nominal.config.NominalConfig.from_yaml", return_value=config),
        patch("nominal.core.client.ClientsBunch.from_config", return_value=sentinel.clients) as from_config,
    ):
        client = NominalClient.from_profile("cac", ssl_context_provider=provider)

    assert client._clients is sentinel.clients
    assert from_config.call_args.kwargs["ssl_context_provider"] is provider


def test_from_token_passes_ssl_context_provider_to_clients_bunch() -> None:
    provider = _FakeSslContextProvider()

    with patch("nominal.core.client.ClientsBunch.from_config", return_value=sentinel.clients) as from_config:
        client = NominalClient.from_token(
            "token",
            "https://api.example.mil/api",
            ssl_context_provider=provider,
        )

    assert client._clients is sentinel.clients
    assert from_config.call_args.kwargs["ssl_context_provider"] is provider


def test_from_profile_defaults_ssl_context_provider_to_none() -> None:
    profile = ConfigProfile(base_url="https://api.gov.nominal.io/api", token="token")
    config = NominalConfig(version=2, profiles={"default": profile})

    with (
        patch("nominal.config.NominalConfig.from_yaml", return_value=config),
        patch("nominal.core.client.ClientsBunch.from_config", return_value=MagicMock()) as from_config,
    ):
        NominalClient.from_profile("default")

    assert from_config.call_args.kwargs["ssl_context_provider"] is None
