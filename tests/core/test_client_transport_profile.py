from __future__ import annotations

from unittest.mock import patch, sentinel

from nominal.config import ConfigProfile, NominalConfig
from nominal.core import TransportProvider
from nominal.core.client import NominalClient


def test_from_profile_passes_explicit_transport_provider_to_clients_bunch() -> None:
    profile = ConfigProfile(
        base_url="https://api.example.mil/api",
        token="token",
        workspace_rid="ri.workspace.test.workspace.123",
    )
    config = NominalConfig(version=2, profiles={"cac": profile})
    provider = TransportProvider()

    with (
        patch("nominal.config.NominalConfig.from_yaml", return_value=config),
        patch("nominal.core.client.ClientsBunch.from_config", return_value=sentinel.clients) as from_config,
    ):
        client = NominalClient.from_profile("cac", transport_provider=provider)

    assert client._clients is sentinel.clients
    assert from_config.call_args.kwargs["transport_provider"] is provider


def test_from_token_passes_transport_provider_to_clients_bunch() -> None:
    provider = TransportProvider()

    with patch("nominal.core.client.ClientsBunch.from_config", return_value=sentinel.clients) as from_config:
        client = NominalClient.from_token(
            "token",
            "https://api.example.mil/api",
            transport_provider=provider,
        )

    assert client._clients is sentinel.clients
    assert from_config.call_args.kwargs["transport_provider"] is provider


def test_from_profile_defaults_transport_provider_to_none() -> None:
    profile = ConfigProfile(base_url="https://api.gov.nominal.io/api", token="token")
    config = NominalConfig(version=2, profiles={"default": profile})

    with (
        patch("nominal.config.NominalConfig.from_yaml", return_value=config),
        patch("nominal.core.client.ClientsBunch.from_config", return_value=sentinel.clients) as from_config,
    ):
        NominalClient.from_profile("default")

    assert from_config.call_args.kwargs["transport_provider"] is None
