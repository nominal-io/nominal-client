"""Unit tests for the with_http_shim client transform and gRPC-stub discovery."""

from __future__ import annotations

import pytest

from nominal.core.client import NominalClient
from nominal.experimental.grpc_hacks import _is_grpc_stub, with_http_shim

_BASE_URL = "https://api.example.test/api"

# gRPC-backed services the shim should rebind, and conjure/HTTP services it must leave alone.
_GRPC_SERVICES = ("workspace", "registry", "containerized_extractor", "units", "comments", "roles")
_CONJURE_SERVICES = ("run", "ingest", "catalog")


@pytest.fixture
def offline_client() -> NominalClient:
    """A client built without any network I/O (channels connect lazily)."""
    return NominalClient.from_token("test-token", _BASE_URL)


def test_is_grpc_stub_distinguishes_grpc_from_conjure(offline_client: NominalClient) -> None:
    """_is_grpc_stub recognizes generated gRPC stubs and rejects conjure clients and plain values."""
    assert _is_grpc_stub(offline_client._clients.workspace) is True
    assert _is_grpc_stub(offline_client._clients.run) is False
    assert _is_grpc_stub("not a stub") is False


def test_with_http_shim_rebinds_every_grpc_service_and_leaves_conjure_untouched(offline_client: NominalClient) -> None:
    """with_http_shim rebinds all gRPC services while leaving conjure services as-is."""
    shimmed = with_http_shim(offline_client)
    for name in _GRPC_SERVICES:
        assert getattr(shimmed._clients, name) is not getattr(offline_client._clients, name)
    for name in _CONJURE_SERVICES:
        assert getattr(shimmed._clients, name) is getattr(offline_client._clients, name)
