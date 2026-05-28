from __future__ import annotations

import ssl
import sys
from unittest.mock import MagicMock

import pytest

from nominal.core import TransportProvider
from nominal.core.dataset import Dataset, DatasetBounds
from nominal.core.user import User
from nominal.experimental.dataset_utils import get_dataset_owner, get_dataset_owner_rid
from nominal.experimental.dataset_utils._dataset_utils import (
    _api_base_url_to_grpc_target,
    _lookup_dataset_owner_rid,
)


class _FakeGrpcTransportProvider(TransportProvider):
    def __init__(self) -> None:
        self.credentials = MagicMock(name="custom-grpc-credentials")

    def create_ssl_context(self) -> ssl.SSLContext:
        return ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    def create_grpc_channel_credentials(self, *, root_certificates=None, certificate_chain_pem=None):
        return self.credentials


def _make_mock_grpc_env():
    """Return (mock_grpc, sys_modules_patches) for use in tests that call _lookup_dataset_owner_rid."""
    mock_grpc = MagicMock(name="grpc")
    mock_channel = MagicMock()
    mock_channel.__enter__ = lambda s: s
    mock_channel.__exit__ = MagicMock(return_value=False)
    mock_grpc.secure_channel.return_value = mock_channel

    stub = MagicMock()
    stub.GetResourceRoles.return_value = MagicMock(role_assignments=[])
    roles_pb2 = MagicMock()
    roles_pb2_grpc = MagicMock()
    roles_pb2_grpc.RoleServiceStub.return_value = stub

    protos_v1 = MagicMock()
    protos_v1.roles_pb2 = roles_pb2
    protos_v1.roles_pb2_grpc = roles_pb2_grpc

    patches = {
        "grpc": mock_grpc,
        "nominal_api_protos": MagicMock(),
        "nominal_api_protos.nominal": MagicMock(),
        "nominal_api_protos.nominal.authorization": MagicMock(),
        "nominal_api_protos.nominal.authorization.roles": MagicMock(),
        "nominal_api_protos.nominal.authorization.roles.v1": protos_v1,
    }
    return mock_grpc, patches


@pytest.fixture
def mock_dataset():
    clients = MagicMock()
    dataset = Dataset(
        rid="test-rid",
        name="Test Dataset",
        description="A dataset for testing",
        bounds=DatasetBounds(start=123455, end=123456),
        properties={},
        labels=[],
        _clients=clients,
    )
    clients.auth_header = "Bearer test-token"
    clients._api_base_url = "https://api.gov.nominal.io/api"
    return dataset


def test_get_dataset_owner_rid_uses_role_service_lookup(monkeypatch: pytest.MonkeyPatch, mock_dataset: Dataset):
    def fake_lookup(*, auth_header: str, api_base_url: str, dataset_rid: str, transport_provider=None) -> str | None:
        assert auth_header is mock_dataset._clients.auth_header
        assert api_base_url is mock_dataset._clients._api_base_url  # type: ignore[attr-defined]
        assert dataset_rid == mock_dataset.rid
        return "ri.authn.user.owner"

    monkeypatch.setattr("nominal.experimental.dataset_utils._dataset_utils._lookup_dataset_owner_rid", fake_lookup)

    assert get_dataset_owner_rid(mock_dataset) == "ri.authn.user.owner"


def test_get_dataset_owner_returns_user_from_owner_rid(monkeypatch: pytest.MonkeyPatch, mock_dataset: Dataset):
    mock_dataset._clients.authentication.get_user.return_value = User(  # type: ignore[attr-defined]
        rid="ri.authn.user.owner",
        display_name="Owner User",
        email="owner@nominal.io",
    )
    monkeypatch.setattr(
        "nominal.experimental.dataset_utils._dataset_utils.get_dataset_owner_rid",
        lambda dataset: "ri.authn.user.owner",
    )

    owner = get_dataset_owner(mock_dataset)

    assert owner.rid == "ri.authn.user.owner"
    mock_dataset._clients.authentication.get_user.assert_called_once_with(  # type: ignore[attr-defined]
        mock_dataset._clients.auth_header, "ri.authn.user.owner"
    )


def test_get_dataset_owner_rid_raises_when_no_owner_found(monkeypatch: pytest.MonkeyPatch, mock_dataset: Dataset):
    monkeypatch.setattr("nominal.experimental.dataset_utils._dataset_utils._lookup_dataset_owner_rid", lambda **_: None)

    with pytest.raises(ValueError, match="Could not resolve an owner for dataset"):
        get_dataset_owner_rid(mock_dataset)


def test_api_base_url_to_grpc_target_strips_scheme_and_api_suffix() -> None:
    assert _api_base_url_to_grpc_target("https://api.gov.nominal.io/api") == "api.gov.nominal.io"
    assert _api_base_url_to_grpc_target("https://api.gov.nominal.io") == "api.gov.nominal.io"
    assert _api_base_url_to_grpc_target("http://localhost:8080/api") == "localhost:8080"


def test_get_dataset_owner_rid_passes_transport_provider_to_lookup(
    monkeypatch: pytest.MonkeyPatch, mock_dataset: Dataset
) -> None:
    """get_dataset_owner_rid must forward dataset._clients.transport_provider to _lookup_dataset_owner_rid."""
    provider = _FakeGrpcTransportProvider()
    mock_dataset._clients.transport_provider = provider  # type: ignore[attr-defined]

    captured: dict = {}

    def fake_lookup(*, auth_header, api_base_url, dataset_rid, transport_provider=None):
        captured["transport_provider"] = transport_provider
        return "ri.authn.user.owner"

    monkeypatch.setattr("nominal.experimental.dataset_utils._dataset_utils._lookup_dataset_owner_rid", fake_lookup)

    get_dataset_owner_rid(mock_dataset)

    assert captured["transport_provider"] is provider


def test_lookup_dataset_owner_rid_uses_provider_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """When transport_provider is given, create_grpc_channel_credentials() must be used for the gRPC channel."""
    provider = _FakeGrpcTransportProvider()
    mock_grpc, patches = _make_mock_grpc_env()

    for name, mod in patches.items():
        monkeypatch.setitem(sys.modules, name, mod)

    _lookup_dataset_owner_rid(
        auth_header="Bearer test",
        api_base_url="https://api.example.com",
        dataset_rid="ri.dataset.main.dataset.1",
        transport_provider=provider,
    )

    mock_grpc.ssl_channel_credentials.assert_not_called()
    _target, credentials = mock_grpc.secure_channel.call_args.args
    assert credentials is provider.credentials


def test_lookup_dataset_owner_rid_falls_back_to_default_ssl_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without transport_provider, grpc.ssl_channel_credentials() must be used as the fallback."""
    mock_grpc, patches = _make_mock_grpc_env()

    for name, mod in patches.items():
        monkeypatch.setitem(sys.modules, name, mod)

    _lookup_dataset_owner_rid(
        auth_header="Bearer test",
        api_base_url="https://api.example.com",
        dataset_rid="ri.dataset.main.dataset.1",
    )

    mock_grpc.ssl_channel_credentials.assert_called_once_with()
    _target, credentials = mock_grpc.secure_channel.call_args.args
    assert credentials is mock_grpc.ssl_channel_credentials.return_value
