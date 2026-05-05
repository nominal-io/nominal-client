from __future__ import annotations

from dataclasses import fields
from typing import cast
from unittest.mock import MagicMock

import pytest
from conjure_python_client import ServiceConfiguration

from nominal.core._clientsbunch import (
    ON_BEHALF_OF_USER_RID_HEADER,
    ClientsBunch,
    api_base_url_to_app_base_url,
)
from nominal.core.client import NominalClient
from nominal.core.exceptions import NominalConfigError
from nominal.experimental import as_user


def _make_clients_bunch(*, workspace_rid: str | None) -> ClientsBunch:
    workspace = MagicMock()
    services = {
        field.name: MagicMock(name=field.name)
        for field in fields(ClientsBunch)
        if field.init
        and field.name
        not in {
            "auth_header",
            "workspace_rid",
            "app_base_url",
            "header_provider",
            "_api_base_url",
            "_user_agent",
            "_token",
            "_service_config",
            "workspace",
        }
    }
    return ClientsBunch(
        auth_header="Bearer token",
        workspace_rid=workspace_rid,
        app_base_url="https://app.nominal.test",
        header_provider=None,
        _api_base_url="https://api.nominal.test",
        _user_agent="test-agent",
        _token="token",
        _service_config=ServiceConfiguration(uris=["https://api.nominal.test"]),
        workspace=workspace,
        **services,
    )


def _raw_workspace(rid: str) -> MagicMock:
    workspace = MagicMock()
    workspace.rid = rid
    workspace.id = rid.rsplit(".", 1)[-1]
    workspace.org = "test-org"
    return workspace


class _FakeSession:
    def __init__(self, headers: dict[str, str] | None = None) -> None:
        self.headers = {"User-Agent": "test-agent", **(headers or {})}


class _FakeCatalogService:
    def __init__(self, headers: dict[str, str] | None = None) -> None:
        self._requests_session = _FakeSession(headers)


class _FakeService:
    def __init__(self, headers: dict[str, str] | None = None) -> None:
        self._requests_session = _FakeSession(headers)


def _fake_create_conjure_client_factory(
    *,
    user_agent,
    service_config,
    return_none_for_unknown_union_types=False,
    header_provider=None,
):
    del user_agent, service_config, return_none_for_unknown_union_types
    headers = header_provider.headers() if header_provider is not None else None

    def factory(service_class):
        if service_class.__name__ == "CatalogService":
            return _FakeCatalogService(headers)
        return _FakeService(headers)

    return factory


def test_api_app_url_conversion():
    c = api_base_url_to_app_base_url
    assert c("https://api.gov.nominal.io/api") == "https://app.gov.nominal.io"
    assert c("https://api-staging.gov.nominal.io/api") == "https://app-staging.gov.nominal.io"
    assert c("https://api.nominal.test") == "https://app.nominal.test"
    assert c("https://api-customer.eu.nominal.io/api") == "https://app-customer.eu.nominal.io"
    assert c("https://api-customer.gov.nominal.io/api") == "https://app-customer.gov.nominal.io"
    assert c("https://api.nominal.gov.deployment.customer.com/api") == "https://app.nominal.gov.deployment.customer.com"
    assert c("https://api.nominal.customer.internal/api") == "https://app.nominal.customer.internal"
    assert c("https://unknown") == ""


def test_resolve_default_workspace_rid_returns_configured_workspace_rid_via_cached_workspace_lookup():
    """Pinned clients should resolve and cache their configured workspace before returning its RID."""
    configured_workspace_rid = "ri.workspace.main.workspace.configured"
    clients = _make_clients_bunch(workspace_rid=configured_workspace_rid)
    workspace_service = cast(MagicMock, clients.workspace)
    raw_workspace = _raw_workspace(configured_workspace_rid)
    workspace_service.get_workspace.return_value = raw_workspace

    assert clients.resolve_default_workspace_rid() == configured_workspace_rid
    assert clients.resolve_default_workspace_rid() == configured_workspace_rid

    workspace_service.get_workspace.assert_called_once_with("Bearer token", configured_workspace_rid)
    workspace_service.get_default_workspace.assert_not_called()


def test_resolve_default_workspace_rid_uses_workspace_service_once_and_caches_result():
    """An unconfigured client should resolve through the workspace service and cache the RID."""
    clients = _make_clients_bunch(workspace_rid=None)
    workspace_service = cast(MagicMock, clients.workspace)
    raw_workspace = _raw_workspace("ri.workspace.main.workspace.default")
    workspace_service.get_default_workspace.return_value = raw_workspace

    assert clients.resolve_default_workspace_rid() == raw_workspace.rid
    assert clients.resolve_default_workspace_rid() == raw_workspace.rid

    workspace_service.get_default_workspace.assert_called_once_with("Bearer token")


def test_resolve_default_workspace_rid_raises_when_workspace_service_cannot_resolve_default():
    """Missing service-side defaults should raise the same config error the client surfaces."""
    clients = _make_clients_bunch(workspace_rid=None)
    workspace_service = cast(MagicMock, clients.workspace)
    workspace_service.get_default_workspace.return_value = None

    with pytest.raises(NominalConfigError, match="Could not retrieve default workspace"):
        clients.resolve_default_workspace_rid()


def test_resolve_workspace_none_returns_configured_workspace_via_get_workspace_once():
    """Resolving the default workspace on a pinned client should fetch and cache that workspace object."""
    configured_workspace_rid = "ri.workspace.main.workspace.configured"
    clients = _make_clients_bunch(workspace_rid=configured_workspace_rid)
    workspace_service = cast(MagicMock, clients.workspace)
    raw_workspace = _raw_workspace(configured_workspace_rid)
    workspace_service.get_workspace.return_value = raw_workspace

    assert clients.resolve_workspace() == raw_workspace
    assert clients.resolve_workspace() == raw_workspace

    workspace_service.get_workspace.assert_called_once_with("Bearer token", configured_workspace_rid)
    workspace_service.get_default_workspace.assert_not_called()


def test_resolve_workspace_none_uses_default_workspace_endpoint_and_caches_the_result():
    """Resolving the default workspace on an unpinned client should reuse the cached workspace object."""
    clients = _make_clients_bunch(workspace_rid=None)
    workspace_service = cast(MagicMock, clients.workspace)
    raw_default_workspace = _raw_workspace("ri.workspace.main.workspace.default")
    workspace_service.get_default_workspace.return_value = raw_default_workspace

    assert clients.resolve_workspace() == raw_default_workspace
    assert clients.resolve_workspace() == raw_default_workspace

    workspace_service.get_default_workspace.assert_called_once_with("Bearer token")
    workspace_service.get_workspace.assert_not_called()


def test_resolve_default_workspace_rid_and_resolve_workspace_share_the_same_lazy_default():
    """RID and workspace-object resolution should share the same lazily initialized default workspace."""
    clients = _make_clients_bunch(workspace_rid=None)
    workspace_service = cast(MagicMock, clients.workspace)
    raw_default_workspace = _raw_workspace("ri.workspace.main.workspace.default")
    workspace_service.get_default_workspace.return_value = raw_default_workspace

    assert clients.resolve_default_workspace_rid() == raw_default_workspace.rid
    assert clients.resolve_workspace() == raw_default_workspace

    workspace_service.get_default_workspace.assert_called_once_with("Bearer token")
    workspace_service.get_workspace.assert_not_called()


def test_resolve_workspace_reuses_the_cached_default_workspace_object():
    """Explicit resolution of the cached default workspace RID should avoid a second workspace fetch."""
    clients = _make_clients_bunch(workspace_rid=None)
    workspace_service = cast(MagicMock, clients.workspace)
    raw_default_workspace = _raw_workspace("ri.workspace.main.workspace.default")
    workspace_service.get_default_workspace.return_value = raw_default_workspace

    assert clients.resolve_default_workspace_rid() == raw_default_workspace.rid
    assert clients.resolve_workspace(raw_default_workspace.rid) == raw_default_workspace

    workspace_service.get_default_workspace.assert_called_once_with("Bearer token")
    workspace_service.get_workspace.assert_not_called()


def test_resolve_workspace_reuses_the_cached_configured_default_workspace_object():
    """Pinned clients should also reuse their cached default workspace for later explicit RID lookups."""
    configured_workspace_rid = "ri.workspace.main.workspace.configured"
    clients = _make_clients_bunch(workspace_rid=configured_workspace_rid)
    workspace_service = cast(MagicMock, clients.workspace)
    raw_workspace = _raw_workspace(configured_workspace_rid)
    workspace_service.get_workspace.return_value = raw_workspace

    assert clients.resolve_workspace() == raw_workspace
    assert clients.resolve_workspace(configured_workspace_rid) == raw_workspace

    workspace_service.get_workspace.assert_called_once_with("Bearer token", configured_workspace_rid)
    workspace_service.get_default_workspace.assert_not_called()


def test_experimental_as_user_returns_derived_nominal_client(monkeypatch):
    monkeypatch.setattr("nominal.core._clientsbunch.create_conjure_client_factory", _fake_create_conjure_client_factory)

    client = NominalClient(
        _clients=ClientsBunch.from_config(
            ServiceConfiguration(uris=["https://api.nominal.test"]),
            "https://api.nominal.test",
            "test-agent",
            "token",
            None,
        )
    )

    impersonated = as_user(client, "ri.authn.dev.user.target")

    assert isinstance(impersonated, NominalClient)
    assert impersonated is not client
    assert ON_BEHALF_OF_USER_RID_HEADER not in client._clients.catalog._requests_session.headers
    assert impersonated._clients.catalog._requests_session.headers[ON_BEHALF_OF_USER_RID_HEADER] == (
        "ri.authn.dev.user.target"
    )
    assert impersonated._clients.assets._requests_session.headers[ON_BEHALF_OF_USER_RID_HEADER] == (
        "ri.authn.dev.user.target"
    )
