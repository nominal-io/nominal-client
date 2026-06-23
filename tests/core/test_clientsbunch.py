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
from nominal.protos.workspaces.v1 import workspaces_pb2


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


def _ws(rid: str) -> workspaces_pb2.Workspace:
    return workspaces_pb2.Workspace(rid=rid, id=rid.rsplit(".", 1)[-1], org="test-org")


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
    configured = "ri.workspace.main.workspace.configured"
    clients = _make_clients_bunch(workspace_rid=configured)
    workspace_stub = cast(MagicMock, clients.workspace)
    workspace_stub.GetWorkspace.return_value = workspaces_pb2.GetWorkspaceResponse(workspace=_ws(configured))

    assert clients.resolve_default_workspace_rid() == configured
    assert clients.resolve_default_workspace_rid() == configured  # cached

    assert workspace_stub.GetWorkspace.call_count == 1
    request = workspace_stub.GetWorkspace.call_args.args[0]
    assert request.workspace_rid == configured
    workspace_stub.GetDefaultWorkspace.assert_not_called()


def test_resolve_default_workspace_rid_uses_workspace_service_once_and_caches_result():
    """An unconfigured client should resolve through the workspace service and cache the RID."""
    clients = _make_clients_bunch(workspace_rid=None)
    workspace_stub = cast(MagicMock, clients.workspace)
    default_rid = "ri.workspace.main.workspace.default"
    workspace_stub.GetDefaultWorkspace.return_value = workspaces_pb2.GetDefaultWorkspaceResponse(
        workspace=_ws(default_rid)
    )

    assert clients.resolve_default_workspace_rid() == default_rid
    assert clients.resolve_default_workspace_rid() == default_rid  # cached

    assert workspace_stub.GetDefaultWorkspace.call_count == 1
    workspace_stub.GetWorkspace.assert_not_called()


def test_resolve_default_workspace_rid_raises_when_workspace_service_cannot_resolve_default():
    """Missing service-side defaults should raise the same config error the client surfaces."""
    clients = _make_clients_bunch(workspace_rid=None)
    workspace_stub = cast(MagicMock, clients.workspace)
    workspace_stub.GetDefaultWorkspace.return_value = workspaces_pb2.GetDefaultWorkspaceResponse()  # no workspace set

    with pytest.raises(NominalConfigError, match="Could not retrieve default workspace"):
        clients.resolve_default_workspace_rid()


def test_resolve_workspace_none_returns_configured_workspace_via_get_workspace_once():
    """Resolving the default workspace on a pinned client should fetch and cache that workspace object."""
    configured = "ri.workspace.main.workspace.configured"
    clients = _make_clients_bunch(workspace_rid=configured)
    workspace_stub = cast(MagicMock, clients.workspace)
    ws = _ws(configured)
    workspace_stub.GetWorkspace.return_value = workspaces_pb2.GetWorkspaceResponse(workspace=ws)

    result1 = clients.resolve_workspace()
    result2 = clients.resolve_workspace()

    assert result1.rid == configured
    assert result2.rid == configured
    assert workspace_stub.GetWorkspace.call_count == 1
    request = workspace_stub.GetWorkspace.call_args.args[0]
    assert request.workspace_rid == configured
    workspace_stub.GetDefaultWorkspace.assert_not_called()


def test_resolve_workspace_none_uses_default_workspace_endpoint_and_caches_the_result():
    """Resolving the default workspace on an unpinned client should reuse the cached workspace object."""
    clients = _make_clients_bunch(workspace_rid=None)
    workspace_stub = cast(MagicMock, clients.workspace)
    default_rid = "ri.workspace.main.workspace.default"
    ws = _ws(default_rid)
    workspace_stub.GetDefaultWorkspace.return_value = workspaces_pb2.GetDefaultWorkspaceResponse(workspace=ws)

    result1 = clients.resolve_workspace()
    result2 = clients.resolve_workspace()

    assert result1.rid == default_rid
    assert result2.rid == default_rid
    assert workspace_stub.GetDefaultWorkspace.call_count == 1
    workspace_stub.GetWorkspace.assert_not_called()


def test_resolve_default_workspace_rid_and_resolve_workspace_share_the_same_lazy_default():
    """RID and workspace-object resolution should share the same lazily initialized default workspace."""
    clients = _make_clients_bunch(workspace_rid=None)
    workspace_stub = cast(MagicMock, clients.workspace)
    default_rid = "ri.workspace.main.workspace.default"
    ws = _ws(default_rid)
    workspace_stub.GetDefaultWorkspace.return_value = workspaces_pb2.GetDefaultWorkspaceResponse(workspace=ws)

    assert clients.resolve_default_workspace_rid() == default_rid
    result = clients.resolve_workspace()
    assert result.rid == default_rid

    assert workspace_stub.GetDefaultWorkspace.call_count == 1
    workspace_stub.GetWorkspace.assert_not_called()


def test_resolve_workspace_reuses_the_cached_default_workspace_object():
    """Explicit resolution of the cached default workspace RID should avoid a second workspace fetch."""
    clients = _make_clients_bunch(workspace_rid=None)
    workspace_stub = cast(MagicMock, clients.workspace)
    default_rid = "ri.workspace.main.workspace.default"
    ws = _ws(default_rid)
    workspace_stub.GetDefaultWorkspace.return_value = workspaces_pb2.GetDefaultWorkspaceResponse(workspace=ws)

    assert clients.resolve_default_workspace_rid() == default_rid
    result = clients.resolve_workspace(default_rid)
    assert result.rid == default_rid

    assert workspace_stub.GetDefaultWorkspace.call_count == 1
    workspace_stub.GetWorkspace.assert_not_called()


def test_resolve_workspace_reuses_the_cached_configured_default_workspace_object():
    """Pinned clients should also reuse their cached default workspace for later explicit RID lookups."""
    configured = "ri.workspace.main.workspace.configured"
    clients = _make_clients_bunch(workspace_rid=configured)
    workspace_stub = cast(MagicMock, clients.workspace)
    ws = _ws(configured)
    workspace_stub.GetWorkspace.return_value = workspaces_pb2.GetWorkspaceResponse(workspace=ws)

    result1 = clients.resolve_workspace()
    result2 = clients.resolve_workspace(configured)

    assert result1.rid == configured
    assert result2.rid == configured
    assert workspace_stub.GetWorkspace.call_count == 1
    workspace_stub.GetDefaultWorkspace.assert_not_called()


def test_from_config_wires_roles_via_the_grpc_factory(monkeypatch):
    """from_config builds `roles`, `comments`, `units`, and `workspace` through the gRPC stub factory."""
    monkeypatch.setattr("nominal.core._clientsbunch.create_conjure_client_factory", _fake_create_conjure_client_factory)
    roles_stub = object()
    grpc_factory = MagicMock(return_value=MagicMock(return_value=roles_stub))
    monkeypatch.setattr("nominal.core._clientsbunch.create_grpc_stub_factory", grpc_factory)

    clients = ClientsBunch.from_config(
        ServiceConfiguration(uris=["https://api.nominal.test"]),
        "https://api.nominal.test",
        "test-agent",
        "token",
        None,
    )

    assert clients.roles is roles_stub
    assert clients.comments is roles_stub
    assert clients.units is roles_stub
    assert clients.workspace is roles_stub
    assert grpc_factory.call_args.kwargs["auth_header"] == "Bearer token"
    assert grpc_factory.call_args.kwargs["api_base_url"] == "https://api.nominal.test"
    assert grpc_factory.call_args.kwargs["header_provider"] is None


def test_experimental_as_user_returns_derived_nominal_client(monkeypatch):
    """as_user returns a new client that injects the on-behalf-of header on both the HTTP and gRPC paths."""
    monkeypatch.setattr("nominal.core._clientsbunch.create_conjure_client_factory", _fake_create_conjure_client_factory)
    grpc_factory = MagicMock(return_value=MagicMock(return_value=MagicMock(name="roles")))
    monkeypatch.setattr("nominal.core._clientsbunch.create_grpc_stub_factory", grpc_factory)

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
    # The impersonation header_provider must also reach the gRPC stub factory; the most
    # recent factory call is the impersonated client's.
    header_provider = grpc_factory.call_args.kwargs["header_provider"]
    assert header_provider is not None
    assert header_provider.headers()[ON_BEHALF_OF_USER_RID_HEADER] == "ri.authn.dev.user.target"
