from __future__ import annotations

import pytest

from nominal.config import NominalConfig
from nominal.core import NominalClient, WorkspaceSearchType
from nominal.core.exceptions import NominalConfigError


def _client_with_workspace_override(client: NominalClient, pytestconfig, *, workspace_rid: str | None) -> NominalClient:
    """Reuse the session client when possible, otherwise rebuild it with a different workspace pin."""
    if client._clients.workspace_rid == workspace_rid:
        return client

    if client._profile is not None:
        prof = NominalConfig.from_yaml().get_profile(client._profile)
        return NominalClient.from_token(prof.token, prof.base_url, workspace_rid=workspace_rid)

    auth_token = client._clients.auth_header.removeprefix("Bearer ")
    if auth_token:
        base_url = pytestconfig.getoption("base_url")
        return NominalClient.from_token(auth_token, base_url, workspace_rid=workspace_rid)

    raise pytest.UsageError("Either --profile or --auth-token must be provided")


def _get_service_default_workspace_rid(client: NominalClient) -> str:
    try:
        return client.get_workspace().rid
    except NominalConfigError as exc:
        pytest.skip(f"Tenant does not expose a default workspace: {exc}")


def test_workspace_rid_for_search_returns_workspace_object_rid(client: NominalClient) -> None:
    """A live Workspace object should resolve back to its RID without altering search behavior."""
    workspace = client.get_workspace()

    assert client._workspace_rid_for_search(workspace) == workspace.rid


def test_workspace_rid_for_search_returns_none_for_all(client: NominalClient) -> None:
    """A live client should still translate ALL into an omitted workspace RID."""
    assert client._workspace_rid_for_search(WorkspaceSearchType.ALL) is None


def test_unconfigured_client_uses_workspace_service_default(client: NominalClient, pytestconfig) -> None:
    """Without a pinned workspace, DEFAULT should resolve to the tenant's service-side default."""
    with pytest.warns(UserWarning, match="NominalClient will soon require a workspace RID"):
        unconfigured_client = _client_with_workspace_override(client, pytestconfig, workspace_rid=None)
    expected_workspace_rid = _get_service_default_workspace_rid(unconfigured_client)

    assert unconfigured_client._clients.resolve_default_workspace_rid() == expected_workspace_rid
    assert unconfigured_client._workspace_rid_for_search(WorkspaceSearchType.DEFAULT) == expected_workspace_rid


def test_configured_workspace_rid_takes_precedence_over_service_default(client: NominalClient, pytestconfig) -> None:
    """A pinned workspace RID should win over the tenant default exposed by the workspace service."""
    with pytest.warns(UserWarning, match="NominalClient will soon require a workspace RID"):
        unconfigured_client = _client_with_workspace_override(client, pytestconfig, workspace_rid=None)
    service_default_workspace_rid = _get_service_default_workspace_rid(unconfigured_client)
    configured_workspace = next(
        (
            workspace
            for workspace in unconfigured_client.list_workspaces()
            if workspace.rid != service_default_workspace_rid
        ),
        None,
    )

    if configured_workspace is None:
        pytest.skip("Need at least two visible workspaces to distinguish configured workspace precedence")

    configured_client = _client_with_workspace_override(client, pytestconfig, workspace_rid=configured_workspace.rid)

    assert configured_client._clients.resolve_default_workspace_rid() == configured_workspace.rid
    assert configured_client._workspace_rid_for_search(WorkspaceSearchType.DEFAULT) == configured_workspace.rid
