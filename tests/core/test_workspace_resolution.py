from unittest.mock import MagicMock, patch

import pytest

from nominal.core.client import NominalClient, WorkspaceSearchType
from nominal.core.exceptions import NominalConfigError
from nominal.core.workspace import Workspace


def _make_client() -> tuple[NominalClient, MagicMock]:
    clients = MagicMock()
    clients.auth_header = "Bearer token"
    clients.resolve_default_workspace_rid = MagicMock()
    clients.resolve_workspace = MagicMock()
    clients.workspace = MagicMock()
    return NominalClient(_clients=clients), clients


def _raw_workspace(rid: str) -> MagicMock:
    workspace = MagicMock()
    workspace.rid = rid
    workspace.id = rid.rsplit(".", 1)[-1]
    workspace.org = "test-org"
    return workspace


def test_workspace_rid_for_search_returns_workspace_object_rid_immediately() -> None:
    """Passing a Workspace object should return its RID without any further client lookups."""
    client, clients = _make_client()
    workspace = Workspace(rid="ri.workspace.main.workspace.manual", id="manual", org="test-org")

    assert client._workspace_rid_for_search(workspace) == workspace.rid

    clients.workspace.get_workspace.assert_not_called()
    clients.resolve_default_workspace_rid.assert_not_called()


def test_workspace_rid_for_search_validates_string_workspace_rid() -> None:
    """String workspace selectors should still be validated through the workspace service."""
    client, clients = _make_client()
    workspace_rid = "ri.workspace.main.workspace.manual"
    clients.resolve_workspace.return_value = _raw_workspace(workspace_rid)

    assert client._workspace_rid_for_search(workspace_rid) == workspace_rid

    clients.resolve_workspace.assert_called_once_with(workspace_rid)
    clients.resolve_default_workspace_rid.assert_not_called()


def test_workspace_rid_for_search_returns_none_for_all() -> None:
    """WorkspaceSearchType.ALL should continue to suppress the workspace RID filter entirely."""
    client, clients = _make_client()

    assert client._workspace_rid_for_search(WorkspaceSearchType.ALL) is None

    clients.workspace.get_workspace.assert_not_called()
    clients.resolve_default_workspace_rid.assert_not_called()


def test_workspace_rid_for_search_uses_clientsbunch_default_resolution_for_default() -> None:
    """WorkspaceSearchType.DEFAULT should delegate to ClientsBunch default resolution."""
    client, clients = _make_client()
    clients.resolve_default_workspace_rid.return_value = "ri.workspace.main.workspace.default"

    assert (
        client._workspace_rid_for_search(WorkspaceSearchType.DEFAULT)
        == clients.resolve_default_workspace_rid.return_value
    )

    clients.workspace.get_workspace.assert_not_called()
    clients.resolve_default_workspace_rid.assert_called_once_with()


def test_workspace_rid_for_search_rewrites_default_resolution_error() -> None:
    """DEFAULT resolution failures should be rewritten into the user-facing search error message."""
    client, clients = _make_client()
    clients.resolve_default_workspace_rid.side_effect = NominalConfigError("no default workspace")

    with pytest.raises(NominalConfigError, match="WorkspaceSearchType.DEFAULT provided"):
        client._workspace_rid_for_search(WorkspaceSearchType.DEFAULT)


def test_get_workspace_uses_clientsbunch_default_resolution_when_no_rid_is_provided() -> None:
    """get_workspace() with no RID should route through the shared workspace resolver with a None selector."""
    client, clients = _make_client()
    workspace_rid = "ri.workspace.main.workspace.default"
    raw_workspace = _raw_workspace(workspace_rid)
    clients.resolve_workspace.return_value = raw_workspace

    workspace = client.get_workspace()

    assert workspace.rid == workspace_rid
    clients.resolve_workspace.assert_called_once_with(None)
    clients.resolve_default_workspace_rid.assert_not_called()


def test_get_workspace_resolves_explicit_workspace_rids_through_clientsbunch() -> None:
    """get_workspace(rid) should use the shared workspace resolver so cached defaults can short-circuit lookups."""
    client, clients = _make_client()
    workspace_rid = "ri.workspace.main.workspace.manual"
    clients.resolve_workspace.return_value = _raw_workspace(workspace_rid)

    workspace = client.get_workspace(workspace_rid)

    assert workspace.rid == workspace_rid
    clients.resolve_workspace.assert_called_once_with(workspace_rid)


def test_get_or_create_asset_by_properties_uses_default_workspace_resolution_for_search() -> None:
    """get_or_create_asset_by_properties should search with DEFAULT rather than a pre-resolved workspace RID."""
    client, _ = _make_client()
    properties = {"k": "v"}
    asset = MagicMock()

    with (
        patch.object(NominalClient, "search_assets", return_value=[]) as search_assets,
        patch.object(NominalClient, "create_asset", return_value=asset) as create_asset,
    ):
        assert client.get_or_create_asset_by_properties(properties, name="asset-name") is asset

    search_assets.assert_called_once_with(properties=properties, workspace=WorkspaceSearchType.DEFAULT)
    create_asset.assert_called_once_with(name="asset-name", description=None, properties=properties, labels=())
