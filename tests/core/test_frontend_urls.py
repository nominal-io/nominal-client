from unittest.mock import MagicMock

import pytest

from nominal.core._utils.frontend_urls import asset_url

_APP_BASE_URL = "https://app.nominal.test"
_WORKSPACE_RID = "ri.workspace.main.workspace.test"


@pytest.fixture
def clients() -> MagicMock:
    clients = MagicMock()
    clients.app_base_url = _APP_BASE_URL
    clients.resolve_default_workspace_rid.return_value = _WORKSPACE_RID
    return clients


@pytest.mark.parametrize("base_url", [_APP_BASE_URL, f"{_APP_BASE_URL}/"])
def test_resource_urls_are_workspace_scoped_without_double_slashes(clients, base_url):
    """Callers pass leading-slash paths, and app_base_url may carry a trailing slash."""
    clients.app_base_url = base_url

    assert asset_url(clients, "ri.asset.test") == f"{_APP_BASE_URL}/w/{_WORKSPACE_RID}/assets/ri.asset.test"
