"""Unit tests for workspace resolution in Workbook.create_template."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from nominal_api import scout_workbookcommon_api

from nominal.core.workbook import Workbook, WorkbookType
from nominal.core.workspace import Workspace

_CLIENT_WORKSPACE_RID = "ri.scout.cerulean-staging.workspace.client-default"
_EXPLICIT_WORKSPACE_RID = "ri.scout.cerulean-staging.workspace.explicit"


@pytest.fixture
def workbook(mock_clients):
    """A workbook whose raw notebook has no charts, so the content passes through untouched.

    `content_v2` is None so the legacy `content` field is used. create_template requires content_v2 to be a
    real UnifiedWorkbookContent when it is present.
    """
    notebook = MagicMock()
    notebook.content_v2 = None
    notebook.content = scout_workbookcommon_api.WorkbookContent(channel_variables={}, charts={})
    notebook.metadata.title = "Flight 12 review"
    mock_clients.notebook.get.return_value = notebook
    mock_clients.resolve_default_workspace_rid.return_value = _CLIENT_WORKSPACE_RID
    return Workbook(
        rid="ri.scout.cerulean-staging.notebook.abc123",
        title="Flight 12 review",
        description="post-flight",
        workbook_type=WorkbookType.WORKBOOK,
        run_rids=["ri.scout.cerulean-staging.run.def456"],
        asset_rids=None,
        _clients=mock_clients,
    )


@pytest.mark.parametrize(
    ("workspace", "expected_rid"),
    [
        (None, _CLIENT_WORKSPACE_RID),
        (_EXPLICIT_WORKSPACE_RID, _EXPLICIT_WORKSPACE_RID),
        (Workspace(rid=_EXPLICIT_WORKSPACE_RID, id="ws-1", org="org-1"), _EXPLICIT_WORKSPACE_RID),
    ],
    ids=["defaults to the client workspace", "accepts a rid", "accepts an instance"],
)
def test_workspace_resolution(workbook, mock_clients, workspace, expected_rid):
    workbook.create_template(workspace=workspace)
    request = mock_clients.template.create.call_args.args[1]
    assert request.workspace == expected_rid


def test_deprecated_alias_maps_workspace_rid_onto_workspace(workbook, mock_clients):
    """The alias keeps the old `workspace_rid` name, so the rename across the boundary must hold."""
    with pytest.warns(UserWarning):
        workbook._create_template_from_workbook(workspace_rid=_EXPLICIT_WORKSPACE_RID)
    request = mock_clients.template.create.call_args.args[1]
    assert request.workspace == _EXPLICIT_WORKSPACE_RID
