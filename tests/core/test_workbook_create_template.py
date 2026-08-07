"""Unit tests for Workbook.create_template: workspace resolution and the documented ValueErrors."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from nominal_api import scout_workbookcommon_api

from nominal.core.workbook import Workbook, WorkbookType
from nominal.core.workspace import Workspace

_CLIENT_WORKSPACE_RID = "ri.scout.cerulean-staging.workspace.client-default"
_EXPLICIT_WORKSPACE_RID = "ri.scout.cerulean-staging.workspace.explicit"


@pytest.fixture
def notebook(mock_clients):
    """The raw Notebook returned by NotebookService.get, with no charts so content passes through untouched.

    `content_v2` is None so the legacy `content` field is used. create_template requires content_v2 to be a
    real UnifiedWorkbookContent when it is present.
    """
    raw = MagicMock()
    raw.content_v2 = None
    raw.content = scout_workbookcommon_api.WorkbookContent(channel_variables={}, charts={})
    raw.metadata.title = "Flight 12 review"
    mock_clients.notebook.get.return_value = raw
    return raw


@pytest.fixture
def workbook(mock_clients, notebook):
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


def test_comparison_workbook_type_is_rejected(workbook, mock_clients):
    comparison = Workbook(
        rid=workbook.rid,
        title=workbook.title,
        description=workbook.description,
        workbook_type=WorkbookType.COMPARISON_WORKBOOK,
        run_rids=workbook.run_rids,
        asset_rids=None,
        _clients=mock_clients,
    )
    with pytest.raises(ValueError, match="Comparison workbook types not yet supported"):
        comparison.create_template()


def test_comparison_content_is_rejected_for_a_standard_workbook_type(workbook, notebook):
    """The rejection has two arms. This covers the content arm, which the workbook type check does not reach."""
    notebook.content_v2 = scout_workbookcommon_api.UnifiedWorkbookContent(
        workbook=None, comparison_workbook=MagicMock()
    )
    with pytest.raises(ValueError, match="Comparison workbook types not yet supported"):
        workbook.create_template()


def test_missing_content_is_rejected(workbook, notebook):
    notebook.content = None
    with pytest.raises(ValueError, match="Missing content for workbook"):
        workbook.create_template()
