from __future__ import annotations

from dataclasses import replace
from typing import Any
from unittest.mock import MagicMock

import pytest
from conjure_python_client import ConjureEncoder
from nominal_api import scout, scout_layout_api, scout_notebook_api, scout_workbookcommon_api

from nominal.core.workbook import Workbook, WorkbookType


def _metadata(
    data_scope: scout_notebook_api.NotebookDataScope,
    notebook_type: scout_notebook_api.NotebookType = scout_notebook_api.NotebookType.WORKBOOK,
    *,
    title: str = "Server title",
    description: str = "Latest description",
) -> scout_notebook_api.NotebookMetadata:
    return scout_notebook_api.NotebookMetadata(
        created_at="2026-10-07T00:00:00Z",
        created_by_rid="creator-rid",
        data_scope=data_scope,
        description=description,
        is_archived=False,
        is_draft=False,
        labels=["flight"],
        lock=scout_notebook_api.Lock(is_locked=False),
        notebook_type=notebook_type,
        properties={"campaign": "October"},
        title=title,
    )


@pytest.fixture
def workbook(mock_clients: MagicMock) -> Workbook:
    mock_clients.notebook = MagicMock(spec=scout.NotebookService)
    mock_clients.notebook.get.side_effect = AssertionError(
        "Should use the route response without fetching the workbook"
    )
    mock_clients.resolve_default_workspace_rid.return_value = "workspace-rid"
    return Workbook(
        rid="source-workbook-rid",
        title="Flight review",
        description="Cached description",
        workbook_type=WorkbookType.WORKBOOK,
        run_rids=["cached-run-rid"],
        asset_rids=None,
        _clients=mock_clients,
    )


@pytest.mark.parametrize(
    ("title", "description"),
    [(None, None), ("New title", "New description"), ("", "")],
)
@pytest.mark.parametrize(
    ("data_scope", "notebook_type"),
    [
        (scout_notebook_api.NotebookDataScope(run_rids=["latest-run-rid"]), scout_notebook_api.NotebookType.WORKBOOK),
        (scout_notebook_api.NotebookDataScope(asset_rids=["asset-rid"]), scout_notebook_api.NotebookType.WORKBOOK),
        (
            scout_notebook_api.NotebookDataScope(run_rids=["run-a", "run-b"]),
            scout_notebook_api.NotebookType.COMPARISON_WORKBOOK,
        ),
        (scout_notebook_api.NotebookDataScope(run_rids=[]), scout_notebook_api.NotebookType.COMPARISON_WORKBOOK),
    ],
)
def test_clone_duplicates_latest_workbook(
    workbook: Workbook,
    mock_clients: MagicMock,
    title: str | None,
    description: str | None,
    data_scope: scout_notebook_api.NotebookDataScope,
    notebook_type: scout_notebook_api.NotebookType,
) -> None:
    workbook = replace(workbook, workbook_type=WorkbookType._from_conjure(notebook_type))
    metadata = _metadata(
        data_scope,
        notebook_type,
        title="Workbook clone from 'Flight review'" if title is None else title,
        description="Latest description" if description is None else description,
    )
    duplicated = scout_notebook_api.Notebook(
        content_v2=scout_workbookcommon_api.UnifiedWorkbookContent(
            workbook=scout_workbookcommon_api.WorkbookContent(channel_variables={}, charts={})
        ),
        event_refs=[],
        layout=scout_layout_api.WorkbookLayout(
            v1=scout_layout_api.WorkbookLayoutV1(
                root_panel=scout_layout_api.Panel(
                    tabbed=scout_layout_api.TabbedPanel(v1=scout_layout_api.TabbedPanelV1(id="root", tabs=[]))
                )
            )
        ),
        metadata=metadata,
        rid="duplicated-workbook-rid",
        snapshot_author_rid="creator-rid",
        snapshot_created_at="2026-10-07T00:00:00Z",
        snapshot_rid="snapshot-rid",
        state_as_json="{}",
    )
    mock_clients.notebook.duplicate.return_value = duplicated

    result = workbook.clone(title, description)

    mock_clients.notebook.duplicate.assert_called_once()
    auth_header, request, rid = mock_clients.notebook.duplicate.call_args.args
    assert auth_header == mock_clients.auth_header
    assert rid == workbook.rid
    assert ConjureEncoder().default(request) == {
        "workspace": "workspace-rid",
        "title": metadata.title,
        "titleSuffix": None,
        "description": description,
        "dataScope": None,
        "isDraft": False,
        "isLocked": None,
        "labels": None,
        "properties": None,
        "previewImage": None,
    }
    mock_clients.resolve_default_workspace_rid.assert_called_once_with()
    mock_clients.notebook.get.assert_not_called()
    mock_clients.notebook.create.assert_not_called()
    assert result == Workbook._from_conjure(mock_clients, duplicated)
    assert result is not workbook
    assert result._clients is mock_clients
    assert workbook.rid == "source-workbook-rid"
    assert workbook.description == "Cached description"
    assert workbook.run_rids == ["cached-run-rid"]


def test_clone_propagates_duplicate_error(workbook: Workbook, mock_clients: MagicMock) -> None:
    error = RuntimeError("Duplication failed")
    mock_clients.notebook.duplicate.side_effect = error

    with pytest.raises(RuntimeError) as caught:
        workbook.clone()

    assert caught.value is error
    mock_clients.notebook.get.assert_not_called()
    mock_clients.notebook.create.assert_not_called()


@pytest.mark.parametrize(
    "changes",
    [
        {},
        {"title": "", "description": "", "labels": [], "properties": {}, "is_draft": False},
        {
            "title": "New title",
            "description": "New description",
            "labels": ("flight",),
            "properties": {"campaign": "October"},
            "is_draft": True,
        },
    ],
)
def test_update_refreshes_in_place_from_returned_metadata(
    workbook: Workbook, mock_clients: MagicMock, changes: dict[str, Any]
) -> None:
    metadata = _metadata(scout_notebook_api.NotebookDataScope(asset_rids=["latest-asset-rid"]))
    mock_clients.notebook.update_metadata.return_value = metadata
    expected = Workbook._from_notebook_metadata(
        mock_clients, scout_notebook_api.NotebookMetadataWithRid(metadata=metadata, rid=workbook.rid)
    )

    result = workbook.update(**changes)

    assert result is workbook
    assert workbook == expected
    assert workbook._clients is mock_clients
    mock_clients.notebook.get.assert_not_called()
    mock_clients.notebook.update_metadata.assert_called_once()
    auth_header, request, rid = mock_clients.notebook.update_metadata.call_args.args
    assert auth_header == mock_clients.auth_header
    assert rid == workbook.rid
    assert request.title == changes.get("title")
    assert request.description == changes.get("description")
    assert request.labels == (list(changes["labels"]) if "labels" in changes else None)
    assert request.properties == changes.get("properties")
    assert request.is_draft == changes.get("is_draft")
