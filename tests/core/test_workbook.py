from __future__ import annotations

from dataclasses import replace
from typing import Any
from unittest.mock import MagicMock

import pytest
from conjure_python_client import ConjureEncoder
from nominal_api import scout, scout_layout_api, scout_notebook_api, scout_workbookcommon_api

from nominal.core.asset import Asset
from nominal.core.run import Run
from nominal.core.workbook import Workbook, WorkbookType
from nominal.core.workspace import Workspace


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


def _notebook(metadata: scout_notebook_api.NotebookMetadata) -> scout_notebook_api.Notebook:
    return scout_notebook_api.Notebook(
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
    """Clone uses server metadata and one duplication request across supported workbook scopes."""
    workbook = replace(workbook, workbook_type=WorkbookType._from_conjure(notebook_type))
    metadata = _metadata(
        data_scope,
        notebook_type,
        title="Latest flight review - copy" if title is None else title,
        description="Latest description" if description is None else description,
    )
    duplicated = _notebook(metadata)
    mock_clients.notebook.duplicate.return_value = duplicated

    result = workbook.clone(title, description)

    mock_clients.notebook.duplicate.assert_called_once()
    auth_header, request, rid = mock_clients.notebook.duplicate.call_args.args
    assert auth_header == mock_clients.auth_header
    assert rid == workbook.rid
    assert ConjureEncoder().default(request) == {
        "workspace": "workspace-rid",
        "title": title,
        "titleSuffix": None,
        "description": description,
        "dataScope": None,
        "isDraft": False,
        "isLocked": False,
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


@pytest.mark.parametrize(
    ("changes", "expected_fields"),
    [
        (
            {"labels": ("new-label",), "properties": {"campaign": "new"}, "is_draft": True, "is_locked": True},
            {"labels": ["new-label"], "properties": {"campaign": "new"}, "isDraft": True, "isLocked": True},
        ),
        ({"labels": [], "properties": {}, "is_draft": False}, {"labels": [], "properties": {}, "isDraft": False}),
        ({"is_draft": None}, {"isDraft": None}),
        ({"workspace": "destination-rid"}, {"workspace": "destination-rid"}),
        (
            {"workspace": Workspace(rid="destination-rid", id="destination-id", org="org-rid")},
            {"workspace": "destination-rid"},
        ),
    ],
)
def test_clone_metadata_overrides(
    workbook: Workbook, mock_clients: MagicMock, changes: dict[str, Any], expected_fields: dict[str, Any]
) -> None:
    """Metadata overrides preserve explicit clears, false flags, and workspace selection."""
    mock_clients.notebook.duplicate.return_value = _notebook(
        _metadata(scout_notebook_api.NotebookDataScope(run_rids=[]))
    )

    workbook.clone(**changes)

    mock_clients.notebook.duplicate.assert_called_once()
    request = ConjureEncoder().default(mock_clients.notebook.duplicate.call_args.args[1])
    assert {name: request[name] for name in expected_fields} == expected_fields
    if "workspace" in changes:
        mock_clients.resolve_default_workspace_rid.assert_not_called()


@pytest.mark.parametrize(
    ("changes", "expected_scope"),
    [
        ({"runs": ["run-a", "run-b"]}, {"type": "runRids", "runRids": ["run-a", "run-b"]}),
        ({"assets": ("asset-rid",)}, {"type": "assetRids", "assetRids": ["asset-rid"]}),
        ({"runs": []}, {"type": "runRids", "runRids": []}),
        ({"assets": []}, {"type": "assetRids", "assetRids": []}),
    ],
)
def test_clone_data_scope_overrides(
    workbook: Workbook, mock_clients: MagicMock, changes: dict[str, Any], expected_scope: dict[str, Any]
) -> None:
    """Run and asset overrides serialize the correct union arm, including empty scopes."""
    mock_clients.notebook.duplicate.return_value = _notebook(
        _metadata(scout_notebook_api.NotebookDataScope(run_rids=[]))
    )

    workbook.clone(**changes)

    request = mock_clients.notebook.duplicate.call_args.args[1]
    assert ConjureEncoder().default(request)["dataScope"] == expected_scope


@pytest.mark.parametrize(("runs", "assets"), [(["run-rid"], ["asset-rid"]), ([], [])])
def test_clone_rejects_combined_data_scopes(
    workbook: Workbook, mock_clients: MagicMock, runs: list[str], assets: list[str]
) -> None:
    """Supplying both data scope arms fails before any API calls, even for empty lists."""
    with pytest.raises(ValueError, match="Only one of `runs` and `assets`"):
        workbook.clone(runs=runs, assets=assets)

    mock_clients.notebook.duplicate.assert_not_called()
    mock_clients.resolve_default_workspace_rid.assert_not_called()


@pytest.mark.parametrize("scope_type", ["runs", "assets"])
def test_clone_accepts_data_scope_resources(workbook: Workbook, mock_clients: MagicMock, scope_type: str) -> None:
    """Data scopes accept SDK resources alongside RID strings."""
    mock_clients.notebook.duplicate.return_value = _notebook(
        _metadata(scout_notebook_api.NotebookDataScope(run_rids=[]))
    )
    if scope_type == "runs":
        run = Run(
            rid="run-rid",
            name="Run",
            description="",
            properties={},
            labels=[],
            links=[],
            start=0,
            end=1,
            run_number=1,
            assets=[],
            created_at=0,
            is_archived=False,
            _clients=mock_clients,
        )
        workbook.clone(runs=[run, "other-run-rid"])
        expected_scope = {"type": "runRids", "runRids": ["run-rid", "other-run-rid"]}
    else:
        asset = Asset(
            rid="asset-rid",
            name="Asset",
            description="",
            properties={},
            labels=[],
            created_at=0,
            is_archived=False,
            _clients=mock_clients,
        )
        workbook.clone(assets=[asset, "other-asset-rid"])
        expected_scope = {"type": "assetRids", "assetRids": ["asset-rid", "other-asset-rid"]}

    request = mock_clients.notebook.duplicate.call_args.args[1]
    assert ConjureEncoder().default(request)["dataScope"] == expected_scope


@pytest.mark.parametrize("title_suffix", ["Run analysis", ""])
@pytest.mark.parametrize(("title", "expected_title"), [(None, None), ("Explicit title", "Explicit title"), ("", "")])
def test_clone_title_suffix_uses_server_naming(
    workbook: Workbook, mock_clients: MagicMock, title: str | None, expected_title: str | None, title_suffix: str
) -> None:
    """A title suffix delegates naming to the backend unless an explicit title is provided."""
    mock_clients.notebook.duplicate.return_value = _notebook(
        _metadata(scout_notebook_api.NotebookDataScope(run_rids=[]))
    )

    workbook.clone(title=title, title_suffix=title_suffix)

    request = mock_clients.notebook.duplicate.call_args.args[1]
    assert request.title == expected_title
    assert request.title_suffix == title_suffix


def test_refresh_from_preserves_identity(workbook: Workbook) -> None:
    """Refresh from a constructed resource copies its fields into the same frozen instance."""
    updated = replace(
        workbook, title="Updated title", run_rids=None, asset_rids=["asset-rid"], created_by_rid="creator-rid"
    )

    result = workbook._refresh_from(updated)

    assert result is workbook
    assert workbook == updated
    assert workbook is not updated


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
    """Metadata updates refresh every exposed field in place without fetching workbook content."""
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
