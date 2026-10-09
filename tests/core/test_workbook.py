from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from conjure_python_client import ConjureEncoder
from nominal_api import scout, scout_layout_api, scout_notebook_api, scout_workbookcommon_api

from nominal.core.workbook import Workbook, WorkbookType


def _metadata(
    data_scope: scout_notebook_api.NotebookDataScope,
    *,
    title: str = "Server title",
) -> scout_notebook_api.NotebookMetadata:
    return scout_notebook_api.NotebookMetadata(
        created_at="2026-10-07T00:00:00Z",
        created_by_rid="creator-rid",
        data_scope=data_scope,
        description="Latest description",
        is_archived=False,
        is_draft=False,
        labels=["flight"],
        lock=scout_notebook_api.Lock(is_locked=False),
        notebook_type=scout_notebook_api.NotebookType.WORKBOOK,
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
    mock_clients.notebook.duplicate.return_value = _notebook(
        _metadata(scout_notebook_api.NotebookDataScope(run_rids=[]))
    )
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


def test_clone_leaves_inherited_metadata_to_server(workbook: Workbook, mock_clients: MagicMock) -> None:
    """Cached fields must not replace the server's latest title, description, or scope."""
    mock_clients.notebook.duplicate.return_value = _notebook(
        _metadata(
            scout_notebook_api.NotebookDataScope(asset_rids=["latest-asset-rid"]),
            title="Latest flight review - copy",
        )
    )

    result = workbook.clone()

    request = mock_clients.notebook.duplicate.call_args.args[1]
    assert request.title is None
    assert request.description is None
    assert request.data_scope is None
    assert request.labels is None
    assert request.properties is None
    assert request.is_draft is False
    assert request.is_locked is False
    assert result.title == "Latest flight review - copy"
    assert result.description == "Latest description"
    assert result.run_rids is None
    assert result.asset_rids == ["latest-asset-rid"]
    assert result is not workbook
    assert workbook.title == "Flight review"
    assert workbook.run_rids == ["cached-run-rid"]
    mock_clients.notebook.create.assert_not_called()


def test_clone_preserves_empty_metadata_overrides(workbook: Workbook, mock_clients: MagicMock) -> None:
    """Empty values clear inherited metadata; None can still inherit the draft status."""
    workbook.clone("", "", labels=[], properties={}, is_draft=None)

    request = mock_clients.notebook.duplicate.call_args.args[1]
    assert request.title == ""
    assert request.description == ""
    assert request.labels == []
    assert request.properties == {}
    assert request.is_draft is None


@pytest.mark.parametrize("scope_type", ["runs", "assets"])
@pytest.mark.parametrize("rids", [["replacement-rid"], []])
def test_clone_replaces_or_clears_selected_scope(
    workbook: Workbook, mock_clients: MagicMock, scope_type: str, rids: list[str]
) -> None:
    """Selecting an empty scope must clear it instead of inheriting the source scope."""
    if scope_type == "runs":
        workbook.clone(runs=rids)
        expected_scope = {"type": "runRids", "runRids": rids}
    else:
        workbook.clone(assets=rids)
        expected_scope = {"type": "assetRids", "assetRids": rids}

    request = mock_clients.notebook.duplicate.call_args.args[1]
    assert ConjureEncoder().default(request)["dataScope"] == expected_scope


@pytest.mark.parametrize(("runs", "assets"), [(["run-rid"], ["asset-rid"]), ([], [])])
def test_clone_rejects_combined_data_scopes(
    workbook: Workbook, mock_clients: MagicMock, runs: list[str], assets: list[str]
) -> None:
    """Supplying both data scope arms fails before any API calls, even for empty lists."""
    with pytest.raises(ValueError, match="Only one of `runs` and `assets`"):
        workbook.clone(runs=runs, assets=assets)  # type: ignore[call-overload]

    mock_clients.notebook.duplicate.assert_not_called()
    mock_clients.resolve_default_workspace_rid.assert_not_called()


def test_update_applies_server_metadata_in_place(workbook: Workbook, mock_clients: MagicMock) -> None:
    """The update response is authoritative, including fields the caller did not change."""
    mock_clients.notebook.update_metadata.return_value = _metadata(
        scout_notebook_api.NotebookDataScope(asset_rids=["latest-asset-rid"]), title="Server accepted title"
    )

    result = workbook.update(title="Requested title")

    assert result is workbook
    assert workbook.title == "Server accepted title"
    assert workbook.description == "Latest description"
    assert workbook.run_rids is None
    assert workbook.asset_rids == ["latest-asset-rid"]
    assert workbook.created_by_rid == "creator-rid"
