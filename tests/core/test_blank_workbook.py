from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from nominal_api import scout_notebook_api

from nominal.core.asset import Asset
from nominal.core.client import NominalClient
from nominal.core.run import Run
from nominal.core.workbook import WorkbookType, _workbook_title
from nominal.core.workspace import Workspace
from nominal.protos.asset.v2 import asset_pb2
from tests.core._workbook_fixtures import notebook_response


def _echo_created_notebook(
    _auth: str, request: scout_notebook_api.CreateNotebookRequest
) -> scout_notebook_api.Notebook:
    """Return the notebook the server would create for the request."""
    return notebook_response(
        scout_notebook_api.NotebookMetadata(
            created_at="2026-11-04T00:00:00Z",
            created_by_rid="creator-rid",
            data_scope=request.data_scope,
            description=request.description,
            is_archived=False,
            is_draft=request.is_draft,
            labels=request.labels or [],
            lock=scout_notebook_api.Lock(is_locked=False),
            notebook_type=scout_notebook_api.NotebookType.WORKBOOK,
            properties=request.properties or {},
            title=request.title,
        ),
        rid="created-workbook-rid",
    )


@pytest.fixture
def clients(mock_clients: MagicMock) -> MagicMock:
    mock_clients.notebook.create.side_effect = _echo_created_notebook
    mock_clients.resolve_default_workspace_rid.return_value = "default-workspace-rid"
    return mock_clients


def _run(clients: MagicMock, rid: str = "run-rid-1", name: str = "Spin-up 1") -> Run:
    return Run(
        rid=rid,
        name=name,
        description="",
        properties={},
        labels=[],
        links=[],
        start=0,
        end=1,
        run_number=1,
        assets=["asset-rid-1"],
        created_at=0,
        is_archived=False,
        _clients=clients,
    )


def _asset(clients: MagicMock, rid: str = "asset-rid-1", name: str = "Rotor rig") -> Asset:
    return Asset(
        rid=rid,
        name=name,
        description=None,
        properties={},
        labels=[],
        created_at=0,
        is_archived=False,
        _clients=clients,
    )


def _request(clients: MagicMock) -> scout_notebook_api.CreateNotebookRequest:
    clients.notebook.create.assert_called_once()
    return clients.notebook.create.call_args.args[1]


def test_run_creates_a_blank_workbook_scoped_to_itself(clients: MagicMock) -> None:
    """The request has no panels, the run as its only scope, the default title and workspace, and no metadata."""
    workbook = _run(clients).create_workbook()

    request = _request(clients)
    assert request.data_scope.run_rids == ["run-rid-1"]
    assert request.data_scope.asset_rids is None
    assert request.title == "Workbook for Spin-up 1"
    assert request.description == ""
    assert request.labels is None
    assert request.properties is None
    assert request.is_draft is False
    assert request.workspace == "default-workspace-rid"
    assert request.state_as_json == "{}"
    assert request.event_refs == []
    assert request.layout.v1.root_panel.tabbed.v1.tabs == []
    content = request.content_v2.workbook
    assert content.charts == {}
    assert content.channel_variables == {}
    assert workbook.rid == "created-workbook-rid"
    assert workbook.run_rids == ["run-rid-1"]
    assert workbook.workbook_type == WorkbookType.WORKBOOK


def test_asset_creates_a_blank_workbook_scoped_to_itself(clients: MagicMock) -> None:
    """An asset scopes the workbook through asset RIDs only."""
    workbook = _asset(clients).create_workbook()

    request = _request(clients)
    assert request.data_scope.asset_rids == ["asset-rid-1"]
    assert request.data_scope.run_rids is None
    assert request.title == "Workbook for Rotor rig"
    assert workbook.asset_rids == ["asset-rid-1"]


def test_explicit_options_reach_the_request(clients: MagicMock) -> None:
    """Every option is sent as given, and a Workspace object is sent as its RID."""
    workspace = Workspace(rid="other-workspace-rid", id="other", org="org-rid")

    _run(clients).create_workbook(
        title="Spin-up review",
        description="First look",
        labels=["review"],
        properties={"campaign": "rtr"},
        is_draft=True,
        workspace=workspace,
    )

    request = _request(clients)
    assert request.title == "Spin-up review"
    assert request.description == "First look"
    assert request.labels == ["review"]
    assert request.properties == {"campaign": "rtr"}
    assert request.is_draft is True
    assert request.workspace == "other-workspace-rid"
    clients.resolve_default_workspace_rid.assert_not_called()


def test_each_workbook_gets_its_own_root_panel_id(clients: MagicMock) -> None:
    """Root panel IDs are generated per workbook, not shared."""
    run = _run(clients)
    run.create_workbook()
    run.create_workbook()

    first, second = (call.args[1] for call in clients.notebook.create.call_args_list)
    assert first.layout.v1.root_panel.tabbed.v1.id != second.layout.v1.root_panel.tabbed.v1.id


@pytest.mark.parametrize(
    ("runs", "expected"),
    [
        ("run-rid-1", ["run-rid-1"]),
        (["run-rid-1", "run-rid-2"], ["run-rid-1", "run-rid-2"]),
        (("run-rid-1",), ["run-rid-1"]),
    ],
)
def test_client_accepts_one_rid_or_a_sequence(clients: MagicMock, runs: object, expected: list[str]) -> None:
    """A lone RID is one run, not a sequence of characters."""
    NominalClient(_clients=clients).create_workbook(runs=runs, title="Explicit")  # type: ignore[arg-type]

    assert _request(clients).data_scope.run_rids == expected


def test_client_scopes_to_several_run_objects_without_fetching(clients: MagicMock) -> None:
    """Run objects provide their own names for the title."""
    clients.run.GetRun.side_effect = AssertionError("Run objects need no fetch")

    workbook = NominalClient(_clients=clients).create_workbook(
        runs=[_run(clients), _run(clients, "run-rid-2", "Spin-up 2")]
    )

    request = _request(clients)
    assert request.data_scope.run_rids == ["run-rid-1", "run-rid-2"]
    assert request.title == "Workbook for Spin-up 1 and Spin-up 2"
    assert workbook.run_rids == ["run-rid-1", "run-rid-2"]


def test_client_fetches_names_for_rids_only_for_the_default_title(clients: MagicMock) -> None:
    """RIDs are fetched for their names when the title is left out, and never when it is given."""
    clients.assets.GetAssets.return_value = asset_pb2.GetAssetsResponse(
        responses={"asset-rid-2": asset_pb2.Asset(rid="asset-rid-2", title="Gearbox rig")}
    )
    client = NominalClient(_clients=clients)

    client.create_workbook(assets=[_asset(clients), "asset-rid-2"])
    assert _request(clients).title == "Workbook for Rotor rig and Gearbox rig"
    clients.assets.GetAssets.assert_called_once()
    assert list(clients.assets.GetAssets.call_args.args[0].rids) == ["asset-rid-2"]

    clients.notebook.create.reset_mock()
    clients.assets.GetAssets.reset_mock()
    client.create_workbook(assets="asset-rid-2", title="Given")
    assert _request(clients).data_scope.asset_rids == ["asset-rid-2"]
    clients.assets.GetAssets.assert_not_called()


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"runs": "run-rid-1", "assets": "asset-rid-1"},
        {"runs": []},
        {"assets": ()},
    ],
)
def test_client_rejects_a_missing_mixed_or_empty_scope(clients: MagicMock, kwargs: dict[str, object]) -> None:
    """Exactly one non-empty kind of scope is required, checked before the default title or any request."""
    with pytest.raises(ValueError):
        NominalClient(_clients=clients).create_workbook(**kwargs)  # type: ignore[arg-type]

    clients.run.GetRun.assert_not_called()
    clients.assets.GetAssets.assert_not_called()
    clients.notebook.create.assert_not_called()


@pytest.mark.parametrize(
    ("names", "expected"),
    [
        (["A"], "Workbook for A"),
        (["A", "B"], "Workbook for A and B"),
        (["A", "B", "C", "D"], "Workbook for A, B and 2 more"),
    ],
)
def test_default_title_names_the_scope(names: list[str], expected: str) -> None:
    assert _workbook_title(names) == expected


def test_client_creates_a_blank_comparison_workbook_only_when_asked(clients: MagicMock) -> None:
    """`comparison=True` sends the comparison type and content; otherwise the type is left to the server."""
    client = NominalClient(_clients=clients)

    client.create_workbook(runs=["run-rid-1", "run-rid-2"], title="Compare", comparison=True)
    request = _request(clients)
    assert request.notebook_type == scout_notebook_api.NotebookType.COMPARISON_WORKBOOK
    assert request.content_v2.workbook is None
    assert request.content_v2.comparison_workbook.charts == {}
    assert request.data_scope.run_rids == ["run-rid-1", "run-rid-2"]

    clients.notebook.create.reset_mock()
    client.create_workbook(runs=["run-rid-1", "run-rid-2"], title="Standard")
    request = _request(clients)
    assert request.notebook_type is None
    assert request.content_v2.comparison_workbook is None
