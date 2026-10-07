from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from nominal_api import scout_workbookcommon_api

from nominal.core.asset import Asset
from nominal.core.client import NominalClient
from nominal.core.run import Run
from nominal.core.workbook import Workbook, WorkbookType
from nominal.core.workbook_template import WorkbookTemplate
from nominal.exceptions import NominalWorkbookCreationError

TEMPLATE_RID = "ri.scout.template.1"


@pytest.fixture
def mock_clients():
    clients = MagicMock()
    raw_template = clients.template.get.return_value
    raw_template.rid = TEMPLATE_RID
    raw_template.metadata.title = "template"
    raw_template.metadata.description = "template description"
    raw_template.content = scout_workbookcommon_api.WorkbookContent(channel_variables={}, charts={})
    return clients


@pytest.fixture(autouse=True)
def workbook_from_conjure():
    with patch.object(Workbook, "_from_conjure") as mock:
        yield mock


@pytest.fixture
def client(mock_clients):
    return NominalClient(_clients=mock_clients)


@pytest.fixture
def run(mock_clients):
    run = Run(
        rid="ri.scout.run.1",
        name="run",
        description="",
        properties={},
        labels=[],
        links=[],
        start=0,
        end=1,
        run_number=1,
        assets=["ri.scout.asset.1"],
        created_at=0,
        is_archived=False,
        _clients=mock_clients,
    )
    with patch.object(Run, "_from_conjure", return_value=run):
        yield run


@pytest.fixture
def asset(mock_clients):
    asset = Asset(
        rid="ri.scout.asset.1",
        name="asset",
        description=None,
        properties={},
        labels=[],
        created_at=0,
        is_archived=False,
        _clients=mock_clients,
    )
    with patch.object(Asset, "_from_conjure", return_value=asset):
        yield asset


def _template(clients) -> WorkbookTemplate:
    return WorkbookTemplate(
        rid=TEMPLATE_RID,
        title="template",
        description="",
        labels=[],
        properties={},
        workbook_type=WorkbookType.WORKBOOK,
        _clients=clients,
    )


def _notebook_request(mock_clients):
    mock_clients.notebook.create.assert_called_once()
    return mock_clients.notebook.create.call_args.args[1]


def test_create_run_with_template_rid_links_workbook_to_run(client, mock_clients, run):
    """A template RID gives a workbook with the template's layout, whose data scope is the new run."""
    result = client.create_run("run", start=0, end=1, workbook_template=TEMPLATE_RID)

    assert result is run
    mock_clients.template.get.assert_called_once_with(mock_clients.auth_header, TEMPLATE_RID)
    request = _notebook_request(mock_clients)
    assert request.data_scope.run_rids == [run.rid]
    assert request.data_scope.asset_rids is None
    assert request.layout is mock_clients.template.get.return_value.layout
    assert request.title == "Workbook from 'template'"


def test_create_run_with_template_from_other_client_uses_caller_clients(client, mock_clients, run):
    """A template object from another client is fetched and used through the caller's client and workspace."""
    other_clients = MagicMock()

    client.create_run("run", start=0, end=1, workbook_template=_template(other_clients))

    other_clients.template.get.assert_not_called()
    other_clients.notebook.create.assert_not_called()
    mock_clients.template.get.assert_called_once_with(mock_clients.auth_header, TEMPLATE_RID)
    assert _notebook_request(mock_clients).workspace is mock_clients.resolve_default_workspace_rid.return_value


def test_create_run_without_template_creates_no_workbook(client, mock_clients, run):
    """Without a template, no template is fetched and no workbook is created."""
    client.create_run("run", start=0, end=1)

    mock_clients.template.get.assert_not_called()
    mock_clients.notebook.create.assert_not_called()


def test_create_run_with_unknown_template_creates_no_run(client, mock_clients):
    """A template that cannot be fetched raises before the run is created."""
    mock_clients.template.get.side_effect = RuntimeError("template not found")

    with pytest.raises(RuntimeError, match="template not found"):
        client.create_run("run", start=0, end=1, workbook_template="ri.scout.template.missing")

    mock_clients.run.create_run.assert_not_called()
    mock_clients.notebook.create.assert_not_called()


def test_create_run_archives_run_when_workbook_fails(client, mock_clients, run):
    """If the workbook fails, the run is archived with its linked workbooks, and the original error is raised."""
    failure = RuntimeError("notebook service timed out")
    mock_clients.notebook.create.side_effect = failure

    with pytest.raises(RuntimeError) as excinfo:
        client.create_run("run", start=0, end=1, workbook_template=TEMPLATE_RID)

    assert excinfo.value is failure
    mock_clients.run.archive_run.assert_called_once_with(
        mock_clients.auth_header, run.rid, include_linked_workbooks=True
    )


def test_create_run_reports_unarchived_run_when_archive_fails(client, mock_clients, run):
    """If the workbook and the archive both fail, the error names the run that is left over."""
    failure = RuntimeError("notebook service timed out")
    mock_clients.notebook.create.side_effect = failure
    mock_clients.run.archive_run.side_effect = RuntimeError("run service unavailable")

    with pytest.raises(NominalWorkbookCreationError) as excinfo:
        client.create_run("run", start=0, end=1, workbook_template=TEMPLATE_RID)

    assert excinfo.value.resource_rid == run.rid
    assert excinfo.value.__cause__ is failure
    assert "could not archive" in str(excinfo.value)


def test_asset_create_run_with_template_links_workbook_to_run(asset, mock_clients, run):
    """Asset.create_run links the workbook to the new run, not to the asset."""
    result = asset.create_run("run", start=0, end=1, workbook_template=TEMPLATE_RID)

    assert result is run
    request = _notebook_request(mock_clients)
    assert request.data_scope.run_rids == [run.rid]
    assert request.data_scope.asset_rids is None


def test_create_asset_with_template_links_workbook_to_asset(client, mock_clients, asset):
    """A template gives a workbook whose data scope is the new asset."""
    result = client.create_asset("asset", workbook_template=TEMPLATE_RID)

    assert result is asset
    request = _notebook_request(mock_clients)
    assert request.data_scope.asset_rids == [asset.rid]
    assert request.data_scope.run_rids is None


def test_create_asset_with_unknown_template_creates_no_asset(client, mock_clients):
    """A template that cannot be fetched raises before the asset is created."""
    mock_clients.template.get.side_effect = RuntimeError("template not found")

    with pytest.raises(RuntimeError, match="template not found"):
        client.create_asset("asset", workbook_template="ri.scout.template.missing")

    mock_clients.assets.create_asset.assert_not_called()
    mock_clients.notebook.create.assert_not_called()


def test_create_asset_archives_asset_when_workbook_fails(client, mock_clients, asset):
    """If the workbook fails, the asset is archived with its linked workbooks, and the original error is raised."""
    failure = RuntimeError("notebook service timed out")
    mock_clients.notebook.create.side_effect = failure

    with pytest.raises(RuntimeError) as excinfo:
        client.create_asset("asset", workbook_template=TEMPLATE_RID)

    assert excinfo.value is failure
    mock_clients.assets.archive.assert_called_once_with(
        mock_clients.auth_header, asset.rid, include_linked_workbooks=True
    )
