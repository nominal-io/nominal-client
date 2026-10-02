from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from nominal.core.asset import Asset
from nominal.core.client import NominalClient
from nominal.core.run import Run
from nominal.core.workbook import WorkbookType
from nominal.core.workbook_template import WorkbookTemplate


@pytest.fixture
def mock_clients():
    return MagicMock()


@pytest.fixture
def client(mock_clients):
    return NominalClient(_clients=mock_clients)


@pytest.fixture
def run(mock_clients):
    return Run(
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


@pytest.fixture
def asset(mock_clients):
    return Asset(
        rid="ri.scout.asset.1",
        name="asset",
        description=None,
        properties={},
        labels=[],
        created_at=0,
        is_archived=False,
        _clients=mock_clients,
    )


@pytest.fixture
def template(mock_clients):
    return WorkbookTemplate(
        rid="ri.scout.template.1",
        title="template",
        description="",
        labels=[],
        properties={},
        workbook_type=WorkbookType.WORKBOOK,
        _clients=mock_clients,
    )


@pytest.fixture
def create_workbook():
    with patch.object(WorkbookTemplate, "create_workbook", autospec=True) as mock:
        yield mock


def test_create_run_with_template_rid_links_workbook_to_run(client, mock_clients, run, create_workbook):
    with patch.object(Run, "_from_conjure", return_value=run):
        result = client.create_run("run", start=0, end=1, workbook_template="ri.scout.template.1")

    assert result is run
    mock_clients.template.get.assert_called_once_with(mock_clients.auth_header, "ri.scout.template.1")
    create_workbook.assert_called_once()
    assert create_workbook.call_args.args[0].rid == mock_clients.template.get.return_value.rid
    assert create_workbook.call_args.kwargs == {"run": run}


def test_create_run_with_template_instance_does_not_fetch_template(
    client, mock_clients, run, template, create_workbook
):
    with patch.object(Run, "_from_conjure", return_value=run):
        client.create_run("run", start=0, end=1, workbook_template=template)

    mock_clients.template.get.assert_not_called()
    create_workbook.assert_called_once_with(template, run=run)


def test_create_run_without_template_creates_no_workbook(client, mock_clients, run, create_workbook):
    with patch.object(Run, "_from_conjure", return_value=run):
        client.create_run("run", start=0, end=1)

    mock_clients.template.get.assert_not_called()
    create_workbook.assert_not_called()


def test_create_run_with_unknown_template_creates_no_run(client, mock_clients, create_workbook):
    mock_clients.template.get.side_effect = RuntimeError("template not found")

    with pytest.raises(RuntimeError, match="template not found"):
        client.create_run("run", start=0, end=1, workbook_template="ri.scout.template.missing")

    mock_clients.run.create_run.assert_not_called()
    create_workbook.assert_not_called()


def test_asset_create_run_with_template_links_workbook_to_run(asset, run, template, create_workbook):
    with patch.object(Run, "_from_conjure", return_value=run):
        result = asset.create_run("run", start=0, end=1, workbook_template=template)

    assert result is run
    create_workbook.assert_called_once_with(template, run=run)


def test_create_asset_with_template_links_workbook_to_asset(client, asset, template, create_workbook):
    with patch.object(Asset, "_from_conjure", return_value=asset):
        result = client.create_asset("asset", workbook_template=template)

    assert result is asset
    create_workbook.assert_called_once_with(template, asset=asset)


def test_create_asset_with_unknown_template_creates_no_asset(client, mock_clients, create_workbook):
    mock_clients.template.get.side_effect = RuntimeError("template not found")

    with pytest.raises(RuntimeError, match="template not found"):
        client.create_asset("asset", workbook_template="ri.scout.template.missing")

    mock_clients.assets.create_asset.assert_not_called()
    create_workbook.assert_not_called()
