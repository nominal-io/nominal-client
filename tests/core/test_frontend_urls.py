from unittest.mock import MagicMock

import pytest

from nominal.core._utils.frontend_urls import (
    asset_url,
    checklist_preview_url,
    checklist_url,
    data_review_events_url,
    data_review_url,
    dataset_url,
    ingestion_job_url,
    run_url,
    workbook_template_url,
    workbook_url,
)

_APP_BASE_URL = "https://app.nominal.test"
_WORKSPACE_RID = "ri.workspace.main.workspace.test"
_RESOURCE_RID = "ri.resource.test"


@pytest.fixture
def clients() -> MagicMock:
    clients = MagicMock()
    clients.app_base_url = _APP_BASE_URL
    clients.resolve_default_workspace_rid.return_value = _WORKSPACE_RID
    return clients


@pytest.mark.parametrize(
    ("url_builder", "expected_path"),
    [
        (lambda clients: asset_url(clients, _RESOURCE_RID), f"assets/{_RESOURCE_RID}"),
        (lambda clients: checklist_url(clients, _RESOURCE_RID), f"checklists/{_RESOURCE_RID}"),
        (lambda clients: dataset_url(clients, _RESOURCE_RID), f"datasets/{_RESOURCE_RID}"),
        (lambda clients: ingestion_job_url(clients, _RESOURCE_RID), f"ingestion/{_RESOURCE_RID}"),
        (lambda clients: run_url(clients, "ri.run.test"), "runs/ri.run.test"),
        (lambda clients: workbook_url(clients, _RESOURCE_RID), f"workbooks/{_RESOURCE_RID}"),
        (
            lambda clients: workbook_template_url(clients, _RESOURCE_RID),
            f"workbooks/templates/{_RESOURCE_RID}",
        ),
    ],
)
def test_resource_url_includes_workspace_prefix(clients, url_builder, expected_path):
    assert url_builder(clients) == f"{_APP_BASE_URL}/w/{_WORKSPACE_RID}/{expected_path}"


def test_checklist_preview_url_includes_workspace_prefix(clients):
    assert checklist_preview_url(clients, _RESOURCE_RID, "ri.run.test") == (
        f"{_APP_BASE_URL}/w/{_WORKSPACE_RID}/checklists/{_RESOURCE_RID}?previewRunRid=ri.run.test"
    )


def test_data_review_urls_include_workspace_prefix(clients):
    assert data_review_url(clients, "ri.run.test", _RESOURCE_RID) == (
        f"{_APP_BASE_URL}/w/{_WORKSPACE_RID}/runs/ri.run.test/"
        f"?tab=checklist-executions&checklistExecution={_RESOURCE_RID}&openCheckExecutionErrorReview="
    )
    assert data_review_events_url(clients, "ri.run.test", _RESOURCE_RID) == (
        f"{_APP_BASE_URL}/w/{_WORKSPACE_RID}/runs/ri.run.test/?tab=events&checklistExecution={_RESOURCE_RID}"
    )


def test_resource_url_resolves_the_default_workspace(clients):
    asset_url(clients, _RESOURCE_RID)

    clients.resolve_default_workspace_rid.assert_called_once_with()
