"""Links to resource pages in the Nominal app.

Frontend routes are workspace-scoped, so every URL is built under `/w/{workspace_rid}/`.
"""

from nominal.core._clientsbunch import HasScoutParams


def _resource_url(clients: HasScoutParams, path: str) -> str:
    workspace_rid = clients.resolve_default_workspace_rid()
    return f"{clients.app_base_url.rstrip('/')}/w/{workspace_rid}/{path.lstrip('/')}"


def asset_url(clients: HasScoutParams, asset_rid: str) -> str:
    return _resource_url(clients, f"/assets/{asset_rid}")


def checklist_url(clients: HasScoutParams, checklist_rid: str) -> str:
    return _resource_url(clients, f"/checklists/{checklist_rid}")


def checklist_preview_url(clients: HasScoutParams, checklist_rid: str, run_rid: str) -> str:
    return f"{checklist_url(clients, checklist_rid)}?previewRunRid={run_rid}"


def data_review_url(clients: HasScoutParams, run_rid: str, data_review_rid: str) -> str:
    return _resource_url(
        clients,
        f"/runs/{run_rid}/?tab=checklist-executions"
        f"&checklistExecution={data_review_rid}&openCheckExecutionErrorReview=",
    )


def data_review_events_url(clients: HasScoutParams, run_rid: str, data_review_rid: str) -> str:
    return _resource_url(clients, f"/runs/{run_rid}/?tab=events&checklistExecution={data_review_rid}")


def dataset_url(clients: HasScoutParams, dataset_rid: str) -> str:
    return _resource_url(clients, f"/datasets/{dataset_rid}")


def ingestion_job_url(clients: HasScoutParams, ingestion_job_rid: str) -> str:
    return _resource_url(clients, f"/ingestion/{ingestion_job_rid}")


def run_url(clients: HasScoutParams, run_rid: str) -> str:
    return _resource_url(clients, f"/runs/{run_rid}")


def workbook_url(clients: HasScoutParams, workbook_rid: str) -> str:
    return _resource_url(clients, f"/workbooks/{workbook_rid}")


def workbook_template_url(clients: HasScoutParams, workbook_template_rid: str) -> str:
    return _resource_url(clients, f"/workbooks/templates/{workbook_template_rid}")
