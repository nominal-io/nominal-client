from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from nominal_api import scout, scout_notebook_api, scout_template_api

from nominal.core.workbook import WorkbookType
from nominal.core.workbook_template import WorkbookTemplate
from tests.core._workbook_fixtures import template_response


def _api_template(
    rid: str = "source-template-rid", title: str = "Latest template", *, is_archived: bool = False
) -> scout_template_api.Template:
    return template_response(
        scout_template_api.TemplateMetadata(
            created_at="2026-10-07T00:00:00Z",
            created_by="creator-rid",
            description="Latest description",
            edited_at="2026-10-07T00:00:00Z",
            is_archived=is_archived,
            is_published=True,
            labels=["latest-label"],
            properties={"campaign": "latest"},
            title=title,
            updated_at="2026-10-07T00:00:00Z",
        ),
        rid=rid,
        commit_message="Duplicate of Latest template",
    )


@pytest.fixture
def template(mock_clients: MagicMock) -> WorkbookTemplate:
    mock_clients.template = MagicMock(spec=scout.TemplateService)
    mock_clients.template.get.side_effect = AssertionError("Use the returned data without another content fetch")
    mock_clients.template.get.return_value = _api_template()
    mock_clients.template.update_metadata.return_value = _api_template().metadata
    mock_clients.template.update_ref_names.return_value = _api_template()
    mock_clients.template.duplicate.return_value = _api_template("cloned-template-rid", "Latest template - copy")
    mock_clients.resolve_default_workspace_rid.return_value = "workspace-rid"
    return WorkbookTemplate(
        rid="source-template-rid",
        title="Cached title",
        description="Cached description",
        labels=["cached-label"],
        properties={"campaign": "cached"},
        workbook_type=WorkbookType.WORKBOOK,
        _clients=mock_clients,
    )


def test_clone_leaves_inherited_metadata_to_server(template: WorkbookTemplate, mock_clients: MagicMock) -> None:
    """Clone inherits current server metadata and defaults to published, matching Galaxy."""
    result = template.clone()

    request = mock_clients.template.duplicate.call_args.args[1]
    assert request.title is None
    assert request.description is None
    assert request.labels is None
    assert request.properties is None
    assert request.is_published is True
    assert request.workspace == "workspace-rid"
    assert result.rid == "cloned-template-rid"
    assert result.title == "Latest template - copy"
    assert result.description == "Latest description"
    assert result.labels == ["latest-label"]
    assert result.properties == {"campaign": "latest"}
    assert template.rid == "source-template-rid"
    assert template.title == "Cached title"
    assert template.labels == ["cached-label"]
    assert template.properties == {"campaign": "cached"}


def test_clone_preserves_explicit_empty_overrides(template: WorkbookTemplate, mock_clients: MagicMock) -> None:
    """Empty metadata clears inherited values, and an explicit workspace overrides the default."""
    template.clone("", "", labels=[], properties={}, is_published=False, workspace="destination-rid")

    request = mock_clients.template.duplicate.call_args.args[1]
    assert request.title == ""
    assert request.description == ""
    assert request.labels == []
    assert request.properties == {}
    assert request.is_published is False
    assert request.workspace == "destination-rid"
    mock_clients.resolve_default_workspace_rid.assert_not_called()


@pytest.mark.parametrize("operation", ["update", "archive"])
def test_metadata_mutations_apply_server_state_in_place(
    template: WorkbookTemplate, mock_clients: MagicMock, operation: str
) -> None:
    """Metadata responses replace stale fields without fetching content or changing instance identity."""
    if operation == "update":
        assert template.update(title="Requested title") is template
    else:
        mock_clients.template.update_metadata.return_value = _api_template(is_archived=True).metadata
        template.archive()

    assert template.rid == "source-template-rid"
    assert template.title == "Latest template"
    assert template.description == "Latest description"
    assert template.labels == ["latest-label"]
    assert template.properties == {"campaign": "latest"}
    assert template.created_by_rid == "creator-rid"


def test_refname_updates_refresh_returned_metadata(template: WorkbookTemplate) -> None:
    """Renaming refnames also catches the instance up to the metadata returned by the server."""
    template.update_refnames({"source": "replacement"})

    assert template.rid == "source-template-rid"
    assert template.title == "Latest template"
    assert template.description == "Latest description"
    assert template.labels == ["latest-label"]
    assert template.properties == {"campaign": "latest"}


def test_publication_query_refreshes_metadata(template: WorkbookTemplate, mock_clients: MagicMock) -> None:
    """A publication query also refreshes the other metadata it just read from the server."""
    mock_clients.template.get.side_effect = None

    assert template.is_published() is True

    assert template.title == "Latest template"
    assert template.description == "Latest description"
    assert template.labels == ["latest-label"]
    assert template.properties == {"campaign": "latest"}


def test_create_workbook_uses_fresh_template_metadata(template: WorkbookTemplate, mock_clients: MagicMock) -> None:
    """Workbook defaults and the template cache must use the same fresh metadata as the copied content."""
    mock_clients.template.get.side_effect = None
    mock_clients.notebook.create.return_value = MagicMock(
        spec=scout_notebook_api.Notebook,
        rid="workbook-rid",
        metadata=scout_notebook_api.NotebookMetadata(
            created_at="2026-10-07T00:00:00Z",
            created_by_rid="creator-rid",
            data_scope=scout_notebook_api.NotebookDataScope(run_rids=["run-rid"]),
            description="Latest description",
            is_archived=False,
            is_draft=False,
            labels=[],
            lock=scout_notebook_api.Lock(is_locked=False),
            notebook_type=scout_notebook_api.NotebookType.WORKBOOK,
            properties={},
            title="Workbook from 'Latest template'",
        ),
    )

    template.create_workbook(run="run-rid")

    request = mock_clients.notebook.create.call_args.args[1]
    assert request.title == "Workbook from 'Latest template'"
    assert request.description == "Latest description"
    assert template.title == "Latest template"
    assert template.description == "Latest description"
    assert template.labels == ["latest-label"]
    assert template.properties == {"campaign": "latest"}
