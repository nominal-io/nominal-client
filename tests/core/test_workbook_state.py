from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from conjure_python_client import ConjureEncoder
from nominal_api import scout, scout_notebook_api, scout_template_api

from nominal.core.client import NominalClient
from nominal.core.workbook import Workbook, WorkbookType
from nominal.core.workbook_template import WorkbookTemplate
from tests.core._workbook_fixtures import notebook_response, template_response


def _workbook_metadata(
    *, title: str = "Server workbook", is_draft: bool = False, is_locked: bool = False, is_archived: bool = False
) -> scout_notebook_api.NotebookMetadata:
    return scout_notebook_api.NotebookMetadata(
        created_at="2026-10-07T00:00:00Z",
        created_by_rid="server-creator-rid",
        data_scope=scout_notebook_api.NotebookDataScope(asset_rids=["server-asset-rid"]),
        description="Server description",
        is_archived=is_archived,
        is_draft=is_draft,
        labels=[],
        lock=scout_notebook_api.Lock(is_locked=is_locked),
        notebook_type=scout_notebook_api.NotebookType.WORKBOOK,
        properties={},
        title=title,
    )


def _notebook(metadata: scout_notebook_api.NotebookMetadata) -> scout_notebook_api.Notebook:
    return notebook_response(metadata, rid="workbook-rid")


def _template_metadata(
    *, title: str = "Server template", is_published: bool = False
) -> scout_template_api.TemplateMetadata:
    return scout_template_api.TemplateMetadata(
        created_at="2026-10-07T00:00:00Z",
        created_by="server-creator-rid",
        description="Server description",
        edited_at="2026-10-07T00:00:00Z",
        is_archived=False,
        is_published=is_published,
        labels=["server-label"],
        properties={"campaign": "October"},
        title=title,
        updated_at="2026-10-07T00:00:00Z",
    )


def _template(metadata: scout_template_api.TemplateMetadata) -> scout_template_api.Template:
    return template_response(metadata, rid="template-rid")


@pytest.fixture
def state_clients(mock_clients: MagicMock) -> MagicMock:
    mock_clients.notebook = MagicMock(spec=scout.NotebookService)
    mock_clients.template = MagicMock(spec=scout.TemplateService)
    mock_clients.notebook.update_metadata.return_value = _workbook_metadata()
    mock_clients.template.update_metadata.return_value = _template_metadata()
    mock_clients.notebook.get.side_effect = AssertionError("Use the returned metadata without a follow-up GET")
    mock_clients.template.get.side_effect = AssertionError("Use the returned metadata without a follow-up GET")
    mock_clients.resolve_default_workspace_rid.return_value = "workspace-rid"
    return mock_clients


@pytest.fixture
def workbook(state_clients: MagicMock) -> Workbook:
    return Workbook(
        rid="workbook-rid",
        title="Cached workbook",
        description="Cached description",
        workbook_type=WorkbookType.WORKBOOK,
        run_rids=["cached-run-rid"],
        asset_rids=None,
        _clients=state_clients,
    )


@pytest.fixture
def template(state_clients: MagicMock) -> WorkbookTemplate:
    return WorkbookTemplate(
        rid="template-rid",
        title="Cached template",
        description="Cached description",
        labels=[],
        properties={},
        workbook_type=WorkbookType.WORKBOOK,
        _clients=state_clients,
    )


def test_workbook_update_distinguishes_false_from_omitted_flags(workbook: Workbook, state_clients: MagicMock) -> None:
    """Explicit False changes state; omitted flags leave the backend's state untouched."""
    workbook.update(is_published=False, is_locked=False)
    explicit = ConjureEncoder().default(state_clients.notebook.update_metadata.call_args.args[1])
    assert explicit["isDraft"] is True
    assert explicit["isLocked"] is False

    workbook.update(title="Rename without changing state")
    omitted = ConjureEncoder().default(state_clients.notebook.update_metadata.call_args.args[1])
    assert omitted["isDraft"] is None
    assert omitted["isLocked"] is None

    workbook.update(is_draft=False)
    assert state_clients.notebook.update_metadata.call_args.args[1].is_draft is False


@pytest.mark.parametrize(("is_draft", "is_published"), [(False, True), (False, False), (True, False), (True, True)])
def test_workbook_rejects_both_publication_aliases_before_io(
    workbook: Workbook, state_clients: MagicMock, is_draft: bool, is_published: bool
) -> None:
    """Even consistent aliases are ambiguous when both are explicitly supplied."""
    with pytest.raises(ValueError, match="is_draft.*is_published"):
        workbook.update(is_draft=is_draft, is_published=is_published)

    state_clients.notebook.update_metadata.assert_not_called()


def test_lock_and_unlock_refresh_authoritative_metadata(workbook: Workbook, state_clients: MagicMock) -> None:
    """Lock changes also adopt metadata changed by another user, without a second fetch."""
    state_clients.notebook.update_metadata.side_effect = [
        _workbook_metadata(title="Locked server title", is_locked=True),
        _workbook_metadata(title="Unlocked server title"),
    ]
    state_clients.notebook.lock.side_effect = AssertionError("Use the metadata update route")
    state_clients.notebook.unlock.side_effect = AssertionError("Use the metadata update route")

    workbook.lock()
    assert state_clients.notebook.update_metadata.call_args.args[1].is_locked is True
    assert workbook.title == "Locked server title"
    assert workbook.run_rids is None
    assert workbook.asset_rids == ["server-asset-rid"]
    assert workbook.created_by_rid == "server-creator-rid"

    workbook.unlock()
    assert state_clients.notebook.update_metadata.call_args.args[1].is_locked is False
    assert workbook.title == "Unlocked server title"


def test_workbook_lock_and_archive_queries_refresh_from_the_same_response(
    workbook: Workbook, state_clients: MagicMock
) -> None:
    """Reading a state flag must also refresh the cached title, author, and data scope."""
    state_clients.notebook.get.side_effect = [
        _notebook(_workbook_metadata(title="Locked workbook", is_locked=True)),
        _notebook(_workbook_metadata(title="Archived workbook", is_archived=True)),
    ]

    assert workbook.is_locked() is True
    assert workbook.title == "Locked workbook"
    assert workbook.run_rids is None
    assert workbook.asset_rids == ["server-asset-rid"]
    assert workbook.created_by_rid == "server-creator-rid"

    assert workbook.is_archived() is True
    assert workbook.title == "Archived workbook"
    assert state_clients.notebook.get.call_count == 2


@pytest.mark.parametrize("published", [False, True])
def test_publication_and_draft_queries_are_inverses_and_refresh_metadata(
    workbook: Workbook, template: WorkbookTemplate, state_clients: MagicMock, published: bool
) -> None:
    """The common publication vocabulary maps to each resource's backend state flag."""
    state_clients.notebook.get.side_effect = None
    state_clients.notebook.get.return_value = _notebook(_workbook_metadata(is_draft=not published))
    state_clients.template.get.side_effect = None
    state_clients.template.get.return_value = _template(_template_metadata(is_published=published))

    assert workbook.is_published() is published
    assert workbook.title == "Server workbook"
    assert workbook.is_draft() is not published
    assert template.is_draft() is not published
    assert template.title == "Server template"
    assert template.labels == ["server-label"]
    assert template.is_published() is published
    assert state_clients.notebook.get.call_count == 2
    assert state_clients.template.get.call_count == 2


def test_template_update_refreshes_response_and_preserves_omitted_publication(
    template: WorkbookTemplate, state_clients: MagicMock
) -> None:
    """An unpublish request uses False, and unrelated updates preserve publication state."""
    result = template.update(title="Requested title", is_published=False)

    assert result is template
    assert state_clients.template.update_metadata.call_args.args[1].is_published is False
    assert template.title == "Server template"
    assert template.description == "Server description"
    assert template.labels == ["server-label"]
    assert template.properties == {"campaign": "October"}
    assert template.created_by_rid == "server-creator-rid"

    template.update(description="Rename without changing publication")
    omitted = ConjureEncoder().default(state_clients.template.update_metadata.call_args.args[1])
    assert omitted["isPublished"] is None


def test_workbook_publication_controls_map_to_draft_state(workbook: Workbook, state_clients: MagicMock) -> None:
    """Publishing leaves draft state, and unpublishing restores it."""
    workbook.publish()
    assert state_clients.notebook.update_metadata.call_args.args[1].is_draft is False
    assert workbook.title == "Server workbook"

    workbook.unpublish()
    assert state_clients.notebook.update_metadata.call_args.args[1].is_draft is True


def test_template_publication_controls_refresh_metadata(template: WorkbookTemplate, state_clients: MagicMock) -> None:
    """Publication controls consume the returned metadata without fetching the template."""
    template.publish()
    assert state_clients.template.update_metadata.call_args.args[1].is_published is True
    assert template.title == "Server template"

    template.unpublish()
    assert state_clients.template.update_metadata.call_args.args[1].is_published is False


@pytest.mark.parametrize("published", [None, False, True])
def test_create_template_keeps_unpublished_default_and_honors_explicit_state(
    state_clients: MagicMock, published: bool | None
) -> None:
    """Existing callers create private templates; callers can also publish at creation."""
    state_clients.template.create.return_value = _template(_template_metadata(is_published=published is True))
    client = NominalClient(_clients=state_clients)

    if published is None:
        result = client.create_workbook_template("Flight review")
    else:
        result = client.create_workbook_template("Flight review", is_published=published)

    assert state_clients.template.create.call_args.args[1].is_published is (published is True)
    assert result.title == "Server template"
