from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from nominal_api import scout, scout_layout_api, scout_template_api, scout_versioning_api, scout_workbookcommon_api

from nominal.core.workbook import WorkbookType
from nominal.core.workbook_template import WorkbookTemplate


@pytest.fixture
def template(mock_clients: MagicMock) -> WorkbookTemplate:
    mock_clients.template = MagicMock(spec=scout.TemplateService)
    mock_clients.template.get.side_effect = AssertionError("Use the duplication response without fetching content")
    mock_clients.resolve_default_workspace_rid.return_value = "workspace-rid"
    mock_clients.template.duplicate.return_value = scout_template_api.Template(
        charts=[],
        commit=scout_versioning_api.Commit(
            committed_at="2026-10-07T00:00:00Z",
            committed_by="creator-rid",
            id="commit-id",
            is_working_state=False,
            message="Duplicate of Latest template",
            resource_rid="cloned-template-rid",
        ),
        content=scout_workbookcommon_api.WorkbookContent(channel_variables={}, charts={}),
        layout=scout_layout_api.WorkbookLayout(
            v1=scout_layout_api.WorkbookLayoutV1(
                root_panel=scout_layout_api.Panel(
                    tabbed=scout_layout_api.TabbedPanel(v1=scout_layout_api.TabbedPanelV1(id="root", tabs=[]))
                )
            )
        ),
        metadata=scout_template_api.TemplateMetadata(
            created_at="2026-10-07T00:00:00Z",
            created_by="creator-rid",
            description="Latest description",
            edited_at="2026-10-07T00:00:00Z",
            is_archived=False,
            is_published=False,
            labels=["latest-label"],
            properties={"campaign": "latest"},
            title="Latest template - copy",
            updated_at="2026-10-07T00:00:00Z",
        ),
        rid="cloned-template-rid",
    )
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
    """Clone must use the latest server metadata and default to an unpublished copy."""
    result = template.clone()

    request = mock_clients.template.duplicate.call_args.args[1]
    assert request.title is None
    assert request.description is None
    assert request.labels is None
    assert request.properties is None
    assert request.is_published is False
    assert request.workspace == "workspace-rid"
    assert result.rid == "cloned-template-rid"
    assert result.title == "Latest template - copy"
    assert result.description == "Latest description"
    assert result.labels == ["latest-label"]
    assert result.properties == {"campaign": "latest"}
    assert template.rid == "source-template-rid"
    assert template.title == "Cached title"


def test_clone_preserves_explicit_empty_overrides(template: WorkbookTemplate, mock_clients: MagicMock) -> None:
    """Empty metadata clears inherited values, and an explicit workspace overrides the default."""
    template.clone("", "", labels=[], properties={}, is_published=True, workspace="destination-rid")

    request = mock_clients.template.duplicate.call_args.args[1]
    assert request.title == ""
    assert request.description == ""
    assert request.labels == []
    assert request.properties == {}
    assert request.is_published is True
    assert request.workspace == "destination-rid"
    mock_clients.resolve_default_workspace_rid.assert_not_called()
