from __future__ import annotations

from unittest.mock import MagicMock

from nominal_api import scout, scout_template_api

from nominal.core.workbook import WorkbookType
from nominal.core.workbook_template import WorkbookTemplate


def test_update_applies_server_metadata_in_place(mock_clients: MagicMock) -> None:
    """The update response is authoritative, including fields the caller did not change."""
    mock_clients.template = MagicMock(spec=scout.TemplateService)
    mock_clients.template.get.side_effect = AssertionError(
        "Should use the route response without fetching the template"
    )
    template = WorkbookTemplate(
        rid="template-rid",
        title="Cached title",
        description="Cached description",
        labels=[],
        properties={},
        workbook_type=WorkbookType.WORKBOOK,
        _clients=mock_clients,
    )
    metadata = scout_template_api.TemplateMetadata(
        created_at="2026-10-07T00:00:00Z",
        created_by="creator-rid",
        description="Latest description",
        edited_at="2026-10-07T00:00:00Z",
        is_archived=False,
        is_published=False,
        labels=["flight"],
        properties={"campaign": "October"},
        title="Server title",
        updated_at="2026-10-07T00:00:00Z",
    )
    mock_clients.template.update_metadata.return_value = metadata

    result = template.update(title="Requested title")

    assert result is template
    assert template.title == "Server title"
    assert template.description == "Latest description"
    assert template.labels == ["flight"]
    assert template.properties == {"campaign": "October"}
    assert template.created_by_rid == "creator-rid"
