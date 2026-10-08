from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from nominal_api import scout, scout_template_api

from nominal.core.workbook import WorkbookType
from nominal.core.workbook_template import WorkbookTemplate


@pytest.mark.parametrize(
    "changes",
    [
        {},
        {"title": "", "description": "", "labels": [], "properties": {}},
        {
            "title": "New title",
            "description": "New description",
            "labels": ("flight",),
            "properties": {"campaign": "October"},
        },
    ],
)
def test_update_refreshes_in_place_from_returned_metadata(mock_clients: MagicMock, changes: dict[str, Any]) -> None:
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
    expected = WorkbookTemplate._from_template_summary(
        mock_clients, scout_template_api.TemplateSummary(metadata=metadata, rid=template.rid)
    )

    result = template.update(**changes)

    assert result is template
    assert template == expected
    assert template._clients is mock_clients
    mock_clients.template.get.assert_not_called()
    mock_clients.template.update_metadata.assert_called_once()
    auth_header, request, rid = mock_clients.template.update_metadata.call_args.args
    assert auth_header == mock_clients.auth_header
    assert rid == template.rid
    assert request.title == changes.get("title")
    assert request.description == changes.get("description")
    assert request.labels == (list(changes["labels"]) if "labels" in changes else None)
    assert request.properties == changes.get("properties")
