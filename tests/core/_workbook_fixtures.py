from __future__ import annotations

from nominal_api import (
    scout_layout_api,
    scout_notebook_api,
    scout_template_api,
    scout_versioning_api,
    scout_workbookcommon_api,
)


def empty_workbook_layout() -> scout_layout_api.WorkbookLayout:
    """Build a workbook layout with an empty root panel."""
    return scout_layout_api.WorkbookLayout(
        v1=scout_layout_api.WorkbookLayoutV1(
            root_panel=scout_layout_api.Panel(
                tabbed=scout_layout_api.TabbedPanel(v1=scout_layout_api.TabbedPanelV1(id="root", tabs=[]))
            )
        )
    )


def notebook_response(metadata: scout_notebook_api.NotebookMetadata, *, rid: str) -> scout_notebook_api.Notebook:
    """Build a complete notebook response around a scenario's metadata."""
    return scout_notebook_api.Notebook(
        content_v2=scout_workbookcommon_api.UnifiedWorkbookContent(
            workbook=scout_workbookcommon_api.WorkbookContent(channel_variables={}, charts={})
        ),
        event_refs=[],
        layout=empty_workbook_layout(),
        metadata=metadata,
        rid=rid,
        snapshot_author_rid=metadata.created_by_rid,
        snapshot_created_at=metadata.created_at,
        snapshot_rid="snapshot-rid",
        state_as_json="{}",
    )


def template_response(
    metadata: scout_template_api.TemplateMetadata, *, rid: str, commit_message: str = "Initial version"
) -> scout_template_api.Template:
    """Build a complete template response around a scenario's metadata."""
    return scout_template_api.Template(
        charts=[],
        commit=scout_versioning_api.Commit(
            committed_at=metadata.created_at,
            committed_by=metadata.created_by,
            id="commit-id",
            is_working_state=False,
            message=commit_message,
            resource_rid=rid,
        ),
        content=scout_workbookcommon_api.WorkbookContent(channel_variables={}, charts={}),
        layout=empty_workbook_layout(),
        metadata=metadata,
        rid=rid,
    )
