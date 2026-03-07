from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Mapping, Sequence

from nominal_api import scout_layout_api, scout_workbookcommon_api

from nominal.core.workbook_template import WorkbookTemplate, _create_workbook_template_with_content_and_layout
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.utils.conjure_clone_utils import clone_conjure_objects_with_new_uuids

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkbookTemplateCopyOptions(ResourceCopyOptions):
    new_template_title: str | None = None
    new_template_description: str | None = None
    new_template_labels: Sequence[str] | None = None
    new_template_properties: Mapping[str, str] | None = None
    include_content_and_layout: bool = False


class WorkbookTemplateMigrator(Migrator[WorkbookTemplate, WorkbookTemplate, WorkbookTemplateCopyOptions]):
    def clone(self, source: WorkbookTemplate) -> WorkbookTemplate:
        return self.copy_from(source, WorkbookTemplateCopyOptions(include_content_and_layout=True))

    def copy_from(self, source: WorkbookTemplate, options: WorkbookTemplateCopyOptions) -> WorkbookTemplate:
        log_extras = {
            "destination_client_workspace": self.ctx.destination_client.get_workspace(
                self.ctx.destination_client._clients.workspace_rid
            ).rid
        }
        logger.debug(
            "Cloning workbook template: %s (rid: %s)",
            source.title,
            source.rid,
            extra=log_extras,
        )
        raw_source_template = source._clients.template.get(source._clients.auth_header, source.rid)

        if options.include_content_and_layout:
            template_layout = raw_source_template.layout
            template_content = raw_source_template.content
            (new_template_layout, new_workbook_content) = clone_conjure_objects_with_new_uuids(
                (template_layout, template_content)
            )
        else:
            new_template_layout = scout_layout_api.WorkbookLayout(
                v1=scout_layout_api.WorkbookLayoutV1(
                    root_panel=scout_layout_api.Panel(
                        tabbed=scout_layout_api.TabbedPanel(
                            v1=scout_layout_api.TabbedPanelV1(
                                id=str(uuid.uuid4()),
                                tabs=[],
                            )
                        )
                    )
                )
            )
            new_workbook_content = scout_workbookcommon_api.WorkbookContent(channel_variables={}, charts={})

        new_workbook_template = _create_workbook_template_with_content_and_layout(
            clients=self.ctx.destination_client._clients,
            title=options.new_template_title or raw_source_template.metadata.title,
            description=options.new_template_description or raw_source_template.metadata.description,
            labels=options.new_template_labels or raw_source_template.metadata.labels,
            properties=options.new_template_properties or raw_source_template.metadata.properties,
            layout=new_template_layout,
            content=new_workbook_content,
            commit_message="Cloned from template",
            workspace_rid=self.ctx.destination_client.get_workspace(
                self.ctx.destination_client._clients.workspace_rid
            ).rid,
        )
        logger.debug(
            "New workbook template created %s from %s (rid: %s)",
            new_workbook_template.title,
            source.title,
            source.rid,
            extra=log_extras,
        )
        self.record_mapping("WORKBOOK_TEMPLATE", source.rid, new_workbook_template.rid)
        return new_workbook_template
