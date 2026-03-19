from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Mapping, Sequence

from nominal_api import scout_layout_api, scout_template_api, scout_workbookcommon_api

from nominal.core.workbook_template import WorkbookTemplate, _create_workbook_template_with_content_and_layout
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.resource_type import ResourceType
from nominal.experimental.migration.utils.conjure_clone_utils import clone_conjure_objects_with_new_uuids

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkbookTemplateCopyOptions(ResourceCopyOptions):
    new_template_title: str | None = None
    new_template_description: str | None = None
    new_template_labels: Sequence[str] | None = None
    new_template_properties: Mapping[str, str] | None = None
    include_content_and_layout: bool = False


class WorkbookTemplateMigrator(Migrator[WorkbookTemplate, WorkbookTemplateCopyOptions]):
    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.WORKBOOK_TEMPLATE

    def default_copy_options(self) -> WorkbookTemplateCopyOptions:
        return WorkbookTemplateCopyOptions(include_content_and_layout=True)

    def _copy_from_impl(self, source: WorkbookTemplate, options: WorkbookTemplateCopyOptions) -> WorkbookTemplate:
        mapped_rid = self.ctx.migration_state.get_mapped_rid(self.resource_type, source.rid)
        if mapped_rid is not None:
            logger.debug("Skipping %s (rid: %s): already in migration state", self.resource_label, source.rid)
            return self.ctx.destination_client.get_workbook_template(mapped_rid)

        raw_source_template = source._clients.template.get(source._clients.auth_header, source.rid)
        new_template_layout, new_workbook_content = self._resolve_template_content_and_layout(
            raw_source_template,
            options,
        )

        new_workbook_template = _create_workbook_template_with_content_and_layout(
            clients=self.ctx.destination_client._clients,
            title=options.new_template_title
            if options.new_template_title is not None
            else raw_source_template.metadata.title,
            description=options.new_template_description
            if options.new_template_description is not None
            else raw_source_template.metadata.description,
            labels=options.new_template_labels
            if options.new_template_labels is not None
            else raw_source_template.metadata.labels,
            properties=options.new_template_properties
            if options.new_template_properties is not None
            else raw_source_template.metadata.properties,
            layout=new_template_layout,
            content=new_workbook_content,
            commit_message="Cloned from template",
            workspace_rid=self.ctx.destination_client.get_workspace(
                self.ctx.destination_client._clients.workspace_rid
            ).rid,
            is_published=raw_source_template.metadata.is_published,
        )
        self.ctx.migration_state.record_mapping(self.resource_type, source.rid, new_workbook_template.rid)
        return new_workbook_template

    def _resolve_template_content_and_layout(
        self,
        raw_source_template: scout_template_api.Template,
        options: WorkbookTemplateCopyOptions,
    ) -> tuple[scout_layout_api.WorkbookLayout, scout_workbookcommon_api.WorkbookContent]:
        if options.include_content_and_layout:
            return clone_conjure_objects_with_new_uuids((raw_source_template.layout, raw_source_template.content))

        return (
            scout_layout_api.WorkbookLayout(
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
            ),
            scout_workbookcommon_api.WorkbookContent(channel_variables={}, charts={}),
        )

    def _get_resource_name(self, resource: WorkbookTemplate) -> str:
        return resource.title
