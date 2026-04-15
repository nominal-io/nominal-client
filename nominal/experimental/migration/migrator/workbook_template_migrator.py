from __future__ import annotations

import json
import logging
import re
import uuid
from dataclasses import dataclass
from typing import Mapping, Sequence, cast

from conjure_python_client._serde.decoder import ConjureDecoder
from conjure_python_client._serde.encoder import ConjureEncoder
from nominal_api import scout_layout_api, scout_template_api, scout_workbookcommon_api

from nominal.core import NominalClient
from nominal.core._clientsbunch import ClientsBunch
from nominal.core.workbook_template import WorkbookTemplate, _create_workbook_template_with_content_and_layout
from nominal.experimental.id_utils.id_utils import UUID_RE
from nominal.experimental.migration.migrator.attachment_migrator import AttachmentMigrator
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.resource_type import ResourceType
from nominal.experimental.migration.utils.conjure_clone_utils import clone_conjure_objects_with_new_uuids

logger = logging.getLogger(__name__)

ATTACHMENT_RID_PATTERN = re.compile(rf"ri\.attachments\.[^.]+\.attachment\.{UUID_RE}")


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

    def _get_existing_destination_resource(
        self, destination_client: NominalClient, mapped_rid: str
    ) -> WorkbookTemplate:
        return destination_client.get_workbook_template(mapped_rid)

    def _copy_from_impl(self, source: WorkbookTemplate, options: WorkbookTemplateCopyOptions) -> WorkbookTemplate:
        existing_template = self.get_existing_destination_resource(source)
        if existing_template is not None:
            return existing_template

        destination_client = self.destination_client_for(source)
        raw_source_template = source._clients.template.get(source._clients.auth_header, source.rid)
        new_template_layout, new_workbook_content = self._resolve_template_content_and_layout(
            raw_source_template,
            options,
        )

        new_template_layout, new_workbook_content = self._migrate_content_attachments(
            source, new_template_layout, new_workbook_content
        )

        new_workbook_template = _create_workbook_template_with_content_and_layout(
            clients=destination_client._clients,
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
            workspace_rid=destination_client.get_workspace(destination_client._clients.workspace_rid).rid,
            is_published=raw_source_template.metadata.is_published,
        )
        self.ctx.migration_state.record_mapping(self.resource_type, source.rid, new_workbook_template.rid)
        return new_workbook_template

    def _migrate_content_attachments(
        self,
        source: WorkbookTemplate,
        layout: scout_layout_api.WorkbookLayout,
        content: scout_workbookcommon_api.WorkbookContent,
    ) -> tuple[scout_layout_api.WorkbookLayout, scout_workbookcommon_api.WorkbookContent]:
        """Find and migrate attachment RIDs in template content.

        Serializes the content to JSON, finds all attachment RIDs, migrates each
        attachment from the source to the destination, and replaces old RIDs with
        new ones in the content.
        """
        content_json = json.dumps(ConjureEncoder.do_encode(content))
        content_rids = set(ATTACHMENT_RID_PATTERN.findall(content_json))

        if not content_rids:
            return layout, content

        source_clients = cast(ClientsBunch, source._clients)
        attachment_migrator = AttachmentMigrator(self.ctx)
        rid_map: dict[str, str] = {}
        for old_rid in content_rids:
            new_attachment = attachment_migrator.migrate_by_rid(source_clients, old_rid)
            rid_map[old_rid] = new_attachment.rid
            logger.debug("Migrated template attachment %s -> %s", old_rid, new_attachment.rid)

        def _replace_rid(match: re.Match[str]) -> str:
            return rid_map.get(match.group(0), match.group(0))

        content_json = ATTACHMENT_RID_PATTERN.sub(_replace_rid, content_json)
        decoder = ConjureDecoder()
        new_content = decoder.do_decode(json.loads(content_json), scout_workbookcommon_api.WorkbookContent)

        logger.info("Migrated %d attachment(s) in template content", len(rid_map))
        return layout, new_content

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
