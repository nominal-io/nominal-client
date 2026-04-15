from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Mapping, Sequence, cast

from nominal_api import api as nominal_api
from nominal_api import scout_notebook_api

from nominal.core import NominalClient
from nominal.core._clientsbunch import ClientsBunch
from nominal.core.asset import Asset
from nominal.core.run import Run
from nominal.core.workbook import Workbook
from nominal.core.workbook_template import WorkbookTemplate
from nominal.experimental.id_utils.id_utils import UUID_RE
from nominal.experimental.migration.migrator.attachment_migrator import AttachmentMigrator
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.migrator.workbook_template_migrator import (
    WorkbookTemplateCopyOptions,
    WorkbookTemplateMigrator,
)
from nominal.experimental.migration.resource_type import ResourceType

logger = logging.getLogger(__name__)

ATTACHMENT_RID_PATTERN = re.compile(rf"ri\.attachments\.[^.]+\.attachment\.{UUID_RE}")


@dataclass(frozen=True)
class WorkbookCopyOptions(ResourceCopyOptions):
    destination_asset: Asset | None = None
    destination_run: Run | None = None
    new_labels: Sequence[str] | None = None
    new_properties: Mapping[str, str] | None = None


class WorkbookMigrator(Migrator[Workbook, WorkbookCopyOptions]):
    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.WORKBOOK

    def clone(self, source: Workbook) -> Workbook:
        raise NotImplementedError("Workbook clone is unsupported; use copy_from with destination asset/run.")

    def default_copy_options(self) -> WorkbookCopyOptions | None:
        return None

    def _get_existing_destination_resource(self, destination_client: NominalClient, mapped_rid: str) -> Workbook:
        return destination_client.get_workbook(mapped_rid)

    def _copy_from_impl(self, source: Workbook, options: WorkbookCopyOptions) -> Workbook:
        """This method copies content from an old workbook to a new workbook by use of templates, in order to
        modify hardcoded variables in workbook content. We do this by creating a template in the source
        client, copying the template to the destination client, creating a new workbook from the template in the
        destination client, and then archiving the template in both clients.
        """
        existing_workbook = self.get_existing_destination_resource(source)
        if existing_workbook is not None:
            return existing_workbook

        if (options.destination_asset is None) == (options.destination_run is None):
            raise ValueError("Exactly one of destination_asset or destination_run must be provided.")

        # NOTE: source_template is ephemeral — _create_template_from_workbook() assigns it a new rid
        # on every call. If the process crashes after new_template is created but before new_workbook
        # is recorded below, the destination template from the previous run becomes orphaned (not
        # archived) and a fresh one is created on resume. This does not cause duplicate workbooks
        # (the early-return above handles that), but orphaned templates may accumulate. Fixing this
        # properly requires a stable dedup key derived from source.rid rather than the ephemeral
        # source_template.rid.
        source_template = source._create_template_from_workbook()
        template_migrator = WorkbookTemplateMigrator(self.ctx)
        new_template = template_migrator.copy_from(
            source_template,
            WorkbookTemplateCopyOptions(include_content_and_layout=True),
        )
        new_workbook = self._create_destination_workbook(source, new_template, options)
        self.ctx.migration_state.record_mapping(self.resource_type, source.rid, new_workbook.rid)

        source_metadata = source._get_latest_api().metadata
        labels = options.new_labels if options.new_labels is not None else source_metadata.labels
        properties = options.new_properties if options.new_properties is not None else source_metadata.properties
        new_workbook.update(labels=labels, properties=properties)

        self._migrate_preview_image(source, new_workbook)

        new_template.archive()
        source_template.archive()
        return new_workbook

    def _create_destination_workbook(
        self,
        source: Workbook,
        new_template: WorkbookTemplate,
        options: WorkbookCopyOptions,
    ) -> Workbook:
        if options.destination_asset is not None:
            return new_template.create_workbook(
                asset=options.destination_asset,
                title=source.title,
                is_draft=source.is_draft(),
            )

        destination_run = options.destination_run
        if destination_run is None:
            raise ValueError("Exactly one of destination_asset or destination_run must be provided.")

        return new_template.create_workbook(
            run=destination_run,
            title=source.title,
            is_draft=source.is_draft(),
        )

    def _migrate_preview_image(self, source: Workbook, dest: Workbook) -> None:
        """Migrate preview image attachment RIDs from source to destination workbook.

        Reads the source workbook's preview image metadata, migrates any referenced
        attachments, and updates the destination workbook with the remapped RIDs.
        Content attachments are handled by the template migrator.
        """
        source_clients = cast(ClientsBunch, source._clients)
        source_raw = source_clients.notebook.get(source_clients.auth_header, source.rid)

        preview_image = source_raw.metadata.preview_image
        if preview_image is None:
            return

        preview_rids: set[str] = set()
        for rid in (preview_image.light, preview_image.dark):
            if rid is not None and ATTACHMENT_RID_PATTERN.fullmatch(rid):
                preview_rids.add(rid)

        if not preview_rids:
            return

        attachment_migrator = AttachmentMigrator(self.ctx)
        rid_map: dict[str, str] = {}
        for old_rid in preview_rids:
            new_attachment = attachment_migrator.migrate_by_rid(source_clients, old_rid)
            rid_map[old_rid] = new_attachment.rid
            logger.debug("Migrated preview image attachment %s -> %s", old_rid, new_attachment.rid)

        dest_clients = self.destination_client_for(source)._clients
        dest_clients.notebook.update_metadata(
            dest_clients.auth_header,
            scout_notebook_api.UpdateNotebookMetadataRequest(
                preview_image=nominal_api.ThemeAwareImage(
                    light=(
                        rid_map.get(preview_image.light, preview_image.light)
                        if preview_image.light is not None
                        else None
                    ),
                    dark=(
                        rid_map.get(preview_image.dark, preview_image.dark) if preview_image.dark is not None else None
                    ),
                ),
            ),
            dest.rid,
        )

        logger.info("Migrated preview image for workbook %s", dest.title)

    def _get_resource_name(self, resource: Workbook) -> str:
        return resource.title
