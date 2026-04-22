from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Mapping, Sequence, cast

from nominal_api import api as nominal_api
from nominal_api import scout_notebook_api, scout_workbookcommon_api

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
from nominal.experimental.migration.utils.conjure_clone_utils import clone_conjure_objects_with_rid_overrides

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

    def copy_multi_asset_workbook(self, source: Workbook, source_asset_rids: list[str]) -> Workbook | None:
        """Copy a multi-asset workbook by find/replacing asset RIDs in the serialized content.

        All source_asset_rids must already be present in the migration state before calling this.
        Returns None if any asset RID is missing from the migration state (already logged as a skip).
        """
        existing = self.get_existing_destination_resource(source)
        if existing is not None:
            return existing

        rid_map: dict[str, str] = {}
        for old_rid in source_asset_rids:
            new_rid = self.ctx.migration_state.get_mapped_rid(ResourceType.ASSET, old_rid)
            if new_rid is None:
                reason = f"asset {old_rid} not found in migration state"
                logger.warning("Skipping multi-asset workbook %s: %s", source.rid, reason)
                self.ctx.migration_state.record_skip(ResourceType.WORKBOOK, source.rid, reason)
                self.ctx.migration_state.clear_pending_multi_asset_workbook(source.rid)
                return None
            rid_map[old_rid] = new_rid

        source_clients = cast(ClientsBunch, source._clients)
        raw_notebook = source_clients.notebook.get(source_clients.auth_header, source.rid)

        content_v2 = raw_notebook.content_v2
        if content_v2 is not None and not isinstance(content_v2, scout_workbookcommon_api.UnifiedWorkbookContent):
            raise ValueError(f"Unexpected content_v2 type for workbook {source.rid}")
        content = (content_v2.workbook if content_v2 is not None else None) or raw_notebook.content
        if content is None:
            raise ValueError(f"Missing content for workbook {source.rid}")

        new_layout, new_content = clone_conjure_objects_with_rid_overrides(
            (raw_notebook.layout, content), rid_overrides=rid_map
        )

        destination_client = self.destination_client_for(source)
        dest_clients = destination_client._clients
        request = scout_notebook_api.CreateNotebookRequest(
            title=raw_notebook.metadata.title,
            description=raw_notebook.metadata.description,
            is_draft=source.is_draft(),
            state_as_json="{}",
            data_scope=scout_notebook_api.NotebookDataScope(
                asset_rids=[rid_map[r] for r in source_asset_rids],
                run_rids=None,
            ),
            layout=new_layout,
            content_v2=scout_workbookcommon_api.UnifiedWorkbookContent(workbook=new_content),
            event_refs=[],
            workspace=dest_clients.resolve_default_workspace_rid(),
        )
        raw_new_notebook = dest_clients.notebook.create(dest_clients.auth_header, request)
        new_workbook = Workbook._from_conjure(dest_clients, raw_new_notebook)

        self.ctx.migration_state.record_mapping(ResourceType.WORKBOOK, source.rid, new_workbook.rid)
        self.ctx.migration_state.clear_pending_multi_asset_workbook(source.rid)

        source_metadata = raw_notebook.metadata
        new_workbook.update(labels=source_metadata.labels, properties=source_metadata.properties)
        self._migrate_preview_image(source, new_workbook)

        logger.info("Migrated multi-asset workbook %s -> %s", source.rid, new_workbook.rid)
        return new_workbook

    def copy_multi_run_workbook(self, source: Workbook, source_run_rids: list[str]) -> Workbook | None:
        """Copy a multi-run workbook by find/replacing run RIDs in the serialized content.

        All source_run_rids must already be present in the migration state before calling this.
        Returns None if any run RID is missing from the migration state (already logged as a skip).
        """
        existing = self.get_existing_destination_resource(source)
        if existing is not None:
            return existing

        rid_map: dict[str, str] = {}
        for old_rid in source_run_rids:
            new_rid = self.ctx.migration_state.get_mapped_rid(ResourceType.RUN, old_rid)
            if new_rid is None:
                reason = f"run {old_rid} not found in migration state"
                logger.warning("Skipping multi-run workbook %s: %s", source.rid, reason)
                self.ctx.migration_state.record_skip(ResourceType.WORKBOOK, source.rid, reason)
                self.ctx.migration_state.clear_pending_multi_run_workbook(source.rid)
                return None
            rid_map[old_rid] = new_rid

        # Also remap any asset RIDs embedded in the content (channels resolve against both
        # run RIDs and asset RIDs, so both must be substituted).
        asset_rid_map = dict(self.ctx.migration_state.rid_mapping.get(ResourceType.ASSET.value, {}))
        rid_overrides = {**asset_rid_map, **rid_map}

        source_clients = cast(ClientsBunch, source._clients)
        raw_notebook = source_clients.notebook.get(source_clients.auth_header, source.rid)

        content_v2 = raw_notebook.content_v2
        if content_v2 is not None and not isinstance(content_v2, scout_workbookcommon_api.UnifiedWorkbookContent):
            raise ValueError(f"Unexpected content_v2 type for workbook {source.rid}")
        content = (content_v2.workbook if content_v2 is not None else None) or raw_notebook.content
        if content is None:
            raise ValueError(f"Missing content for workbook {source.rid}")

        new_layout, new_content = clone_conjure_objects_with_rid_overrides(
            (raw_notebook.layout, content), rid_overrides=rid_overrides
        )

        destination_client = self.destination_client_for(source)
        dest_clients = destination_client._clients
        request = scout_notebook_api.CreateNotebookRequest(
            title=raw_notebook.metadata.title,
            description=raw_notebook.metadata.description,
            is_draft=source.is_draft(),
            state_as_json="{}",
            data_scope=scout_notebook_api.NotebookDataScope(
                asset_rids=None,
                run_rids=[rid_map[r] for r in source_run_rids],
            ),
            layout=new_layout,
            content_v2=scout_workbookcommon_api.UnifiedWorkbookContent(workbook=new_content),
            event_refs=[],
            workspace=dest_clients.resolve_default_workspace_rid(),
        )
        raw_new_notebook = dest_clients.notebook.create(dest_clients.auth_header, request)
        new_workbook = Workbook._from_conjure(dest_clients, raw_new_notebook)

        self.ctx.migration_state.record_mapping(ResourceType.WORKBOOK, source.rid, new_workbook.rid)
        self.ctx.migration_state.clear_pending_multi_run_workbook(source.rid)

        source_metadata = raw_notebook.metadata
        new_workbook.update(labels=source_metadata.labels, properties=source_metadata.properties)
        self._migrate_preview_image(source, new_workbook)

        logger.info("Migrated multi-run workbook %s -> %s", source.rid, new_workbook.rid)
        return new_workbook

    def migrate_deferred_workbooks(self, source_clients_by_asset_rid: dict[str, ClientsBunch]) -> None:
        """Migrate all pending multi-asset and multi-run workbooks recorded in the migration state.

        Should be called after all assets (and their runs) have been migrated so that the full
        RID mapping is available for find/replace.
        """
        pending_multi_asset = dict(self.ctx.migration_state.pending_multi_asset_workbooks)
        pending_multi_run = dict(self.ctx.migration_state.pending_multi_run_workbooks)

        if pending_multi_asset:
            logger.info("Migrating %d deferred multi-asset workbook(s)", len(pending_multi_asset))
            for workbook_rid, source_asset_rids in pending_multi_asset.items():
                source_clients = self._resolve_source_clients(
                    workbook_rid, source_asset_rids, source_clients_by_asset_rid
                )
                if source_clients is None:
                    continue
                raw_notebook = source_clients.notebook.get(source_clients.auth_header, workbook_rid)
                source_workbook = Workbook._from_conjure(source_clients, raw_notebook)
                self.copy_multi_asset_workbook(source_workbook, source_asset_rids)

        if pending_multi_run:
            logger.info("Migrating %d deferred multi-run workbook(s)", len(pending_multi_run))
            for workbook_rid, source_run_rids in pending_multi_run.items():
                source_clients = next(iter(source_clients_by_asset_rid.values()), None)
                if source_clients is None:
                    logger.warning("No source assets available to fetch multi-run workbook %s — skipping", workbook_rid)
                    continue
                raw_notebook = source_clients.notebook.get(source_clients.auth_header, workbook_rid)
                source_workbook = Workbook._from_conjure(source_clients, raw_notebook)
                self.copy_multi_run_workbook(source_workbook, source_run_rids)

    def _resolve_source_clients(
        self,
        workbook_rid: str,
        source_asset_rids: list[str],
        source_clients_by_asset_rid: dict[str, ClientsBunch],
    ) -> ClientsBunch | None:
        for asset_rid in source_asset_rids:
            clients = source_clients_by_asset_rid.get(asset_rid)
            if clients is not None:
                return clients
        logger.warning(
            "Could not resolve source client for multi-asset workbook %s "
            "(none of its assets are in migration resources) — skipping",
            workbook_rid,
        )
        return None

    def _get_resource_name(self, resource: Workbook) -> str:
        return resource.title
