from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence, cast

from conjure_python_client._serde.decoder import ConjureDecoder
from conjure_python_client._serde.encoder import ConjureEncoder
from nominal_api import api as nominal_api
from nominal_api import scout_notebook_api, scout_workbookcommon_api

from nominal.core import NominalClient
from nominal.core._clientsbunch import ClientsBunch
from nominal.core.workbook import Workbook
from nominal.experimental.id_utils.id_utils import UUID_RE
from nominal.experimental.migration.migrator.attachment_migrator import AttachmentMigrator
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.resource_type import ResourceType
from nominal.experimental.migration.utils.conjure_clone_utils import clone_conjure_objects_with_rid_overrides

logger = logging.getLogger(__name__)

ATTACHMENT_RID_PATTERN = re.compile(rf"ri\.attachments\.[^.]+\.attachment\.{UUID_RE}")


@dataclass(frozen=True)
class WorkbookCopyOptions(ResourceCopyOptions):
    source_to_destination_asset_rid_mapping: Mapping[str, str] = field(default_factory=dict)
    source_to_destination_run_rid_mapping: Mapping[str, str] = field(default_factory=dict)
    new_labels: Sequence[str] | None = None
    new_properties: Mapping[str, str] | None = None


class WorkbookMigrator(Migrator[Workbook, WorkbookCopyOptions]):
    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.WORKBOOK

    def clone(self, source: Workbook) -> Workbook:
        """Not supported — workbooks must be copied with an explicit RID mapping via copy_from."""
        raise NotImplementedError("Workbook clone is unsupported; use copy_from with destination asset/run.")

    def default_copy_options(self) -> WorkbookCopyOptions | None:
        return None

    def _get_existing_destination_resource(self, destination_client: NominalClient, mapped_rid: str) -> Workbook:
        return destination_client.get_workbook(mapped_rid)

    def _copy_from_impl(self, source: Workbook, options: WorkbookCopyOptions) -> Workbook:
        existing_workbook = self.get_existing_destination_resource(source)
        if existing_workbook is not None:
            return existing_workbook

        source_clients = cast(ClientsBunch, source._clients)
        raw_notebook = source_clients.notebook.get(source_clients.auth_header, source.rid)

        source_run_rids = raw_notebook.metadata.data_scope.run_rids or []
        source_asset_rids = raw_notebook.metadata.data_scope.asset_rids or []
        rid_overrides: dict[str, str] = {
            **options.source_to_destination_asset_rid_mapping,
            **options.source_to_destination_run_rid_mapping,
        }

        if source_run_rids:
            missing = [r for r in source_run_rids if r not in options.source_to_destination_run_rid_mapping]
            if missing:
                raise ValueError(f"Run RIDs not provided for workbook {source.rid}: {missing}")
            data_scope = scout_notebook_api.NotebookDataScope(
                run_rids=[options.source_to_destination_run_rid_mapping[r] for r in source_run_rids], asset_rids=None
            )
        else:
            missing = [r for r in source_asset_rids if r not in options.source_to_destination_asset_rid_mapping]
            if missing:
                raise ValueError(f"Asset RIDs not provided for workbook {source.rid}: {missing}")
            data_scope = scout_notebook_api.NotebookDataScope(
                run_rids=None,
                asset_rids=[options.source_to_destination_asset_rid_mapping[r] for r in source_asset_rids],
            )

        return self._copy_workbook(
            source,
            raw_notebook,
            rid_overrides,
            data_scope,
            labels=options.new_labels,
            properties=options.new_properties,
        )

    def _copy_workbook(
        self,
        source: Workbook,
        raw_notebook: Any,
        rid_overrides: dict[str, str],
        data_scope: scout_notebook_api.NotebookDataScope,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
    ) -> Workbook:
        """Create a new workbook in the destination by find/replacing RIDs in the source content.

        Handles RID substitution, content attachment migration, preview image migration,
        and metadata (labels/properties) propagation.
        """
        content_v2 = raw_notebook.content_v2
        content = (content_v2.workbook if content_v2 is not None else None) or raw_notebook.content
        if content is None:
            raise ValueError(f"Missing content for workbook {source.rid}")

        new_layout, new_content = clone_conjure_objects_with_rid_overrides(
            (raw_notebook.layout, content), rid_overrides=rid_overrides
        )
        new_content = self._migrate_content_attachments(source, new_content)

        destination_client = self.destination_client_for(source)
        dest_clients = destination_client._clients
        request = scout_notebook_api.CreateNotebookRequest(
            title=raw_notebook.metadata.title,
            description=raw_notebook.metadata.description,
            is_draft=source.is_draft(),
            state_as_json="{}",
            data_scope=data_scope,
            layout=new_layout,
            content_v2=scout_workbookcommon_api.UnifiedWorkbookContent(workbook=new_content),
            event_refs=[],
            workspace=dest_clients.resolve_default_workspace_rid(),
        )
        raw_new_notebook = dest_clients.notebook.create(dest_clients.auth_header, request)
        new_workbook = Workbook._from_conjure(dest_clients, raw_new_notebook)

        self.ctx.migration_state.record_mapping(ResourceType.WORKBOOK, source.rid, new_workbook.rid)

        source_metadata = raw_notebook.metadata
        new_workbook.update(
            labels=labels if labels is not None else source_metadata.labels,
            properties=properties if properties is not None else source_metadata.properties,
        )

        self._migrate_preview_image(source, new_workbook)
        return new_workbook

    def _migrate_content_attachments(
        self,
        source: Workbook,
        content: scout_workbookcommon_api.WorkbookContent,
    ) -> scout_workbookcommon_api.WorkbookContent:
        """Migrate attachment RIDs embedded in workbook content (e.g. images in markdown panels).

        Attachment RIDs appear inside markdown strings, so they cannot be handled by the structured
        RID find/replace in clone_conjure_objects_with_rid_overrides — a regex pass is needed.
        """
        content_json = json.dumps(ConjureEncoder.do_encode(content))
        attachment_rids = set(ATTACHMENT_RID_PATTERN.findall(content_json))
        if not attachment_rids:
            return content

        source_clients = cast(ClientsBunch, source._clients)
        attachment_migrator = AttachmentMigrator(self.ctx)
        rid_map: dict[str, str] = {}
        for old_rid in attachment_rids:
            new_attachment = attachment_migrator.migrate_by_rid(source_clients, old_rid)
            rid_map[old_rid] = new_attachment.rid
            logger.debug("Migrated content attachment %s -> %s", old_rid, new_attachment.rid)

        def _replace_rid(match: re.Match[str]) -> str:
            return rid_map.get(match.group(0), match.group(0))

        content_json = ATTACHMENT_RID_PATTERN.sub(_replace_rid, content_json)
        result: scout_workbookcommon_api.WorkbookContent = ConjureDecoder().do_decode(
            json.loads(content_json), scout_workbookcommon_api.WorkbookContent
        )
        return result

    def _migrate_preview_image(self, source: Workbook, dest: Workbook) -> None:
        """Migrate preview image attachment RIDs from source to destination workbook.

        Reads the source workbook's preview image metadata, migrates any referenced
        attachments, and updates the destination workbook with the remapped RIDs.
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

    def migrate_deferred_workbooks(self, source_clients_by_asset_rid: dict[str, ClientsBunch]) -> None:
        """Migrate all pending multi-asset and multi-run workbooks recorded in the migration state.

        Should be called after all assets (and their runs) have been migrated so that the full
        RID mapping is available for find/replace.
        """
        pending_multi_asset = dict(self.ctx.migration_state.pending_multi_asset_workbooks)
        pending_multi_run = dict(self.ctx.migration_state.pending_multi_run_workbooks)

        asset_rid_map = dict(self.ctx.migration_state.rid_mapping.get(ResourceType.ASSET.value, {}))
        run_rid_map = dict(self.ctx.migration_state.rid_mapping.get(ResourceType.RUN.value, {}))

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
                self.copy_from(
                    source_workbook, WorkbookCopyOptions(source_to_destination_asset_rid_mapping=asset_rid_map)
                )
                self.ctx.migration_state.clear_pending_multi_asset_workbook(workbook_rid)
                logger.info("Migrated multi-asset workbook %s", workbook_rid)

        if pending_multi_run:
            logger.info("Migrating %d deferred multi-run workbook(s)", len(pending_multi_run))
            for workbook_rid in pending_multi_run:
                source_clients = next(iter(source_clients_by_asset_rid.values()), None)
                if source_clients is None:
                    logger.warning("No source assets available to fetch multi-run workbook %s — skipping", workbook_rid)
                    continue
                raw_notebook = source_clients.notebook.get(source_clients.auth_header, workbook_rid)
                source_workbook = Workbook._from_conjure(source_clients, raw_notebook)
                self.copy_from(
                    source_workbook,
                    WorkbookCopyOptions(
                        source_to_destination_asset_rid_mapping=asset_rid_map,
                        source_to_destination_run_rid_mapping=run_rid_map,
                    ),
                )
                self.ctx.migration_state.clear_pending_multi_run_workbook(workbook_rid)
                logger.info("Migrated multi-run workbook %s", workbook_rid)

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
