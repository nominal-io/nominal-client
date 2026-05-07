from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Sequence

from nominal_api import scout_asset_api

from nominal.core import NominalClient
from nominal.core._event_types import SearchEventOriginType
from nominal.core.asset import Asset
from nominal.core.workbook import Workbook
from nominal.experimental.migration.config.migration_data_config import MigrationDatasetConfig
from nominal.experimental.migration.migrator.attachment_migrator import AttachmentMigrator
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.migrator.checklist_migrator import ChecklistCopyOptions, ChecklistMigrator
from nominal.experimental.migration.migrator.dataset_migrator import DatasetCopyOptions, DatasetMigrator
from nominal.experimental.migration.migrator.event_migrator import EventCopyOptions, EventMigrator
from nominal.experimental.migration.migrator.run_migrator import RunCopyOptions, RunMigrator
from nominal.experimental.migration.migrator.video_migrator import VideoCopyOptions, VideoMigrator
from nominal.experimental.migration.migrator.workbook_migrator import WorkbookCopyOptions, WorkbookMigrator
from nominal.experimental.migration.resource_type import ResourceType

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AssetCopyOptions(ResourceCopyOptions):
    new_asset_name: str | None = None
    new_asset_description: str | None = None
    new_asset_properties: dict[str, Any] | None = None
    new_asset_labels: Sequence[str] | None = None
    dataset_config: MigrationDatasetConfig | None = None
    include_attachments: bool = False
    include_events: bool = False
    include_runs: bool = False
    include_video: bool = False
    include_checklists: bool = False
    include_workbooks: bool = True


class AssetMigrator(Migrator[Asset, AssetCopyOptions]):
    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.ASSET

    def default_copy_options(self) -> AssetCopyOptions:
        return AssetCopyOptions(
            dataset_config=MigrationDatasetConfig(preserve_dataset_uuid=True, include_dataset_files=True),
            include_attachments=True,
            include_events=True,
            include_runs=True,
            include_video=True,
            include_workbooks=True,
        )

    def _copy_from_impl(self, source_asset: Asset, options: AssetCopyOptions) -> Asset:
        if options.include_checklists and not options.include_runs:
            raise ValueError("include_checklists set to True requires include_runs to be set to True.")

        new_asset = self._resolve_destination_asset(source_asset, options)
        # Record immediately so a crash during child migrations doesn't duplicate the asset on resume.
        # base.copy_from will call record_mapping again after this returns, which is idempotent.
        self.ctx.migration_state.record_mapping(self.resource_type, source_asset.rid, new_asset.rid)

        if options.dataset_config is not None:
            self._copy_asset_datasets(source_asset, new_asset, options)

        if options.include_events:
            logger.info("Copying events for asset %s (rid: %s)", source_asset.name, source_asset.rid)
            self._copy_asset_events(source_asset, new_asset)

        if options.include_runs:
            logger.info("Copying runs for asset %s (rid: %s)", source_asset.name, source_asset.rid)
            self._copy_asset_runs(source_asset, new_asset)

        if options.include_checklists:
            logger.info("Copying checklists for asset %s (rid: %s)", source_asset.name, source_asset.rid)
            self._copy_asset_checklists(source_asset)

        if options.include_video:
            logger.info("Copying videos for asset %s (rid: %s)", source_asset.name, source_asset.rid)
            self._copy_asset_videos(source_asset, new_asset)

        if options.include_attachments:
            logger.info("Copying attachments for asset %s (rid: %s)", source_asset.name, source_asset.rid)
            self._copy_asset_attachments(source_asset, new_asset)

        if options.include_workbooks:
            self._copy_asset_and_run_workbooks(source_asset, new_asset, options.include_runs)
        return new_asset

    def _get_resource_name(self, resource: Asset) -> str:
        return resource.name

    def _get_existing_destination_resource(self, destination_client: NominalClient, mapped_rid: str) -> Asset:
        return destination_client.get_asset(mapped_rid)

    def _resolve_destination_asset(self, source_asset: Asset, options: AssetCopyOptions) -> Asset:
        existing_asset = self.get_existing_destination_resource(source_asset)
        if existing_asset is not None:
            return existing_asset

        new_asset = self.destination_client_for(source_asset).create_asset(
            name=options.new_asset_name if options.new_asset_name is not None else source_asset.name,
            description=options.new_asset_description
            if options.new_asset_description is not None
            else source_asset.description,
            properties=options.new_asset_properties
            if options.new_asset_properties is not None
            else source_asset.properties,
            labels=options.new_asset_labels if options.new_asset_labels is not None else source_asset.labels,
        )

        if source_asset._get_latest_api().is_staged:
            new_asset._clients.assets.update_asset(
                new_asset._clients.auth_header,
                scout_asset_api.UpdateAssetRequest(is_staged=True),
                new_asset.rid,
            )

        return new_asset

    def _copy_asset_datasets(self, source_asset: Asset, destination_asset: Asset, options: AssetCopyOptions) -> None:
        if options.dataset_config is None:
            return

        dataset_migrator = DatasetMigrator(self.ctx)

        source_data_scopes = source_asset._list_dataset_scopes()
        source_datasets = {ds.rid: ds for _, ds in source_asset.list_datasets()}

        for source_data_scope in source_data_scopes:
            source_data_scope_name = source_data_scope.data_scope_name
            source_dataset_rid = source_data_scope.data_source.dataset
            if source_dataset_rid is None or source_dataset_rid not in source_datasets:
                raise ValueError(
                    f"Data scope {source_data_scope_name} on asset {source_asset.rid} does not have a dataset"
                )

            source_dataset = source_datasets[source_dataset_rid]
            source_series_tags = source_data_scope.series_tags
            # Always delegate to dataset_migrator.copy_from so that file migrations are
            # never skipped on resume. DatasetMigrator._copy_from_impl handles fetch-or-create
            # internally and always proceeds to file copies regardless.
            new_dataset = dataset_migrator.copy_from(
                source_dataset,
                DatasetCopyOptions(
                    include_files=options.dataset_config.include_dataset_files,
                    preserve_uuid=options.dataset_config.preserve_dataset_uuid,
                ),
            )

            scope_key = f"{source_asset.rid}:{source_data_scope_name}"
            if self.ctx.migration_state.get_mapped_rid(ResourceType.ASSET_DATA_SCOPE, scope_key) is None:
                destination_asset.add_dataset(source_data_scope_name, new_dataset, series_tags=source_series_tags)
                self.ctx.migration_state.record_mapping(ResourceType.ASSET_DATA_SCOPE, scope_key, new_dataset.rid)
            else:
                logger.debug(
                    "Skipping add_dataset for scope %s on asset %s: already in migration state",
                    source_data_scope_name,
                    source_asset.rid,
                )

    def _copy_asset_events(self, source_asset: Asset, destination_asset: Asset) -> None:
        event_migrator = EventMigrator(self.ctx)
        source_events = source_asset.search_events(origin_types=SearchEventOriginType.get_manual_origin_types())
        for source_event in source_events:
            event_migrator.copy_from(source_event, EventCopyOptions(new_assets=[destination_asset]))

    def _copy_asset_runs(self, source_asset: Asset, destination_asset: Asset) -> None:
        run_migrator = RunMigrator(self.ctx)
        for source_run in source_asset.list_runs():
            run_migrator.copy_from(source_run, RunCopyOptions(new_assets=[destination_asset]))

    def _copy_asset_checklists(self, source_asset: Asset) -> None:
        checklist_migrator = ChecklistMigrator(self.ctx)
        for source_data_review in source_asset.search_data_reviews():
            source_checklist = source_data_review.get_checklist()
            logger.debug("Found Data Review %s", source_checklist.rid)
            destination_checklist = checklist_migrator.copy_from(source_checklist, ChecklistCopyOptions())
            destination_run_rid = self.ctx.migration_state.get_mapped_rid(ResourceType.RUN, source_data_review.run_rid)
            if destination_run_rid is None:
                logger.warning(
                    "Run %s not found in migration state for data review checklist %s — skipping",
                    source_data_review.run_rid,
                    source_checklist.rid,
                )
                continue
            if self.ctx.migration_state.get_mapped_rid(ResourceType.DATA_REVIEW, source_data_review.rid) is None:
                new_data_review = destination_checklist.execute(destination_run_rid)
                self.ctx.migration_state.record_mapping(
                    ResourceType.DATA_REVIEW, source_data_review.rid, new_data_review.rid
                )
            else:
                logger.debug(
                    "Skipping data review execution for %s: already in migration state", source_data_review.rid
                )

    def _copy_asset_attachments(self, source_asset: Asset, destination_asset: Asset) -> None:
        attachment_migrator = AttachmentMigrator(self.ctx)
        new_attachments = [attachment_migrator.copy_from(a) for a in source_asset.list_attachments()]
        if new_attachments:
            destination_asset.add_attachments(new_attachments)

    def _copy_asset_videos(self, source_asset: Asset, new_asset: Asset) -> None:
        video_migrator = VideoMigrator(self.ctx)
        for data_scope, video_dataset in source_asset.list_videos():
            new_video_dataset = video_migrator.copy_from(
                video_dataset,
                VideoCopyOptions(
                    include_files=True,
                ),
            )
            scope_key = f"{source_asset.rid}:{data_scope}"
            if self.ctx.migration_state.get_mapped_rid(ResourceType.ASSET_DATA_SCOPE, scope_key) is None:
                new_asset.add_video(data_scope, new_video_dataset)
                self.ctx.migration_state.record_mapping(ResourceType.ASSET_DATA_SCOPE, scope_key, new_video_dataset.rid)
            else:
                logger.debug(
                    "Skipping add_video for scope %s on asset %s: already in migration state",
                    data_scope,
                    source_asset.rid,
                )

    def _copy_asset_and_run_workbooks(self, source_asset: Asset, new_asset: Asset, include_runs: bool) -> None:
        workbook_migrator = WorkbookMigrator(self.ctx)
        asset_workbooks = source_asset.search_workbooks(include_drafts=True)
        for workbook in asset_workbooks:
            if not workbook.asset_rids:
                continue
            if len(workbook.asset_rids) == 1:
                workbook_migrator.copy_from(workbook, WorkbookCopyOptions(destination_asset=new_asset))
            else:
                self._enqueue_multi_asset_workbook(workbook, list(workbook.asset_rids))

        if include_runs:
            for source_run in source_asset.list_runs():
                destination_run_rid = self.ctx.migration_state.get_mapped_rid(ResourceType.RUN, source_run.rid)
                if destination_run_rid is None:
                    logger.warning("Run %s not found in migration state", source_run.rid)
                    continue
                destination_run = self.ctx.destination_client_for(source_run).get_run(destination_run_rid)
                for workbook in source_run.search_workbooks(include_drafts=True):
                    if not workbook.run_rids:
                        continue
                    if len(workbook.run_rids) == 1:
                        workbook_migrator.copy_from(workbook, WorkbookCopyOptions(destination_run=destination_run))
                    else:
                        self._enqueue_multi_run_workbook(workbook, list(workbook.run_rids))

    def _enqueue_multi_asset_workbook(self, workbook: Workbook, source_asset_rids: list[str]) -> None:
        missing = [
            rid
            for rid in source_asset_rids
            if rid not in self.ctx.source_asset_rids
            and self.ctx.migration_state.get_mapped_rid(ResourceType.ASSET, rid) is None
        ]
        if missing:
            reason = f"assets not in migration scope: {missing}"
            logger.warning("Skipping multi-asset workbook %s: %s", workbook.rid, reason)
            self.ctx.migration_state.record_skip(ResourceType.WORKBOOK, workbook.rid, reason)
        else:
            logger.debug("Queuing multi-asset workbook %s for deferred migration", workbook.rid)
            self.ctx.migration_state.record_pending_multi_asset_workbook(workbook.rid, source_asset_rids)

    def _enqueue_multi_run_workbook(self, workbook: Workbook, source_run_rids: list[str]) -> None:
        logger.debug("Queuing multi-run workbook %s for deferred migration", workbook.rid)
        self.ctx.migration_state.record_pending_multi_run_workbook(workbook.rid, source_run_rids)
