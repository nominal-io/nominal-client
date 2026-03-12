from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Sequence

from nominal.core._event_types import SearchEventOriginType
from nominal.core.asset import Asset
from nominal.core.checklist import Checklist
from nominal.core.dataset import Dataset
from nominal.experimental.migration.config.migration_data_config import MigrationDatasetConfig
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.migrator.checklist_migrator import ChecklistCopyOptions, ChecklistMigrator
from nominal.experimental.migration.migrator.context import MigrationContext
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
    old_to_new_dataset_rid_mapping: dict[str, str] = field(default_factory=dict)
    include_events: bool = False
    include_runs: bool = False
    include_video: bool = False
    include_checklists: bool = False


class AssetMigrator(Migrator[Asset, AssetCopyOptions]):
    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.ASSET

    def default_copy_options(self) -> AssetCopyOptions:
        return AssetCopyOptions(
            dataset_config=MigrationDatasetConfig(preserve_dataset_uuid=True, include_dataset_files=True),
            include_events=True,
            include_runs=True,
            include_video=True,
        )

    def _copy_from_impl(self, source: Asset, options: AssetCopyOptions) -> Asset:
        if options.include_checklists and not options.include_runs:
            raise ValueError("include_checklists set to True requires include_runs to be set to True.")
        new_asset = self.ctx.destination_client.create_asset(
            name=options.new_asset_name if options.new_asset_name is not None else source.name,
            description=options.new_asset_description
            if options.new_asset_description is not None
            else source.description,
            properties=options.new_asset_properties if options.new_asset_properties is not None else source.properties,
            labels=options.new_asset_labels if options.new_asset_labels is not None else source.labels,
        )

        self._copy_asset_datasets(source, new_asset, options)

        if options.include_events:
            logger.info("Copying events for asset %s (rid: %s)", source.name, source.rid)
            self._copy_asset_events(source, new_asset)

        run_mapping = self._copy_optional_runs(source, new_asset, options)

        if options.include_checklists:
            logger.info("Copying checklists for asset %s (rid: %s)", source.name, source.rid)
            self._copy_asset_checklists(source, run_mapping)

        if options.include_video:
            logger.info("Copying videos for asset %s (rid: %s)", source.name, source.rid)
            self._copy_asset_videos(source, new_asset)

        self._copy_asset_and_run_workbooks(source, new_asset, run_mapping)
        return new_asset

    def _get_resource_name(self, resource: Asset) -> str:
        return resource.name

    def _copy_asset_datasets(self, source_asset: Asset, new_asset: Asset, options: AssetCopyOptions) -> None:
        dataset_config = options.dataset_config
        if dataset_config is None:
            return

        dataset_migrator = DatasetMigrator(
            MigrationContext(
                destination_client=self.ctx.destination_client,
                migration_state=self.ctx.migration_state,
            )
        )
        dataset_mapping = options.old_to_new_dataset_rid_mapping
        for data_scope, source_dataset in source_asset.list_datasets():
            new_dataset = self._resolve_destination_dataset(
                source_dataset, dataset_config, dataset_mapping, dataset_migrator
            )
            dataset_mapping[source_dataset.rid] = new_dataset.rid
            new_asset.add_dataset(data_scope, new_dataset)

    def _resolve_destination_dataset(
        self,
        source_dataset: Dataset,
        dataset_config: MigrationDatasetConfig,
        dataset_mapping: dict[str, str],
        dataset_migrator: DatasetMigrator,
    ) -> Dataset:
        if source_dataset.rid in dataset_mapping:
            new_dataset_rid = dataset_mapping[source_dataset.rid]
            return self.ctx.destination_client.get_dataset(new_dataset_rid)

        return dataset_migrator.copy_from(
            source_dataset,
            DatasetCopyOptions(
                include_files=dataset_config.include_dataset_files,
                preserve_uuid=dataset_config.preserve_dataset_uuid,
            ),
        )

    def _copy_asset_events(self, source_asset: Asset, new_asset: Asset) -> None:
        event_migrator = EventMigrator(
            MigrationContext(
                destination_client=self.ctx.destination_client,
                migration_state=self.ctx.migration_state,
            )
        )
        source_events = source_asset.search_events(origin_types=SearchEventOriginType.get_manual_origin_types())
        for source_event in source_events:
            event_migrator.copy_from(source_event, EventCopyOptions(new_assets=[new_asset]))

    def _copy_optional_runs(self, source_asset: Asset, new_asset: Asset, options: AssetCopyOptions) -> Dict[str, str]:
        if not options.include_runs:
            return {}

        logger.info("Copying runs for asset %s (rid: %s)", source_asset.name, source_asset.rid)
        return self._copy_asset_runs(source_asset, new_asset)

    def _copy_asset_runs(self, source_asset: Asset, new_asset: Asset) -> Dict[str, str]:
        run_mapping: Dict[str, str] = {}
        run_migrator = RunMigrator(
            MigrationContext(
                destination_client=self.ctx.destination_client,
                migration_state=self.ctx.migration_state,
            )
        )
        source_runs = source_asset.list_runs()
        for source_run in source_runs:
            new_run = run_migrator.copy_from(source_run, RunCopyOptions(new_assets=[new_asset]))
            run_mapping[source_run.rid] = new_run.rid
        return run_mapping

    def _copy_asset_checklists(self, source_asset: Asset, run_mapping: Dict[str, str]) -> None:
        checklist_migrator = ChecklistMigrator(
            MigrationContext(
                destination_client=self.ctx.destination_client,
                migration_state=self.ctx.migration_state,
            )
        )
        source_checklist_rid_to_destination_checklist_map: Dict[str, Checklist] = {}
        for source_data_review in source_asset.search_data_reviews():
            source_checklist = source_data_review.get_checklist()
            logger.debug("Found Data Review %s", source_checklist.rid)
            destination_checklist = self._resolve_destination_checklist(
                source_checklist,
                source_checklist_rid_to_destination_checklist_map,
                checklist_migrator,
            )
            if source_data_review.run_rid not in run_mapping:
                logger.warning(
                    "Run %s not found in run mapping for data review checklist %s — skipping",
                    source_data_review.run_rid,
                    source_checklist.rid,
                )
                continue
            destination_checklist.execute(run_mapping[source_data_review.run_rid])

    def _resolve_destination_checklist(
        self,
        source_checklist: Checklist,
        checklist_mapping: Dict[str, Checklist],
        checklist_migrator: ChecklistMigrator,
    ) -> Checklist:
        if source_checklist.rid in checklist_mapping:
            return checklist_mapping[source_checklist.rid]

        destination_checklist = checklist_migrator.copy_from(source_checklist, ChecklistCopyOptions())
        checklist_mapping[source_checklist.rid] = destination_checklist
        return destination_checklist

    def _copy_asset_videos(self, source_asset: Asset, new_asset: Asset) -> None:
        video_migrator = VideoMigrator(
            MigrationContext(
                destination_client=self.ctx.destination_client,
                migration_state=self.ctx.migration_state,
            )
        )
        for data_scope, video_dataset in source_asset.list_videos():
            new_video_dataset = video_migrator.copy_from(
                video_dataset,
                VideoCopyOptions(
                    include_files=True,
                ),
            )
            new_asset.add_video(data_scope, new_video_dataset)

    def _copy_asset_and_run_workbooks(
        self,
        source_asset: Asset,
        new_asset: Asset,
        run_mapping: Dict[str, str] | None = None,
    ) -> None:
        workbook_migrator = WorkbookMigrator(
            MigrationContext(
                destination_client=self.ctx.destination_client,
                migration_state=self.ctx.migration_state,
            )
        )
        asset_workbooks = source_asset.search_workbooks(include_drafts=True)
        for workbook in asset_workbooks:
            if workbook.asset_rids and len(workbook.asset_rids) == 1:
                workbook_migrator.copy_from(workbook, WorkbookCopyOptions(destination_asset=new_asset))

        if run_mapping:
            for source_run in source_asset.list_runs():
                if source_run.rid not in run_mapping:
                    logger.warning("Run %s not found in run mapping", source_run.rid)
                    continue
                destination_run = self.ctx.destination_client.get_run(run_mapping[source_run.rid])
                for workbook in source_run.search_workbooks(include_drafts=True):
                    if workbook.run_rids and len(workbook.run_rids) == 1:
                        workbook_migrator.copy_from(workbook, WorkbookCopyOptions(destination_run=destination_run))
