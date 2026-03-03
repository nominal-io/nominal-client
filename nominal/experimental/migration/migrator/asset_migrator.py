from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Sequence

from nominal.core._event_types import SearchEventOriginType
from nominal.core.asset import Asset
from nominal.core.checklist import Checklist
from nominal.experimental.migration.config.migration_data_config import MigrationDatasetConfig
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.migrator.checklist_migrator import ChecklistCopyOptions, ChecklistMigrator
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.migrator.dataset_migrator import DatasetCopyOptions, DatasetMigrator
from nominal.experimental.migration.migrator.event_migrator import EventCopyOptions, EventMigrator
from nominal.experimental.migration.migrator.run_migrator import RunCopyOptions, RunMigrator
from nominal.experimental.migration.migrator.workbook_migrator import WorkbookMigrator
from nominal.experimental.migration.utils.video_file_utils import copy_video_file_to_video_dataset

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


class AssetMigrator(Migrator[Asset, Asset, AssetCopyOptions]):
    def clone(self, source: Asset) -> Asset:
        return self.copy_from(
            source,
            AssetCopyOptions(
                dataset_config=MigrationDatasetConfig(preserve_dataset_uuid=True, include_dataset_files=True),
                include_events=True,
                include_runs=True,
                include_video=True,
            ),
        )

    def copy_from(self, source: Asset, options: AssetCopyOptions) -> Asset:
        if options.include_checklists and not options.include_runs:
            raise ValueError("include_checklists set to True requires include_runs to be set to True.")

        log_extras = {
            "destination_client_workspace": self.ctx.destination_client.get_workspace(
                self.ctx.destination_client._clients.workspace_rid
            ).rid
        }
        logger.debug(
            "Copying asset %s (rid: %s)",
            source.name,
            source.rid,
            extra=log_extras,
        )
        new_asset = self.ctx.destination_client.create_asset(
            name=options.new_asset_name if options.new_asset_name is not None else source.name,
            description=options.new_asset_description
            if options.new_asset_description is not None
            else source.description,
            properties=options.new_asset_properties if options.new_asset_properties is not None else source.properties,
            labels=options.new_asset_labels if options.new_asset_labels is not None else source.labels,
        )

        resolved_dataset_config = options.dataset_config
        if resolved_dataset_config is not None:
            dataset_migrator = DatasetMigrator(
                MigrationContext(
                    destination_client=self.ctx.destination_client,
                    migration_state=self.ctx.migration_state,
                )
            )
            source_datasets = source.list_datasets()
            dataset_mapping = options.old_to_new_dataset_rid_mapping
            for data_scope, source_dataset in source_datasets:
                if source_dataset.rid in dataset_mapping:
                    new_dataset_rid = dataset_mapping[source_dataset.rid]
                    new_dataset = self.ctx.destination_client.get_dataset(new_dataset_rid)
                else:
                    new_dataset = dataset_migrator.copy_from(
                        source_dataset,
                        DatasetCopyOptions(
                            include_files=resolved_dataset_config.include_dataset_files,
                            preserve_uuid=resolved_dataset_config.preserve_dataset_uuid,
                        ),
                    )
                dataset_mapping[source_dataset.rid] = new_dataset.rid
                new_asset.add_dataset(data_scope, new_dataset)

        run_mapping: Dict[str, str] = {}

        if options.include_events:
            self._copy_asset_events(source, new_asset)

        if options.include_runs:
            run_mapping = self._copy_asset_runs(source, new_asset)

        if options.include_checklists:
            self._copy_asset_checklists(source, run_mapping)

        if options.include_video:
            self._copy_asset_videos(source, new_asset)

        self._copy_asset_and_run_workbooks(source, new_asset, run_mapping)

        logger.debug("New asset created: %s (rid: %s)", new_asset, new_asset.rid, extra=log_extras)
        self.record_mapping("ASSET", source.rid, new_asset.rid)
        return new_asset

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
            if source_checklist.rid not in source_checklist_rid_to_destination_checklist_map:
                destination_checklist = checklist_migrator.copy_from(source_checklist, ChecklistCopyOptions())
                source_checklist_rid_to_destination_checklist_map[source_checklist.rid] = destination_checklist
            else:
                destination_checklist = source_checklist_rid_to_destination_checklist_map[source_checklist.rid]
            destination_checklist.execute(run_mapping[source_data_review.run_rid])

    def _copy_asset_videos(self, source_asset: Asset, new_asset: Asset) -> None:
        for data_scope, video_dataset in source_asset.list_videos():
            new_video_dataset = self.ctx.destination_client.create_video(
                name=video_dataset.name,
                description=video_dataset.description,
                properties=video_dataset.properties,
                labels=video_dataset.labels,
            )
            new_asset.add_video(data_scope, new_video_dataset)
            for source_video_file in video_dataset.list_files():
                copy_video_file_to_video_dataset(source_video_file, new_video_dataset)

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
        workbook_migrator.copy_asset_and_run_workbooks(source_asset, new_asset, run_mapping)
