from __future__ import annotations

import json
import logging
import re
from pathlib import Path

from nominal.core import NominalClient
from nominal.experimental.migration.config.migration_data_config import MigrationDatasetConfig
from nominal.experimental.migration.config.migration_resources import MigrationResources
from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.migrator.asset_migrator import AssetCopyOptions, AssetMigrator
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.migrator.workbook_template_migrator import WorkbookTemplateMigrator

logger = logging.getLogger(__name__)


def _next_state_path(path: Path) -> Path:
    match = re.match(r"^(.+)_v(\d+)$", path.stem)
    if match:
        new_stem = f"{match.group(1)}_v{int(match.group(2)) + 1}"
    else:
        new_stem = f"{path.stem}_v2"
    return path.parent / f"{new_stem}{path.suffix}"


class MigrationRunner:
    migration_state_path: Path
    migration_state: MigrationState
    migration_resources: MigrationResources
    dataset_config: MigrationDatasetConfig
    destination_client: NominalClient

    def __init__(
        self,
        migration_resources: MigrationResources,
        dataset_config: MigrationDatasetConfig,
        destination_client: NominalClient,
        migration_state_path: Path | str | None = None,
    ) -> None:
        """Create a migration runner state.

        Args:
            migration_resources (MigrationResources): _description_
            dataset_config (MigrationDatasetConfig): _description_
            destination_client (NominalClient): _description_
            migration_state_path (Path | str | None, optional): _description_. Defaults to None.

        Raises:
            ValueError: _description_
        """
        self.migration_resources = migration_resources
        self.dataset_config = dataset_config
        self.destination_client = destination_client
        resolved_path = Path(migration_state_path) if migration_state_path is not None else Path("migration_state.json")

        if migration_state_path is not None and resolved_path.exists():
            self.migration_state = MigrationState.from_dict(json.loads(resolved_path.read_text(encoding="utf-8")))
            if self.migration_state.rid_mapping:
                self.migration_state_path = _next_state_path(resolved_path)
            else:
                self.migration_state_path = resolved_path
        else:
            self.migration_state = MigrationState(rid_mapping={})
            self.migration_state_path = resolved_path

    def run_migration(self) -> None:
        """Based on a list of assets and workbook templates, copy resources to destination client, creating
        new datasets, datafiles, and workbooks along the way. Standalone templates are cloned without
        creating workbooks.

        Args:
        destination_client (NominalClient): client of the tenant/workspace to copy resources to.
        migration_resources (MigrationResources): resources to copy.
        dataset_config (MigrationDataConfig | None): Configuration for dataset migration.
        """
        try:
            log_extras = {
                "destination_client_workspace": self.destination_client.get_workspace(
                    self.destination_client._clients.workspace_rid
                ).rid,
            }

            asset_migrator = AssetMigrator(
                MigrationContext(destination_client=self.destination_client, migration_state=self.migration_state)
            )
            template_migrator = WorkbookTemplateMigrator(
                MigrationContext(destination_client=self.destination_client, migration_state=self.migration_state)
            )
            for asset_resources in self.migration_resources.source_assets.values():
                source_asset = asset_resources.asset
                new_asset = asset_migrator.copy_from(
                    source_asset,
                    AssetCopyOptions(
                        dataset_config=self.dataset_config,
                        include_events=True,
                        include_runs=True,
                        include_video=True,
                        include_checklists=True,
                    ),
                )

                for source_workbook_template in asset_resources.source_workbook_templates:
                    new_template = template_migrator.clone(source_workbook_template)
                    new_workbook = new_template.create_workbook(
                        title=new_template.title, description=new_template.description, asset=new_asset
                    )
                    logger.debug(
                        "Created new workbook %s (rid: %s) from template %s (rid: %s)",
                        new_workbook.title,
                        new_workbook.rid,
                        new_template.title,
                        new_template.rid,
                        extra=log_extras,
                    )

            for source_template in self.migration_resources.source_standalone_templates:
                new_template = template_migrator.clone(source_template)
        finally:
            self.save_state()
        logger.info("Completed migration")

    def save_state(self) -> None:
        self.migration_state_path.parent.mkdir(parents=True, exist_ok=True)
        self.migration_state_path.write_text(self.migration_state.to_json(), encoding="utf-8")
