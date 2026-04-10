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
from nominal.experimental.migration.migrator.context import DestinationClientResolver, MigrationContext
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
    destination_client_resolver: DestinationClientResolver | None

    def __init__(
        self,
        migration_resources: MigrationResources,
        dataset_config: MigrationDatasetConfig,
        destination_client: NominalClient,
        destination_client_resolver: DestinationClientResolver | None = None,
        migration_state_path: Path | str | None = None,
    ) -> None:
        """Create a migration runner state.

        Args:
            migration_resources (MigrationResources): _description_
            dataset_config (MigrationDatasetConfig): _description_
            destination_client (NominalClient): _description_
            destination_client_resolver (DestinationClientResolver | None): Optional callback that resolves the
                destination client for each source resource. Defaults to None.
            migration_state_path (Path | str | None, optional): _description_. Defaults to None.
        """
        self.migration_resources = migration_resources
        self.dataset_config = dataset_config
        self.destination_client = destination_client
        self.destination_client_resolver = destination_client_resolver
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
            migration_context = MigrationContext(
                destination_client=self.destination_client,
                migration_state=self.migration_state,
                destination_client_resolver=self.destination_client_resolver,
            )
            asset_migrator = AssetMigrator(migration_context)
            template_migrator = WorkbookTemplateMigrator(migration_context)
            for asset_resources in self.migration_resources.source_assets.values():
                source_asset = asset_resources.asset
                asset_migrator.copy_from(
                    source_asset,
                    AssetCopyOptions(
                        dataset_config=self.dataset_config,
                        include_events=True,
                        include_runs=True,
                        include_video=True,
                        include_checklists=True,
                    ),
                )

            for source_template in self.migration_resources.source_standalone_templates:
                template_migrator.clone(source_template)
        finally:
            self.save_state()
        logger.info("Completed migration")

    def save_state(self) -> None:
        self.migration_state_path.parent.mkdir(parents=True, exist_ok=True)
        self.migration_state_path.write_text(self.migration_state.to_json(), encoding="utf-8")
