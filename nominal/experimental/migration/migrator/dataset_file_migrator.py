from __future__ import annotations

import logging

from nominal.core.dataset import Dataset
from nominal.core.dataset_file import DatasetFile
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.resource_type import ResourceType
from nominal.experimental.migration.utils.file_utils import copy_file_to_dataset

logger = logging.getLogger(__name__)


class DatasetFileMigrator:
    def __init__(self, ctx: MigrationContext) -> None:
        """Constructs a DatasetFileMigrator with the given MigrationContext."""
        self.ctx = ctx

    def copy_from(self, source_file: DatasetFile, destination_dataset: Dataset) -> None:
        mapped_id = self.ctx.migration_state.get_mapped_rid(ResourceType.DATASET_FILE, source_file.id)
        if mapped_id is not None:
            logger.debug("Skipping dataset file (id: %s): already in migration state", source_file.id)
            return

        new_file = copy_file_to_dataset(source_file, destination_dataset)
        self.ctx.migration_state.record_mapping(ResourceType.DATASET_FILE, source_file.id, new_file.id)
