from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Sequence

from nominal.core.dataset import Dataset
from nominal.experimental.dataset_utils import create_dataset_with_uuid
from nominal.experimental.id_utils.id_utils import UUID_PATTERN
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.resource_type import ResourceType
from nominal.experimental.migration.utils.file_utils import copy_file_to_dataset

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatasetCopyOptions(ResourceCopyOptions):
    new_dataset_name: str | None = None
    new_dataset_description: str | None = None
    new_dataset_properties: dict[str, Any] | None = None
    new_dataset_labels: Sequence[str] | None = None
    include_files: bool = False
    preserve_uuid: bool = False


class DatasetMigrator(Migrator[Dataset, DatasetCopyOptions]):
    resource_type = ResourceType.DATASET

    def default_copy_options(self) -> DatasetCopyOptions:
        return DatasetCopyOptions(include_files=True)

    def _copy_from_impl(self, source: Dataset, options: DatasetCopyOptions) -> Dataset:
        log_extras = {
            "destination_client_workspace": self.ctx.destination_client.get_workspace(
                self.ctx.destination_client._clients.workspace_rid
            ).rid
        }
        dataset_name = options.new_dataset_name if options.new_dataset_name is not None else source.name
        dataset_description = (
            options.new_dataset_description if options.new_dataset_description is not None else source.description
        )
        dataset_properties = (
            options.new_dataset_properties if options.new_dataset_properties is not None else source.properties
        )
        dataset_labels = options.new_dataset_labels if options.new_dataset_labels is not None else source.labels

        if options.preserve_uuid:
            match = UUID_PATTERN.search(source.rid)
            if not match:
                raise ValueError(f"Could not extract UUID from dataset rid: {source.rid}")
            source_uuid = match.group(2)
            new_dataset = create_dataset_with_uuid(
                client=self.ctx.destination_client,
                dataset_uuid=source_uuid,
                name=dataset_name,
                description=dataset_description,
                labels=dataset_labels,
                properties=dataset_properties,
            )
        else:
            new_dataset = self.ctx.destination_client.create_dataset(
                name=dataset_name,
                description=dataset_description,
                properties=dataset_properties,
                labels=dataset_labels,
            )

        if options.preserve_uuid:
            channels_copied_count = 0
            for source_channel in source.search_channels():
                if source_channel.data_type is None:
                    logger.warning("Skipping channel %s: unknown data type", source_channel.name, extra=log_extras)
                    continue
                new_dataset.add_channel(
                    name=source_channel.name,
                    data_type=source_channel.data_type,
                    description=source_channel.description,
                    unit=source_channel.unit,
                )
                channels_copied_count += 1
            logger.info("Copied %d channels from dataset %s", channels_copied_count, source.name, extra=log_extras)

        if options.include_files:
            for source_file in source.list_files():
                copy_file_to_dataset(source_file, new_dataset)

        if source.bounds is not None:
            new_dataset = new_dataset.update_bounds(
                start=source.bounds.start,
                end=source.bounds.end,
            )
        return new_dataset

    def _get_resource_name(self, resource: Dataset) -> str:
        return resource.name

    def _get_resource_rid(self, resource: Dataset) -> str:
        return resource.rid
