from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from nominal.core import NominalClient
from nominal.core.dataset import Dataset
from nominal.core.datasource import CreateChannelRequest
from nominal.experimental.dataset_utils import create_dataset_with_uuid
from nominal.experimental.id_utils.id_utils import UUID_PATTERN
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.migrator.dataset_file_migrator import DatasetFileMigrator
from nominal.experimental.migration.resource_type import ResourceType

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
    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.DATASET

    def use_singleflight(self) -> bool:
        return True

    def default_copy_options(self) -> DatasetCopyOptions:
        return DatasetCopyOptions(include_files=True)

    def _copy_from_impl(self, source: Dataset, options: DatasetCopyOptions) -> Dataset:
        new_dataset = self._resolve_destination_dataset(source, options)

        if options.include_files:
            file_migrator = DatasetFileMigrator(self.ctx)
            for source_file in source.list_files():
                file_migrator.copy_from(source_file, new_dataset)

        return new_dataset

    def _get_existing_destination_resource(self, destination_client: NominalClient, mapped_rid: str) -> Dataset:
        return destination_client.get_dataset(mapped_rid)

    def _resolve_destination_dataset(self, source: Dataset, options: DatasetCopyOptions) -> Dataset:
        existing_dataset = self.get_existing_destination_resource(source)
        if existing_dataset is not None:
            return existing_dataset

        destination_client = self.destination_client_for(source)
        log_extras = {
            "destination_client_workspace": destination_client.get_workspace(
                destination_client._clients.workspace_rid
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

        new_dataset = self._create_destination_dataset(
            source,
            options,
            dataset_name,
            dataset_description,
            dataset_properties,
            dataset_labels,
        )
        self.ctx.migration_state.record_mapping(self.resource_type, source.rid, new_dataset.rid)

        if options.preserve_uuid:
            channels_to_add = []
            for source_channel in source.search_channels():
                if source_channel.data_type is None:
                    logger.warning("Skipping channel %s: unknown data type", source_channel.name, extra=log_extras)
                    continue
                channel_key = f"{source.rid}:{source_channel.name}"
                if self.ctx.migration_state.get_mapped_rid(ResourceType.DATASET_CHANNEL, channel_key) is None:
                    channels_to_add.append(
                        CreateChannelRequest(
                            name=source_channel.name,
                            data_type=source_channel.data_type,
                            description=source_channel.description,
                            unit=source_channel.unit,
                        )
                    )
                else:
                    logger.debug(
                        "Skipping channel %s on dataset %s: already in migration state",
                        source_channel.name,
                        source.rid,
                    )
            result = new_dataset.batch_add_channels(channels_to_add)
            for req in result.missing:
                logger.warning(
                    "Channel %s on dataset %s was not found after creation and will not be recorded in migration state",
                    req.name,
                    source.rid,
                    extra=log_extras,
                )
            for channel in result.channels:
                channel_key = f"{source.rid}:{channel.name}"
                self.ctx.migration_state.record_mapping(ResourceType.DATASET_CHANNEL, channel_key, channel.name)
            logger.info("Copied %d channels from dataset %s", len(result.channels), source.name, extra=log_extras)

        if source.bounds is not None:
            if self.ctx.migration_state.get_mapped_rid(ResourceType.DATASET_BOUNDS, source.rid) is None:
                new_dataset = new_dataset.update_bounds(
                    start=source.bounds.start,
                    end=source.bounds.end,
                )
                self.ctx.migration_state.record_mapping(ResourceType.DATASET_BOUNDS, source.rid, new_dataset.rid)
            else:
                logger.debug("Skipping bounds update for dataset %s: already in migration state", source.rid)

        return new_dataset

    def _create_destination_dataset(
        self,
        source: Dataset,
        options: DatasetCopyOptions,
        dataset_name: str,
        dataset_description: str | None,
        dataset_properties: Mapping[str, str] | dict[str, Any],
        dataset_labels: Sequence[str],
    ) -> Dataset:
        if options.preserve_uuid:
            match = UUID_PATTERN.search(source.rid)
            if not match:
                raise ValueError(f"Could not extract UUID from dataset rid: {source.rid}")
            source_uuid = match.group(2)
            return create_dataset_with_uuid(
                client=self.destination_client_for(source),
                dataset_uuid=source_uuid,
                name=dataset_name,
                description=dataset_description,
                labels=dataset_labels,
                properties=dataset_properties,
            )

        return self.destination_client_for(source).create_dataset(
            name=dataset_name,
            description=dataset_description,
            properties=dataset_properties,
            labels=dataset_labels,
        )

    def _get_resource_name(self, resource: Dataset) -> str:
        return resource.name
