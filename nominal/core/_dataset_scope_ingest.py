"""Ingest into a named dataset scope while applying its tags."""

from __future__ import annotations

import abc
from typing import BinaryIO, Iterable, Mapping, overload

from nominal.core._types import PathLike
from nominal.core.containerized_extractor import ContainerizedExtractor
from nominal.core.dataset import Dataset
from nominal.core.dataset_file import DatasetFile
from nominal.core.filetype import FileType
from nominal.core.ingestion_job import IngestionJob
from nominal.ts import (
    _AnyEpochTimestampType,
    _AnyNumericTimestampType,
    _AnyTimestampType,
    _validate_timestamp_pair,
)


class _DatasetScopeIngestMixin(abc.ABC):
    """Delegate scoped ingest to Dataset, merging scope tags with caller tags.

    Subclasses resolve the dataset and scope tags. Caller tags take precedence.
    """

    @abc.abstractmethod
    def _get_dataset_scope(self, data_scope_name: str) -> tuple[Dataset, Mapping[str, str]]:
        """Resolve the named dataset and its scope tags."""

    ################
    # Add Data API #
    ################

    def add_tabular_data(
        self,
        data_scope_name: str,
        path: PathLike,
        *,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        tag_columns: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> DatasetFile:
        """Append tabular data on-disk to the dataset selected by `data_scope_name`.

        This method behaves like `nominal.core.Dataset.add_tabular_data`, except that the data scope's required
        tags are merged into `tags` before ingest (with user-provided tags taking precedence on key collisions).

        For supported file types, argument semantics, and return value details, see
        `nominal.core.Dataset.add_tabular_data`.
        """
        dataset, scope_tags = self._get_dataset_scope(data_scope_name)
        return dataset.add_tabular_data(
            path,
            timestamp_column=timestamp_column,
            timestamp_type=timestamp_type,
            tag_columns=tag_columns,
            tags=_merge_scope_tags(scope_tags, tags),
        )

    def add_avro_stream(
        self,
        data_scope_name: str,
        path: PathLike,
        *,
        timestamp_type: _AnyNumericTimestampType | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> DatasetFile:
        """Upload an avro stream file to the dataset selected by `data_scope_name`.

        This method behaves like `nominal.core.Dataset.add_avro_stream`, except that the data scope's required
        tags are merged into `tags` before ingest (with user-provided tags taking precedence on key collisions).
        Tags in the avro records take precedence over both, so a scope tag whose key a record already sets is
        not applied to that record's data.

        For schema requirements, argument semantics, and return value details, see
        `nominal.core.Dataset.add_avro_stream`.
        """
        dataset, scope_tags = self._get_dataset_scope(data_scope_name)
        return dataset.add_avro_stream(path, timestamp_type=timestamp_type, tags=_merge_scope_tags(scope_tags, tags))

    @overload
    def add_journal_json(self, data_scope_name: str, path: PathLike, *, channel: str | None = ...) -> DatasetFile: ...
    @overload
    def add_journal_json(
        self,
        data_scope_name: str,
        path: PathLike,
        *,
        channel: str | None = ...,
        timestamp_column: str,
        timestamp_type: _AnyEpochTimestampType,
    ) -> DatasetFile: ...
    def add_journal_json(
        self,
        data_scope_name: str,
        path: PathLike,
        *,
        channel: str | None = None,
        timestamp_column: str | None = None,
        timestamp_type: _AnyEpochTimestampType | None = None,
    ) -> DatasetFile:
        """Add a journald json file to the dataset selected by `data_scope_name`.

        This method behaves like `nominal.core.Dataset.add_journal_json`, with one important difference: the
        journal json ingest request has no field for tags, so a scope's required tags cannot be applied to the
        ingested logs. If the selected scope requires tags, this method raises `RuntimeError` rather than
        ingesting logs that would be missing them. The file can still be ingested on the dataset directly, if
        its own contents already carry what the scope needs.

        Pass both `timestamp_column` and `timestamp_type`, or neither; this wrapper enforces that before
        delegating.

        For file expectations, timestamp argument semantics, and return value details, see
        `nominal.core.Dataset.add_journal_json`.
        """
        _validate_timestamp_pair(timestamp_column, timestamp_type)

        dataset, scope_tags = self._get_dataset_scope(data_scope_name)

        # TODO(drake): remove once journal json supports ingest with tags
        if scope_tags:
            raise RuntimeError(
                f"Cannot add journal json files to datascope {data_scope_name}: journal ingest cannot apply "
                f"tags, so the logs would not get the scope's required tags {scope_tags}"
            )

        if timestamp_column is not None and timestamp_type is not None:
            return dataset.add_journal_json(
                path, channel=channel, timestamp_column=timestamp_column, timestamp_type=timestamp_type
            )
        return dataset.add_journal_json(path, channel=channel)

    def add_mcap(
        self,
        data_scope_name: str,
        path: PathLike,
        *,
        include_topics: Iterable[str] | None = None,
        exclude_topics: Iterable[str] | None = None,
        tags: Mapping[str, str] | None = None,
        ignore_invalid_topics: bool | None = None,
    ) -> DatasetFile:
        """Add an MCAP file to the dataset selected by `data_scope_name`.

        This method behaves like `nominal.core.Dataset.add_mcap`, except that the data scope's required
        tags are merged into `tags` before ingest (with user-provided tags taking precedence on key collisions).

        For topic-filtering semantics and return value details, see
        `nominal.core.Dataset.add_mcap`.
        """
        dataset, scope_tags = self._get_dataset_scope(data_scope_name)
        return dataset.add_mcap(
            path,
            include_topics=include_topics,
            exclude_topics=exclude_topics,
            tags=_merge_scope_tags(scope_tags, tags),
            ignore_invalid_topics=ignore_invalid_topics,
        )

    def add_ardupilot_dataflash(
        self,
        data_scope_name: str,
        path: PathLike,
        tags: Mapping[str, str] | None = None,
    ) -> DatasetFile:
        """Add a Dataflash file to the dataset selected by `data_scope_name`.

        This method behaves like `nominal.core.Dataset.add_ardupilot_dataflash`, except that the data scope's
        required tags are merged into `tags` before ingest (with user-provided tags taking precedence on key
        collisions).

        For file expectations and return value details, see
        `nominal.core.Dataset.add_ardupilot_dataflash`.
        """
        dataset, scope_tags = self._get_dataset_scope(data_scope_name)
        return dataset.add_ardupilot_dataflash(path, tags=_merge_scope_tags(scope_tags, tags))

    @overload
    def add_containerized(
        self,
        data_scope_name: str,
        extractor: str | ContainerizedExtractor,
        sources: Mapping[str, PathLike],
        *,
        arguments: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> IngestionJob: ...
    @overload
    def add_containerized(
        self,
        data_scope_name: str,
        extractor: str | ContainerizedExtractor,
        sources: Mapping[str, PathLike],
        *,
        arguments: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
    ) -> IngestionJob: ...
    def add_containerized(
        self,
        data_scope_name: str,
        extractor: str | ContainerizedExtractor,
        sources: Mapping[str, PathLike],
        *,
        arguments: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        timestamp_column: str | None = None,
        timestamp_type: _AnyTimestampType | None = None,
    ) -> IngestionJob:
        """Add data from proprietary formats using a pre-registered custom extractor.

        This method behaves like `nominal.core.Dataset.add_containerized`, except that the data scope's required
        tags are merged into `tags` before ingest (with user-provided tags taking precedence on key collisions).

        Pass both `timestamp_column` and `timestamp_type`, or neither; this wrapper enforces that before
        delegating.

        For extractor inputs, tagging semantics, timestamp metadata behavior, and return value details, see
        `nominal.core.Dataset.add_containerized`.
        """
        _validate_timestamp_pair(timestamp_column, timestamp_type)

        dataset, scope_tags = self._get_dataset_scope(data_scope_name)
        if timestamp_column is not None and timestamp_type is not None:
            return dataset.add_containerized(
                extractor,
                sources,
                arguments=arguments,
                tags=_merge_scope_tags(scope_tags, tags),
                timestamp_column=timestamp_column,
                timestamp_type=timestamp_type,
            )
        return dataset.add_containerized(
            extractor,
            sources,
            arguments=arguments,
            tags=_merge_scope_tags(scope_tags, tags),
        )

    def add_from_io(
        self,
        data_scope_name: str,
        data_stream: BinaryIO,
        file_type: tuple[str, str] | FileType,
        *,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        file_name: str | None = None,
        tag_columns: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> DatasetFile:
        """Append to the dataset selected by `data_scope_name` from a file-like object.

        This method behaves like `nominal.core.Dataset.add_from_io`, except that the data scope's required tags
        are merged into `tags` before ingest (with user-provided tags taking precedence on key collisions).

        For stream requirements, supported file types, argument semantics, and return value details, see
        `nominal.core.Dataset.add_from_io`.
        """
        dataset, scope_tags = self._get_dataset_scope(data_scope_name)
        return dataset.add_from_io(
            data_stream,
            timestamp_column=timestamp_column,
            timestamp_type=timestamp_type,
            file_type=file_type,
            file_name=file_name,
            tag_columns=tag_columns,
            tags=_merge_scope_tags(scope_tags, tags),
        )


def _merge_scope_tags(scope_tags: Mapping[str, str], tags: Mapping[str, str] | None) -> Mapping[str, str]:
    return {**scope_tags, **(tags or {})}
