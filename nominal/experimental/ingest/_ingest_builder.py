"""Experimental builder for submitting many files as a single ingest job.

EXPERIMENTAL / UNSTABLE. This is backed by the in-development v2 gRPC IngestService,
whose caller-facing request contract is still changing and may break without notice.
It targets an existing dataset (the endpoint does not create datasets). Use at your own risk.

Build with an ``add_*`` method per file, then ``submit()``. Supported item kinds: tabular
(csv/parquet), avro stream, mcap, journald json, dataflash, and containerized extractors.

Video and point-cloud ingest are intentionally omitted: the v2 endpoint rejects them today.
TODO(drake): add ``add_video`` / ``add_point_cloud`` once the backend accepts those item kinds.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence, overload

from typing_extensions import Self

from nominal.core import ContainerizedExtractor, Dataset, IngestionJob, NominalClient
from nominal.core._clientsbunch import ClientsBunch
from nominal.core._types import PathLike
from nominal.core._utils.api_tools import rid_from_instance_or_string
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.multipart import upload_multipart_file
from nominal.core.filetype import FileType, FileTypes
from nominal.protos.ingest.v2 import (
    common_pb2,
    containerized_ingest_pb2,
    file_ingest_pb2,
    ingest_service_pb2,
    log_ingest_pb2,
    mcap_ingest_pb2,
)
from nominal.ts import (
    Epoch,
    _AnyTimestampType,
    _to_typed_timestamp_type,
)


def _timestamp_metadata(column: str, timestamp_type: _AnyTimestampType) -> common_pb2.TimestampMetadata:
    return common_pb2.TimestampMetadata(column=column, type=_to_typed_timestamp_type(timestamp_type)._to_proto())


# The canonical avro stream schema fixes the timestamp to an epoch-nanosecond `timestamps` field
# (see `Dataset.add_avro_stream` for the schema). The v2 ingest endpoint requires timestamp_metadata
# on every file item, so avro items always send this definition.
_AVRO_STREAM_TIMESTAMPS_FIELD = "timestamps"


def _canonical_avro_timestamp_metadata() -> common_pb2.TimestampMetadata:
    return _timestamp_metadata(_AVRO_STREAM_TIMESTAMPS_FIELD, Epoch(unit="nanoseconds"))


@dataclass(frozen=True)
class _Upload:
    """A file to upload and the (empty) `IngestSource` sub-message its uploaded result fills.

    `target` is a live reference into the built `IngestItem`, so `target.CopyFrom(source)` after
    upload populates the item in place. It must be captured from the final item, not an intermediate.
    """

    path: Path
    file_type: FileType
    target: common_pb2.IngestSource


@dataclass(frozen=True)
class _PendingItem:
    """A built ingest item and the uploads whose sources it is still waiting on (1 for most kinds)."""

    item: ingest_service_pb2.IngestItem
    uploads: tuple[_Upload, ...]


def _upload_all(
    uploads: Sequence[_Upload],
    workspace_rid: str | None,
    clients: ClientsBunch,
) -> list[common_pb2.IngestSource]:
    """Upload every file in parallel and return an `IngestSource` per upload, in input order.

    Reassembling with `executor.map` preserves order and re-raises the first upload error when the
    results are materialized, so a failure aborts before any ingest is triggered (atomic).
    """
    if not uploads:
        return []

    def _upload(upload: _Upload) -> common_pb2.IngestSource:
        s3_path = upload_multipart_file(
            clients.auth_header,
            workspace_rid,
            upload.path,
            clients.upload,
            file_type=upload.file_type,
            header_provider=clients.header_provider,
        )
        return common_pb2.IngestSource(s3=common_pb2.S3IngestSource(path=s3_path))

    with ThreadPoolExecutor(max_workers=min(8, len(uploads))) as executor:
        return list(executor.map(_upload, uploads))


class IngestBuilder:
    """Accumulate files and submit them as a single (MULTI) ingest job.

    EXPERIMENTAL / UNSTABLE — see the module docstring. Targets an existing dataset; the v2
    endpoint does not create datasets. Build with `add_*` (fluent), then `submit()`.
    """

    def __init__(
        self,
        client: NominalClient,
        dataset: str | Dataset,
        *,
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Create a builder that targets an existing dataset.

        Args:
            client: Client used to upload files and trigger the ingest job.
            dataset: The dataset to ingest into, as a `Dataset` or its RID. It must already exist;
                the v2 ingest endpoint does not create datasets.
            tags: Request-level tags applied to every item in the job. Add more later with
                `add_tags`, or set per-item tags on the individual `add_*` calls.
        """
        self._client = client
        self._dataset_rid = rid_from_instance_or_string(dataset)
        self._items: list[_PendingItem] = []
        self._tags: dict[str, str] = dict(tags or {})

    def add_tags(self, tags: Mapping[str, str]) -> Self:
        """Add request-level tags applied to every item in the job.

        Args:
            tags: Key-value pairs to merge into the request-level tags.

        Returns:
            This builder, for chaining.
        """
        self._tags.update(tags)
        return self

    def add_csv(
        self,
        path: PathLike,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        *,
        tag_columns: Mapping[str, str] | None = None,
        units: Mapping[str, str] | None = None,
        channel_prefix: str | None = None,
        channel_name_overrides: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> Self:
        """Register a CSV file to ingest as a tabular file.

        Supported extensions: .csv / .csv.gz.

        Args:
            path: Path to the file on disk.
            timestamp_column: Column containing the timestamp for each row. This column is not
                ingested as its own channel; it sets the timestamps for every other channel.
            timestamp_type: Type of the timestamp data in `timestamp_column`, e.g. 'epoch_seconds'.
            tag_columns: Mapping of tag keys to the columns whose values supply each tag.
            units: Mapping of channel name to unit symbol.
            channel_prefix: Prefix prepended to every channel name ingested from this file.
            channel_name_overrides: Mapping of original channel name to the name to ingest it under.
            tags: Key-value pairs applied as tags to all data from this file.

        Returns:
            This builder, for chaining.
        """
        file_path = Path(path)
        file_type = FileType.from_tabular(file_path)
        if not file_type.is_csv():
            raise ValueError(f"Cannot add path '{file_path}' as CSV: inferred file type {file_type} not CSV!")

        options = file_ingest_pb2.FileIngestOptions(
            timestamp_metadata=_timestamp_metadata(timestamp_column, timestamp_type),
            units=units,
            channel_prefix=channel_prefix,
            channel_name_overrides=channel_name_overrides,
            csv=file_ingest_pb2.CsvIngestOptions(
                format=file_ingest_pb2.CsvFormat(wide=file_ingest_pb2.WideFormat(tag_columns=tag_columns or {}))
            ),
        )
        item = ingest_service_pb2.IngestItem(file=file_ingest_pb2.FileIngestItem(ingest=options), tags=tags or {})
        self._items.append(_PendingItem(item, (_Upload(file_path, file_type, item.file.source),)))
        return self

    def add_parquet(
        self,
        path: PathLike,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        *,
        tag_columns: Mapping[str, str] | None = None,
        units: Mapping[str, str] | None = None,
        channel_prefix: str | None = None,
        channel_name_overrides: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> Self:
        """Register a Parquet file to ingest as a tabular file.

        Supported extensions: .parquet / .parquet.gz, and the parquet-archive
        formats (.parquet.tar / .parquet.tar.gz / .parquet.zip).

        Args:
            path: Path to the file on disk.
            timestamp_column: Column containing the timestamp for each row. This column is not
                ingested as its own channel; it sets the timestamps for every other channel.
            timestamp_type: Type of the timestamp data in `timestamp_column`, e.g. 'epoch_seconds'.
            tag_columns: Mapping of tag keys to the columns whose values supply each tag.
            units: Mapping of channel name to unit symbol.
            channel_prefix: Prefix prepended to every channel name ingested from this file.
            channel_name_overrides: Mapping of original channel name to the name to ingest it under.
            tags: Key-value pairs applied as tags to all data from this file.

        Returns:
            This builder, for chaining.
        """
        file_path = Path(path)
        file_type = FileType.from_tabular(file_path)
        if not file_type.is_parquet():
            raise ValueError(f"Cannot add path '{file_path}' as parquet: inferred file type {file_type} not parquet!")

        options = file_ingest_pb2.FileIngestOptions(
            timestamp_metadata=_timestamp_metadata(timestamp_column, timestamp_type),
            units=units,
            channel_prefix=channel_prefix,
            channel_name_overrides=channel_name_overrides,
            parquet=file_ingest_pb2.ParquetIngestOptions(
                format=file_ingest_pb2.ParquetFormat(wide=file_ingest_pb2.WideFormat(tag_columns=tag_columns or {})),
                is_archive=file_type.is_parquet_archive(),
            ),
        )
        item = ingest_service_pb2.IngestItem(file=file_ingest_pb2.FileIngestItem(ingest=options), tags=tags or {})
        self._items.append(_PendingItem(item, (_Upload(file_path, file_type, item.file.source),)))
        return self

    def add_avro_stream(
        self,
        path: PathLike,
        *,
        units: Mapping[str, str] | None = None,
        channel_prefix: str | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> Self:
        """Register an Avro stream (.avro) file.

        The file must conform to the canonical Avro stream schema (see `Dataset.add_avro_stream`
        for the schema definition); its timestamps come from the epoch-nanosecond `timestamps`
        field, so no timestamp column is passed here.

        Args:
            path: Path to the .avro file on disk.
            units: Mapping of channel name to unit symbol.
            channel_prefix: Prefix prepended to every channel name ingested from this file.
            tags: Key-value pairs applied as tags to all data from this file.

        Returns:
            This builder, for chaining.
        """
        file_path = Path(path)
        file_type = FileType.from_path(path)
        if file_type is not FileTypes.AVRO_STREAM:
            raise ValueError(
                f"Cannot add path '{file_path}' as avro stream: inferred file type {file_type} not avro stream!"
            )

        # Avro deliberately omits channel_name_overrides: the backend rejects it for avro
        # ("channel names come from record data"). TODO(drake): expose it once the backend accepts it.
        options = file_ingest_pb2.FileIngestOptions(
            timestamp_metadata=_canonical_avro_timestamp_metadata(),
            units=units,
            channel_prefix=channel_prefix,
            avro=file_ingest_pb2.AvroIngestOptions(),
        )
        item = ingest_service_pb2.IngestItem(file=file_ingest_pb2.FileIngestItem(ingest=options), tags=tags or {})
        self._items.append(_PendingItem(item, (_Upload(file_path, file_type, item.file.source),)))
        return self

    @overload
    def add_mcap(
        self,
        path: PathLike,
        *,
        include_topics: Sequence[str] | None = ...,
        ignore_invalid_topics: bool | None = ...,
        tags: Mapping[str, str] | None = ...,
    ) -> Self: ...
    @overload
    def add_mcap(
        self,
        path: PathLike,
        *,
        exclude_topics: Sequence[str] | None = ...,
        ignore_invalid_topics: bool | None = ...,
        tags: Mapping[str, str] | None = ...,
    ) -> Self: ...
    def add_mcap(
        self,
        path: PathLike,
        *,
        include_topics: Sequence[str] | None = None,
        exclude_topics: Sequence[str] | None = None,
        ignore_invalid_topics: bool | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> Self:
        """Register an MCAP file.

        Pass at most one of `include_topics` / `exclude_topics`; the overloads make passing both a
        type error, and the runtime guard below rejects it for callers without a type checker.

        Args:
            path: Path to the MCAP file on disk.
            include_topics: If given, restrict ingestion to these topics. Defaults to all
                protobuf-encoded topics present in the MCAP.
            exclude_topics: If given, ingest every topic except these.
            ignore_invalid_topics: If true, skip invalid MCAP topics and continue ingesting valid ones.
            tags: Key-value pairs applied as tags to all data from this file.

        Returns:
            This builder, for chaining.

        Raises:
            ValueError: if both `include_topics` and `exclude_topics` are given.
        """
        if include_topics is not None and exclude_topics is not None:
            raise ValueError("pass at most one of include_topics or exclude_topics")
        file_path = Path(path)

        mcap_channels: mcap_ingest_pb2.McapChannelSelection | None = None
        if include_topics is not None or exclude_topics is not None:
            mcap_channels = mcap_ingest_pb2.McapChannelSelection(
                include_topics=None if include_topics is None else mcap_ingest_pb2.McapTopicNames(include_topics),
                exclude_topics=None if exclude_topics is None else mcap_ingest_pb2.McapTopicNames(exclude_topics),
            )

        options = mcap_ingest_pb2.McapIngestItem(
            source=None,
            channels=mcap_channels,
            ignore_invalid_topics=ignore_invalid_topics,
        )
        item = ingest_service_pb2.IngestItem(
            mcap=options,
            tags=tags or {},
        )
        self._items.append(_PendingItem(item, (_Upload(file_path, FileTypes.MCAP, item.mcap.source),)))
        return self

    @overload
    def add_journal_json(
        self,
        path: PathLike,
        *,
        channel: str | None = ...,
        tags: Mapping[str, str] | None = ...,
    ) -> Self: ...
    @overload
    def add_journal_json(
        self,
        path: PathLike,
        *,
        channel: str | None = ...,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        tags: Mapping[str, str] | None = ...,
    ) -> Self: ...
    def add_journal_json(
        self,
        path: PathLike,
        *,
        channel: str | None = None,
        timestamp_column: str | None = None,
        timestamp_type: _AnyTimestampType | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> Self:
        """Register a journald-style .jsonl / .jsonl.gz log file.

        Pass both `timestamp_column` and `timestamp_type`, or neither; the overloads make passing
        only one a type error, and the runtime guard below rejects it for callers without a type
        checker.

        Args:
            path: Path to the journal-json file on disk.
            channel: Channel name to ingest the logs under. Defaults to 'logs' if omitted.
            timestamp_column: Field holding each record's timestamp. Omit to use the file's
                default journald timestamp.
            timestamp_type: Type of the timestamp data in `timestamp_column`, e.g. 'epoch_microseconds'.
            tags: Key-value pairs applied as tags to all data from this file.

        Returns:
            This builder, for chaining.

        Raises:
            ValueError: if only one of `timestamp_column` / `timestamp_type` is given.
        """
        if (timestamp_column is None) != (timestamp_type is None):
            raise ValueError("pass both timestamp_column and timestamp_type, or neither")
        file_path = Path(path)
        file_type = FileType.from_path_journal_json(file_path)
        log = log_ingest_pb2.LogIngestItem(
            channel=channel,
            timestamp_metadata=(
                _timestamp_metadata(timestamp_column, timestamp_type)
                if timestamp_column is not None and timestamp_type is not None
                else None
            ),
        )
        item = ingest_service_pb2.IngestItem(log=log, tags=tags or {})
        self._items.append(_PendingItem(item, (_Upload(file_path, file_type, item.log.source),)))
        return self

    def add_dataflash(self, path: PathLike, *, tags: Mapping[str, str] | None = None) -> Self:
        """Register an ArduPilot Dataflash (.bin) file.

        Args:
            path: Path to the Dataflash file on disk.
            tags: Key-value pairs applied as tags to all data from this file.

        Returns:
            This builder, for chaining.
        """
        file_path = Path(path)
        item = ingest_service_pb2.IngestItem(dataflash=mcap_ingest_pb2.DataflashIngestItem(), tags=tags or {})
        self._items.append(_PendingItem(item, (_Upload(file_path, FileTypes.DATAFLASH, item.dataflash.source),)))
        return self

    @overload
    def add_containerized(
        self,
        extractor: str | ContainerizedExtractor,
        sources: Mapping[str, PathLike],
        *,
        arguments: Mapping[str, str] | None = ...,
        tags: Mapping[str, str] | None = ...,
    ) -> Self: ...
    @overload
    def add_containerized(
        self,
        extractor: str | ContainerizedExtractor,
        sources: Mapping[str, PathLike],
        *,
        arguments: Mapping[str, str] | None = ...,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        tags: Mapping[str, str] | None = ...,
    ) -> Self: ...
    def add_containerized(
        self,
        extractor: str | ContainerizedExtractor,
        sources: Mapping[str, PathLike],
        *,
        arguments: Mapping[str, str] | None = None,
        timestamp_column: str | None = None,
        timestamp_type: _AnyTimestampType | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> Self:
        """Register a containerized-extractor run over one or more named source files.

        Pass both `timestamp_column` and `timestamp_type`, or neither; the overloads make passing
        only one a type error, and the runtime guard below rejects it for callers without a type
        checker.

        Args:
            extractor: The containerized extractor to run, as a `ContainerizedExtractor` or its RID.
            sources: Mapping of each registered extractor input name to a local file to upload.
                The names must match the extractor's registered inputs exactly.
            arguments: Key-value input arguments passed to the extractor.
            timestamp_column: Column, applied uniformly to the extractor's output files, holding
                each row's timestamp.
            timestamp_type: Type of the timestamp data in `timestamp_column`, e.g. 'epoch_seconds'.
            tags: Key-value pairs applied as tags to all data produced by this run.

        Returns:
            This builder, for chaining.

        Raises:
            ValueError: if `sources` is empty, or if only one of `timestamp_column` /
                `timestamp_type` is given.
        """
        if (timestamp_column is None) != (timestamp_type is None):
            raise ValueError("pass both timestamp_column and timestamp_type, or neither")
        if not sources:
            raise ValueError("add_containerized requires at least one source")
        containerized = containerized_ingest_pb2.ContainerizedIngestItem(
            extractor_rid=rid_from_instance_or_string(extractor),
            arguments=arguments,
            timestamp_metadata=(
                _timestamp_metadata(timestamp_column, timestamp_type)
                if timestamp_column is not None and timestamp_type is not None
                else None
            ),
        )
        item = ingest_service_pb2.IngestItem(containerized=containerized, tags=tags or {})
        uploads = tuple(
            _Upload(Path(source), FileType.from_path(Path(source)), item.containerized.sources[name])
            for name, source in sources.items()
        )
        self._items.append(_PendingItem(item, uploads))
        return self

    def submit(self) -> IngestionJob:
        """Upload all registered files and trigger one ingest job.

        Uploads run in parallel and the call is atomic: if any upload fails, no ingest is
        triggered. The call returns immediately with the job in flight.

        Returns:
            The created ingest job. Track it by polling `job.refresh().status`, or block on its
            produced files with `list(job.as_files_ingested())`.

        Raises:
            ValueError: if no files have been added.
        """
        if not self._items:
            raise ValueError("cannot submit an ingest job with no files; add at least one file first")
        clients = self._client._clients
        workspace_rid = clients.resolve_default_workspace_rid()

        uploads = [upload for pending in self._items for upload in pending.uploads]
        sources = _upload_all(uploads, workspace_rid, clients)
        for upload, source in zip(uploads, sources):
            upload.target.CopyFrom(source)
        request = ingest_service_pb2.IngestRequest(
            dataset_rid=self._dataset_rid,
            items=[pending.item for pending in self._items],
            tags=self._tags,
        )
        with translate_grpc_errors():
            response = self._client._clients.ingest_v2.Ingest(request)

        return self._client.get_ingestion_job(response.ingest_job_rid)
