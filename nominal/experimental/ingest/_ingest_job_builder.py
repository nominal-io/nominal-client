"""Experimental builder for submitting many files as a single ingest job.

EXPERIMENTAL / UNSTABLE. This is backed by the in-development v2 gRPC IngestService.
Its caller-facing request contract changed as recently as 2026-06-25 (scout #15558,
"require log/avro field locators from caller") and may break without notice. It targets
an existing dataset (the v2 endpoint does not create datasets). Use at your own risk.

Several item kinds (avro, journal-json, video, point-cloud, containerized) are wired for
completeness against the proto even though the server may reject them today; build with
the matching ``add_*`` method, then ``submit()``.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from google.protobuf.timestamp_pb2 import Timestamp
from typing_extensions import Self

from nominal.core import ContainerizedExtractor, Dataset, IngestionJob, NominalClient
from nominal.core._clientsbunch import ClientsBunch
from nominal.core._types import PathLike
from nominal.core._utils.api_tools import rid_from_instance_or_string
from nominal.core._utils.grpc_tools import create_grpc_channel, translate_grpc_errors
from nominal.core._utils.multipart import upload_multipart_file
from nominal.core.filetype import FileType, FileTypes
from nominal.protos.ingest.v2 import (
    common_pb2,
    containerized_ingest_pb2,
    file_ingest_pb2,
    ingest_service_pb2,
    ingest_service_pb2_grpc,
    log_ingest_pb2,
    mcap_ingest_pb2,
    point_cloud_ingest_pb2,
    video_ingest_pb2,
)
from nominal.protos.types.time import timestamp_parsers_pb2 as tp
from nominal.ts import (
    Custom,
    Epoch,
    Iso8601,
    Relative,
    TypedTimestampType,
    _AnyTimestampType,
    _InferrableTimestampType,
    _SecondsNanos,
    _to_typed_timestamp_type,
)


def _timestamp_type_to_proto(typed: TypedTimestampType) -> tp.TimestampType:
    """Convert a typed client timestamp to the proto `nominal.types.time.TimestampType`.

    Proto sibling of the `_to_conjure_ingest_api` methods on `Iso8601`/`Epoch`/`Relative`/`Custom`
    in `nominal/ts/__init__.py` — it lives here only because this experimental module must not touch
    core; keep the two in sync. The proto `time_unit` is the uppercase enum-name string (e.g.
    "SECONDS"), matching scout's v2 ingest parser.
    """
    if isinstance(typed, Iso8601):
        return tp.TimestampType(absolute=tp.AbsoluteTimestamp(iso8601=tp.Iso8601Timestamp()))
    if isinstance(typed, Epoch):
        return tp.TimestampType(
            absolute=tp.AbsoluteTimestamp(epoch_of_time_unit=tp.EpochTimestamp(time_unit=typed.unit.upper()))
        )
    if isinstance(typed, Relative):
        sn = _SecondsNanos.from_flexible(typed.start)
        return tp.TimestampType(
            relative=tp.RelativeTimestamp(
                time_unit=typed.unit.upper(),
                offset=Timestamp(seconds=sn.seconds, nanos=sn.nanos),
            )
        )
    if isinstance(typed, Custom):
        custom = tp.CustomTimestamp(format=typed.format)
        if typed.default_year is not None:
            custom.default_year = typed.default_year
        if typed.default_day_of_year is not None:
            custom.default_day_of_year = typed.default_day_of_year
        return tp.TimestampType(absolute=tp.AbsoluteTimestamp(custom_format=custom))
    raise TypeError(f"unsupported timestamp type: {typed!r}")


def _to_proto_timestamp(value: _InferrableTimestampType) -> Timestamp:
    """Convert a flexible client timestamp to a `google.protobuf.Timestamp`."""
    sn = _SecondsNanos.from_flexible(value)
    return Timestamp(seconds=sn.seconds, nanos=sn.nanos)


def _timestamp_metadata(column: str, timestamp_type: _AnyTimestampType) -> common_pb2.TimestampMetadata:
    return common_pb2.TimestampMetadata(
        column=column, type=_timestamp_type_to_proto(_to_typed_timestamp_type(timestamp_type))
    )


def _file_ingest_options(
    *,
    timestamp_metadata: common_pb2.TimestampMetadata | None = None,
    units: Mapping[str, str] | None = None,
    channel_prefix: str | None = None,
    channel_name_overrides: Mapping[str, str] | None = None,
) -> file_ingest_pb2.FileIngestOptions:
    """Build the `FileIngestOptions` fields shared by tabular and avro items (sans the format arm)."""
    options = file_ingest_pb2.FileIngestOptions()
    if timestamp_metadata is not None:
        options.timestamp_metadata.CopyFrom(timestamp_metadata)
    if units:
        options.units.update(units)
    if channel_prefix is not None:
        options.channel_prefix = channel_prefix
    if channel_name_overrides:
        options.channel_name_overrides.update(channel_name_overrides)
    return options


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


class IngestionJobBuilder:
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
        self._client = client
        self._dataset_rid = rid_from_instance_or_string(dataset)
        self._items: list[_PendingItem] = []
        self._tags: dict[str, str] = dict(tags or {})
        self._stub: ingest_service_pb2_grpc.IngestServiceStub | None = None

    def _ingest_stub(self) -> ingest_service_pb2_grpc.IngestServiceStub:
        if self._stub is None:
            c = self._client._clients
            channel = create_grpc_channel(
                api_base_url=c._api_base_url,
                service_config=c._service_config,
                user_agent=c._user_agent,
                auth_header=c.auth_header,
                header_provider=c.header_provider,
            )
            self._stub = ingest_service_pb2_grpc.IngestServiceStub(channel)
        return self._stub

    def add_tags(self, tags: Mapping[str, str]) -> Self:
        """Add request-level tags applied to every item in the job."""
        self._tags.update(tags)
        return self

    def add_tabular(
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
        """Register a CSV or Parquet file (csv/parquet/parquet-archive extensions)."""
        file_path = Path(path)
        file_type = FileType.from_tabular(file_path)
        options = _file_ingest_options(
            timestamp_metadata=_timestamp_metadata(timestamp_column, timestamp_type),
            units=units,
            channel_prefix=channel_prefix,
            channel_name_overrides=channel_name_overrides,
        )
        wide = file_ingest_pb2.WideFormat(tag_columns=tag_columns or {})
        # Set the csv/parquet `ingest` oneof via CopyFrom so the oneof case is always present, even
        # when there are no tag columns (mutating an empty map would leave the oneof unset, which the
        # backend rejects with "exactly one field is required in oneof").
        if file_type.is_parquet():
            options.parquet.CopyFrom(
                file_ingest_pb2.ParquetIngestOptions(
                    format=file_ingest_pb2.ParquetFormat(wide=wide),
                    is_archive=file_type.is_parquet_archive(),
                )
            )
        else:
            options.csv.CopyFrom(file_ingest_pb2.CsvIngestOptions(format=file_ingest_pb2.CsvFormat(wide=wide)))
        item = ingest_service_pb2.IngestItem(file=file_ingest_pb2.FileIngestItem(ingest=options), tags=tags or {})
        self._items.append(_PendingItem(item, (_Upload(file_path, file_type, item.file.source),)))
        return self

    def add_avro_stream(
        self,
        path: PathLike,
        *,
        units: Mapping[str, str] | None = None,
        channel_prefix: str | None = None,
        channel_name_overrides: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> Self:
        """Register an Avro stream (.avro) file."""
        file_path = Path(path)
        options = _file_ingest_options(
            units=units, channel_prefix=channel_prefix, channel_name_overrides=channel_name_overrides
        )
        options.avro.SetInParent()
        item = ingest_service_pb2.IngestItem(file=file_ingest_pb2.FileIngestItem(ingest=options), tags=tags or {})
        self._items.append(_PendingItem(item, (_Upload(file_path, FileTypes.AVRO_STREAM, item.file.source),)))
        return self

    def add_mcap(
        self,
        path: PathLike,
        *,
        include_topics: Sequence[str] | None = None,
        exclude_topics: Sequence[str] | None = None,
        ignore_invalid_topics: bool | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> Self:
        """Register an MCAP file. `include_topics` and `exclude_topics` are mutually exclusive."""
        if include_topics is not None and exclude_topics is not None:
            raise ValueError("pass at most one of include_topics or exclude_topics")
        file_path = Path(path)
        mcap = mcap_ingest_pb2.McapIngestItem()
        if include_topics is not None:
            mcap.channels.include_topics.topics.extend(include_topics)
        elif exclude_topics is not None:
            mcap.channels.exclude_topics.topics.extend(exclude_topics)
        if ignore_invalid_topics is not None:
            mcap.ignore_invalid_topics = ignore_invalid_topics
        item = ingest_service_pb2.IngestItem(mcap=mcap, tags=tags or {})
        self._items.append(_PendingItem(item, (_Upload(file_path, FileTypes.MCAP, item.mcap.source),)))
        return self

    def add_journal_json(
        self,
        path: PathLike,
        *,
        channel: str | None = None,
        timestamp_column: str | None = None,
        timestamp_type: _AnyTimestampType | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> Self:
        """Register a journald-style .jsonl / .jsonl.gz log file."""
        if (timestamp_column is None) != (timestamp_type is None):
            raise ValueError("pass both timestamp_column and timestamp_type, or neither")
        file_path = Path(path)
        file_type = FileType.from_path_journal_json(file_path)
        log = log_ingest_pb2.LogIngestItem()
        if channel is not None:
            log.channel = channel
        if timestamp_column is not None and timestamp_type is not None:
            log.timestamp_metadata.CopyFrom(_timestamp_metadata(timestamp_column, timestamp_type))
        item = ingest_service_pb2.IngestItem(log=log, tags=tags or {})
        self._items.append(_PendingItem(item, (_Upload(file_path, file_type, item.log.source),)))
        return self

    def add_dataflash(self, path: PathLike, *, tags: Mapping[str, str] | None = None) -> Self:
        """Register an ArduPilot Dataflash (.bin) file."""
        file_path = Path(path)
        item = ingest_service_pb2.IngestItem(dataflash=mcap_ingest_pb2.DataflashIngestItem(), tags=tags or {})
        self._items.append(_PendingItem(item, (_Upload(file_path, FileTypes.DATAFLASH, item.dataflash.source),)))
        return self

    def add_point_cloud(
        self,
        path: PathLike,
        *,
        channel: str | None = None,
        sensor_properties: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> Self:
        """Register a point-cloud file (e.g. .pcd / .las)."""
        file_path = Path(path)
        options = point_cloud_ingest_pb2.PointCloudIngestOptions()
        if channel is not None:
            options.channel = channel
        if sensor_properties:
            options.sensor_metadata.properties.update(sensor_properties)
        item = ingest_service_pb2.IngestItem(
            point_cloud=point_cloud_ingest_pb2.PointCloudIngestItem(ingest=options), tags=tags or {}
        )
        self._items.append(
            _PendingItem(item, (_Upload(file_path, FileType.from_path(file_path), item.point_cloud.source),))
        )
        return self

    def add_video(
        self,
        path: PathLike,
        *,
        channel: str | None = None,
        starting_timestamp: _InferrableTimestampType | None = None,
        frame_rate: int | None = None,
        ending_timestamp: _InferrableTimestampType | None = None,
        scale_factor: int | None = None,
        manifest_paths: Sequence[PathLike] | None = None,
        overwrite_segments: bool | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> Self:
        """Register a video file.

        Timestamps come from either the no-manifest mode (`starting_timestamp` plus at most one of
        `frame_rate` / `ending_timestamp` / `scale_factor`) or the manifest-files mode
        (`manifest_paths`, each uploaded alongside the video). The two modes are mutually exclusive.
        """
        scale_args = [frame_rate, ending_timestamp, scale_factor]
        if starting_timestamp is not None and manifest_paths is not None:
            raise ValueError("pass at most one of starting_timestamp or manifest_paths")
        if sum(arg is not None for arg in scale_args) > 1:
            raise ValueError("pass at most one of frame_rate, ending_timestamp, scale_factor")
        if manifest_paths is not None and (not manifest_paths or any(arg is not None for arg in scale_args)):
            raise ValueError("manifest_paths must be non-empty and excludes the scale options")

        file_path = Path(path)
        options = video_ingest_pb2.VideoIngestOptions()
        if channel is not None:
            options.channel = channel
        if overwrite_segments is not None:
            options.overwrite_segments = overwrite_segments
        if manifest_paths is None:
            no_manifest = options.timestamp_manifest.no_manifest
            if starting_timestamp is not None:
                no_manifest.starting_timestamp.CopyFrom(_to_proto_timestamp(starting_timestamp))
            if frame_rate is not None:
                no_manifest.scale_parameter.true_frame_rate = frame_rate
            elif ending_timestamp is not None:
                no_manifest.scale_parameter.ending_timestamp.CopyFrom(_to_proto_timestamp(ending_timestamp))
            elif scale_factor is not None:
                no_manifest.scale_parameter.scale_factor = scale_factor

        item = ingest_service_pb2.IngestItem(video=video_ingest_pb2.VideoIngestItem(ingest=options), tags=tags or {})
        uploads = [_Upload(file_path, FileType.from_video(file_path), item.video.source)]
        if manifest_paths is not None:
            manifest_files = item.video.ingest.timestamp_manifest.timestamp_manifest_files
            for manifest_path in manifest_paths:
                manifest = Path(manifest_path)
                uploads.append(_Upload(manifest, FileType.from_path(manifest), manifest_files.sources.add()))
        self._items.append(_PendingItem(item, tuple(uploads)))
        return self

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

        `sources` maps each registered extractor input name to a local file (each uploaded).
        """
        if (timestamp_column is None) != (timestamp_type is None):
            raise ValueError("pass both timestamp_column and timestamp_type, or neither")
        if not sources:
            raise ValueError("add_containerized requires at least one source")
        containerized = containerized_ingest_pb2.ContainerizedIngestItem(
            extractor_rid=rid_from_instance_or_string(extractor)
        )
        if arguments:
            containerized.arguments.update(arguments)
        if timestamp_column is not None and timestamp_type is not None:
            containerized.timestamp_metadata.CopyFrom(_timestamp_metadata(timestamp_column, timestamp_type))
        item = ingest_service_pb2.IngestItem(containerized=containerized, tags=tags or {})
        uploads = tuple(
            _Upload(Path(source), FileType.from_path(Path(source)), item.containerized.sources[name])
            for name, source in sources.items()
        )
        self._items.append(_PendingItem(item, uploads))
        return self

    def submit(self) -> IngestionJob:
        """Upload all registered files and trigger one ingest job; returns it for tracking.

        Uploads run in parallel and the call is atomic: if any upload fails, no ingest is
        triggered. Returns immediately with the job in flight; track it by polling
        `job.refresh().status`, or block on its produced files with
        `list(job.as_files_ingested())`. Raises ValueError if no files were added.
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
            response = self._ingest_stub().Ingest(request)

        return self._client.get_ingestion_job(response.ingest_job_rid)
