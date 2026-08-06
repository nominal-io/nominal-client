"""Experimental builder for submitting many files as a single ingest job.

EXPERIMENTAL / UNSTABLE. This is backed by the in-development v2 gRPC IngestService,
whose caller-facing request contract is still changing and may break without notice.
It targets an existing dataset (the endpoint does not create datasets). Use at your own risk.

Build with an ``add_*`` method per file, then ``submit()``. Supported item kinds: tabular
(csv/parquet), avro stream, mcap, journald json, dataflash, video, and containerized extractors.

Point-cloud ingest is intentionally omitted: the v2 endpoint rejects it today.
TODO(drake): add ``add_point_cloud`` once the backend accepts that item kind.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from concurrent.futures import as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Mapping, Sequence, Union, overload

from google.protobuf import timestamp_pb2
from typing_extensions import Self

from nominal.core import ContainerizedExtractor, Dataset, IngestionJob, NominalClient
from nominal.core._types import PathLike
from nominal.core._utils.api_tools import rid_from_instance_or_string
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core.exceptions import NominalIngestError, NominalIngestUploadFailed
from nominal.core.filetype import FileType, FileTypes
from nominal.experimental.ingest._multipart_uploader import MultipartUploader
from nominal.protos.ingest.v2 import (
    common_pb2,
    containerized_ingest_pb2,
    file_ingest_pb2,
    ingest_service_pb2,
    log_ingest_pb2,
    mcap_ingest_pb2,
    video_ingest_pb2,
)
from nominal.ts import (
    Epoch,
    IntegralNanosecondsUTC,
    Relative,
    _AnyNumericTimestampType,
    _AnyTimestampType,
    _SecondsNanos,
    _to_typed_timestamp_type,
    _validate_timestamp_pair,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, eq=False)
class _PendingFile:
    """One file to upload: its local path and the type it should be stored as.

    Compared by identity, never by value: every registration is its own upload — even of a
    repeated path — and the locations mapping `_upload_all` returns is keyed by these objects.
    """

    path: Path
    file_type: FileType


def _s3_source(location: str) -> common_pb2.IngestSource:
    return common_pb2.IngestSource(s3=common_pb2.S3IngestSource(path=location))


_Locations = Mapping[_PendingFile, str]

# One record class per item kind. Each owns the files it needs uploaded — as NAMED fields — and
# builds its finished IngestItem in a single constructor call, looking each file's storage
# location up by the file object itself. There is no positional locations protocol to keep in
# sync, no partially-built protos to fill in later, and no way for uploads and items to desync:
# a record cannot reference a file it does not own.


@dataclass(frozen=True)
class _FileItem:
    """A tabular or avro-stream file and its fully-built ingest options."""

    file: _PendingFile
    options: file_ingest_pb2.FileIngestOptions
    tags: Mapping[str, str]

    @property
    def files(self) -> tuple[_PendingFile, ...]:
        return (self.file,)

    def build(self, locations: _Locations) -> ingest_service_pb2.IngestItem:
        return ingest_service_pb2.IngestItem(
            file=file_ingest_pb2.FileIngestItem(ingest=self.options, source=_s3_source(locations[self.file])),
            tags=self.tags,
        )


@dataclass(frozen=True)
class _McapItem:
    """An MCAP file with optional topic selection."""

    file: _PendingFile
    channels: mcap_ingest_pb2.McapChannelSelection | None
    ignore_invalid_topics: bool
    tags: Mapping[str, str]

    @property
    def files(self) -> tuple[_PendingFile, ...]:
        return (self.file,)

    def build(self, locations: _Locations) -> ingest_service_pb2.IngestItem:
        return ingest_service_pb2.IngestItem(
            mcap=mcap_ingest_pb2.McapIngestItem(
                source=_s3_source(locations[self.file]),
                channels=self.channels,
                ignore_invalid_topics=self.ignore_invalid_topics,
            ),
            tags=self.tags,
        )


@dataclass(frozen=True)
class _LogItem:
    """A journald-style log file with its channel and optional timestamp field."""

    file: _PendingFile
    channel: str | None
    timestamp_metadata: common_pb2.TimestampMetadata | None
    tags: Mapping[str, str]

    @property
    def files(self) -> tuple[_PendingFile, ...]:
        return (self.file,)

    def build(self, locations: _Locations) -> ingest_service_pb2.IngestItem:
        return ingest_service_pb2.IngestItem(
            log=log_ingest_pb2.LogIngestItem(
                channel=self.channel,
                timestamp_metadata=self.timestamp_metadata,
                source=_s3_source(locations[self.file]),
            ),
            tags=self.tags,
        )


@dataclass(frozen=True)
class _DataflashItem:
    """An ArduPilot Dataflash file."""

    file: _PendingFile
    tags: Mapping[str, str]

    @property
    def files(self) -> tuple[_PendingFile, ...]:
        return (self.file,)

    def build(self, locations: _Locations) -> ingest_service_pb2.IngestItem:
        return ingest_service_pb2.IngestItem(
            dataflash=mcap_ingest_pb2.DataflashIngestItem(source=_s3_source(locations[self.file])),
            tags=self.tags,
        )


@dataclass(frozen=True)
class _VideoItem:
    """A video file plus its timestamping: a fixed first-frame instant or a manifest file.

    Exactly one of `no_manifest` / `manifest` is set — `add_video` enforces it.
    """

    video: _PendingFile
    channel: str
    no_manifest: video_ingest_pb2.NoTimestampManifest | None
    manifest: _PendingFile | None
    tags: Mapping[str, str]

    @property
    def files(self) -> tuple[_PendingFile, ...]:
        return (self.video,) if self.manifest is None else (self.video, self.manifest)

    def build(self, locations: _Locations) -> ingest_service_pb2.IngestItem:
        timestamp_manifest = (
            video_ingest_pb2.VideoTimestampManifest(no_manifest=self.no_manifest)
            if self.manifest is None
            else video_ingest_pb2.VideoTimestampManifest(
                timestamp_manifest_files=video_ingest_pb2.TimestampManifestFiles(
                    sources=[_s3_source(locations[self.manifest])]
                )
            )
        )
        return ingest_service_pb2.IngestItem(
            video=video_ingest_pb2.VideoIngestItem(
                source=_s3_source(locations[self.video]),
                ingest=video_ingest_pb2.VideoIngestOptions(channel=self.channel, timestamp_manifest=timestamp_manifest),
            ),
            tags=self.tags,
        )


@dataclass(frozen=True)
class _ContainerizedItem:
    """A containerized-extractor run over named input files."""

    extractor_rid: str
    sources: Mapping[str, _PendingFile]
    arguments: Mapping[str, str] | None
    timestamp_metadata: common_pb2.TimestampMetadata | None
    tags: Mapping[str, str]

    @property
    def files(self) -> tuple[_PendingFile, ...]:
        return tuple(self.sources.values())

    def build(self, locations: _Locations) -> ingest_service_pb2.IngestItem:
        return ingest_service_pb2.IngestItem(
            containerized=containerized_ingest_pb2.ContainerizedIngestItem(
                extractor_rid=self.extractor_rid,
                arguments=self.arguments,
                timestamp_metadata=self.timestamp_metadata,
                sources={name: _s3_source(locations[file]) for name, file in self.sources.items()},
            ),
            tags=self.tags,
        )


_PendingItem = Union[_FileItem, _McapItem, _LogItem, _DataflashItem, _VideoItem, _ContainerizedItem]


def _write_frame_timestamps(frame_timestamps: Sequence[IntegralNanosecondsUTC]) -> Path:
    """Write per-frame timestamps as the JSON manifest file the video segmenter consumes.

    A small temp file, uploaded at submit time like any registered file and deleted by
    `submit()` once the upload phase is over (the builder tracks it in `_temp_files`).
    """
    fd, name = tempfile.mkstemp(prefix="nominal_video_manifest_", suffix=".json")
    with os.fdopen(fd, "w") as f:
        json.dump(list(frame_timestamps), f)
    return Path(name)


def _upload_failure_group(failures: Mapping[_PendingFile, BaseException], total: int) -> NominalIngestUploadFailed:
    """Bundle per-file upload failures into one raisable group, each member naming its file."""
    members: list[NominalIngestError] = []
    for file, exc in failures.items():
        member = NominalIngestError(f"failed to upload '{file.path}'")
        member.__cause__ = exc
        members.append(member)
    return NominalIngestUploadFailed(
        f"{len(failures)} of {total} file(s) failed to upload; nothing was ingested", members
    )


def _upload_all(
    files: Sequence[_PendingFile], client: NominalClient, *, allow_partial: bool
) -> dict[_PendingFile, str | BaseException]:
    """Upload every file in parallel; return each file's outcome (location or failure) by file.

    The uploader has already absorbed transient failures (its file-level retry budget), so a
    failure reaching this layer is permanent or outlasted that budget.

    With `allow_partial=False` this is atomic and fail-fast: the first failure raises a
    `NominalIngestUploadFailed` carrying every failure that has surfaced by that moment, and
    leaving the `with` block on that exception runs the cancelling close that drops the rest
    of the batch. With `allow_partial=True` every file runs to settlement and failures are
    returned as outcomes for the caller to prune.
    """
    if not files:
        return {}
    outcomes: dict[_PendingFile, str | BaseException] = {}
    with MultipartUploader.create(client) as up:
        futures = {up.enqueue_file(file.path, file_type=file.file_type): file for file in files}
        for fut in as_completed(futures):
            try:
                outcomes[futures[fut]] = fut.result()
            except BaseException as exc:
                outcomes[futures[fut]] = exc
                if not allow_partial:
                    # Sweep the other already-settled futures so the report carries every failure
                    # known at this moment — without waiting on files still uploading.
                    for other, other_file in futures.items():
                        if other_file not in outcomes and other.done():
                            other_exc = other.exception()
                            if other_exc is not None:
                                outcomes[other_file] = other_exc
                    failures = {file: out for file, out in outcomes.items() if isinstance(out, BaseException)}
                    raise _upload_failure_group(failures, total=len(files)) from None
    return outcomes


class IngestBuilder:
    """Accumulate files and submit them as a single (MULTI) ingest job.

    EXPERIMENTAL / UNSTABLE — see the module docstring. Targets an existing dataset; the v2
    endpoint does not create datasets. Build with `add_*` (fluent), then `submit()` exactly
    once: a builder is single-use, and a second `submit()` raises rather than re-uploading
    and re-ingesting everything it holds.
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
        self._pending: list[_PendingItem] = []
        self._tags: dict[str, str] = dict(tags or {})
        self._temp_files: list[Path] = []  # builder-generated files, deleted once uploads finish
        self._submitted = False

    def add_tags(self, tags: Mapping[str, str]) -> Self:
        """Add request-level tags applied to every item in the job.

        Args:
            tags: Key-value pairs to merge into the request-level tags.

        Returns:
            This builder, for chaining.
        """
        self._tags.update(tags)
        return self

    def add_tabular_data(
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
        """Register a tabular file (CSV or Parquet), mirroring `Dataset.add_tabular_data`.

        Supported extensions: .csv / .csv.gz, .parquet / .parquet.gz, and the parquet-archive
        formats (.parquet.tar / .parquet.tar.gz / .parquet.zip). The format is inferred from
        the extension.

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

        Raises:
            ValueError: the path is not a supported tabular format.
        """
        file_path = Path(path)
        file_type = FileType.from_tabular(file_path)  # raises on non-tabular extensions

        options = file_ingest_pb2.FileIngestOptions(
            timestamp_metadata=common_pb2.TimestampMetadata(
                column=timestamp_column, type=_to_typed_timestamp_type(timestamp_type)._to_proto()
            ),
            units=units,
            channel_prefix=channel_prefix,
            channel_name_overrides=channel_name_overrides,
        )
        wide_format = file_ingest_pb2.WideFormat(tag_columns=tag_columns or {})
        if file_type.is_csv():
            options.csv.CopyFrom(file_ingest_pb2.CsvIngestOptions(format=file_ingest_pb2.CsvFormat(wide=wide_format)))
        else:
            options.parquet.CopyFrom(
                file_ingest_pb2.ParquetIngestOptions(
                    format=file_ingest_pb2.ParquetFormat(wide=wide_format),
                    is_archive=file_type.is_parquet_archive(),
                )
            )
        self._pending.append(_FileItem(file=_PendingFile(file_path, file_type), options=options, tags=dict(tags or {})))
        return self

    def add_avro_stream(
        self,
        path: PathLike,
        *,
        units: Mapping[str, str] | None = None,
        channel_prefix: str | None = None,
        timestamp_type: _AnyNumericTimestampType | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> Self:
        """Register an Avro stream (.avro) file.

        The file must conform to the canonical Avro stream schema (see `Dataset.add_avro_stream`
        for the schema definition). The schema fixes which field holds the timestamps, so this takes
        no timestamp column -- only how to read the numbers in that field.

        Args:
            path: Path to the .avro file on disk.
            units: Mapping of channel name to unit symbol.
            channel_prefix: Prefix prepended to every channel name ingested from this file.
            timestamp_type: How to read the file's numeric timestamps -- an absolute epoch
                (`ts.Epoch`, or its string literal) or an offset from a start (`ts.Relative`).
                Defaults to epoch nanoseconds, the canonical schema's reading.
            tags: Key-value pairs applied as tags to all data from this file. Tags in the records
                take precedence -- these only fill keys a record does not already set.

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
        # ("channel names come from record data").
        # TODO(drake): expose channel_name_overrides here once the backend accepts it for avro.

        # The canonical avro stream schema fixes the timestamps to a `timestamps` field (see
        # `Dataset.add_avro_stream` for the schema) and the multi-file endpoint requires
        # timestamp_metadata on every file item, so the canonical epoch-nanosecond reading is sent
        # whenever the caller declares none.
        declared_type = Epoch(unit="nanoseconds") if timestamp_type is None else timestamp_type
        options = file_ingest_pb2.FileIngestOptions(
            timestamp_metadata=common_pb2.TimestampMetadata(
                column="timestamps", type=_to_typed_timestamp_type(declared_type)._to_proto()
            ),
            units=units,
            channel_prefix=channel_prefix,
            avro=file_ingest_pb2.AvroIngestOptions(),
        )
        self._pending.append(_FileItem(file=_PendingFile(file_path, file_type), options=options, tags=dict(tags or {})))
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
                include_topics=None
                if include_topics is None
                else mcap_ingest_pb2.McapTopicNames(topics=include_topics),
                exclude_topics=None
                if exclude_topics is None
                else mcap_ingest_pb2.McapTopicNames(topics=exclude_topics),
            )

        self._pending.append(
            _McapItem(
                file=_PendingFile(file_path, FileTypes.MCAP),
                channels=mcap_channels,
                # The proto field has no presence, so None (unset) and False are wire-identical.
                ignore_invalid_topics=ignore_invalid_topics or False,
                tags=dict(tags or {}),
            )
        )
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
        timestamp_type: _AnyNumericTimestampType,
        tags: Mapping[str, str] | None = ...,
    ) -> Self: ...
    def add_journal_json(
        self,
        path: PathLike,
        *,
        channel: str | None = None,
        timestamp_column: str | None = None,
        timestamp_type: _AnyNumericTimestampType | None = None,
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
            timestamp_type: How to read the numbers in `timestamp_column` -- an absolute epoch
                (`ts.Epoch`, or its string literal, e.g. 'epoch_microseconds') or an offset from a
                start (`ts.Relative`). Log timestamps are read as numbers, so ISO 8601 and custom
                string formats cannot be used here.
            tags: Key-value pairs applied as tags to all data from this file.

        Returns:
            This builder, for chaining.

        Raises:
            ValueError: if only one of `timestamp_column` / `timestamp_type` is given, or if
                `timestamp_type` is not numeric.
        """
        _validate_timestamp_pair(timestamp_column, timestamp_type)
        file_path = Path(path)
        file_type = FileType.from_path_journal_json(file_path)
        timestamp_meta = None
        if timestamp_column is not None and timestamp_type is not None:
            typed_timestamp_type = _to_typed_timestamp_type(timestamp_type)
            if not isinstance(typed_timestamp_type, (Epoch, Relative)):
                raise ValueError(
                    f"journal json timestamps must be numeric (ts.Epoch or ts.Relative), "
                    f"received {typed_timestamp_type!r}"
                )
            timestamp_meta = common_pb2.TimestampMetadata(
                column=timestamp_column, type=typed_timestamp_type._to_proto()
            )
        self._pending.append(
            _LogItem(
                file=_PendingFile(file_path, file_type),
                channel=channel,
                timestamp_metadata=timestamp_meta,
                tags=dict(tags or {}),
            )
        )
        return self

    def add_ardupilot_dataflash(self, path: PathLike, *, tags: Mapping[str, str] | None = None) -> Self:
        """Register an ArduPilot Dataflash (.bin) file, mirroring `Dataset.add_ardupilot_dataflash`.

        Args:
            path: Path to the Dataflash file on disk.
            tags: Key-value pairs applied as tags to all data from this file.

        Returns:
            This builder, for chaining.
        """
        file_path = Path(path)
        self._pending.append(_DataflashItem(file=_PendingFile(file_path, FileTypes.DATAFLASH), tags=dict(tags or {})))
        return self

    @overload
    def add_video(
        self,
        path: PathLike,
        channel: str,
        *,
        start: datetime | IntegralNanosecondsUTC,
        tags: Mapping[str, str] | None = ...,
    ) -> Self: ...
    @overload
    def add_video(
        self,
        path: PathLike,
        channel: str,
        *,
        frame_timestamps: Sequence[IntegralNanosecondsUTC],
        tags: Mapping[str, str] | None = ...,
    ) -> Self: ...
    def add_video(
        self,
        path: PathLike,
        channel: str,
        *,
        start: datetime | IntegralNanosecondsUTC | None = None,
        frame_timestamps: Sequence[IntegralNanosecondsUTC] | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> Self:
        """Register a video file.

        Pass exactly one of `start` / `frame_timestamps`; the overloads make anything else a
        type error, and the runtime guard below rejects it for callers without a type checker.
        With `start`, frames are timestamped from that instant at the video's own frame rate.
        With `frame_timestamps`, the per-frame timestamps are written to a manifest file that
        is uploaded alongside the video.

        Args:
            path: Path to the video file on disk.
            channel: Channel name to ingest the video under.
            start: Timestamp of the video's first frame.
            frame_timestamps: One epoch-nanosecond timestamp per video frame.
            tags: Key-value pairs applied as tags to the video.

        Returns:
            This builder, for chaining.

        Raises:
            ValueError: if `channel` is empty, the path is not a supported video container,
                `frame_timestamps` is empty, or both or neither of `start` /
                `frame_timestamps` are given.
        """
        if not channel:
            raise ValueError("videos require a non-empty channel name")
        if (start is None) == (frame_timestamps is None):
            raise ValueError("pass exactly one of start or frame_timestamps")
        if frame_timestamps is not None and not frame_timestamps:
            raise ValueError("frame_timestamps must contain at least one timestamp")
        file_path = Path(path)
        file_type = FileType.from_video(file_path)  # fail fast, before any bytes are spent uploading

        no_manifest: video_ingest_pb2.NoTimestampManifest | None = None
        manifest_file: _PendingFile | None = None
        if start is not None:
            starting = _SecondsNanos.from_flexible(start)
            # TODO(drake): expose the scale parameter (true frame rate / ending timestamp / factor).
            no_manifest = video_ingest_pb2.NoTimestampManifest(
                starting_timestamp=timestamp_pb2.Timestamp(seconds=starting.seconds, nanos=starting.nanos)
            )
        elif frame_timestamps is not None:
            manifest_file = _PendingFile(_write_frame_timestamps(frame_timestamps), FileTypes.JSON)
            self._temp_files.append(manifest_file.path)

        self._pending.append(
            _VideoItem(
                video=_PendingFile(file_path, file_type),
                channel=channel,
                no_manifest=no_manifest,
                manifest=manifest_file,
                tags=dict(tags or {}),
            )
        )
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
        _validate_timestamp_pair(timestamp_column, timestamp_type)
        if not sources:
            raise ValueError("add_containerized requires at least one source")
        timestamp_meta = (
            common_pb2.TimestampMetadata(
                column=timestamp_column, type=_to_typed_timestamp_type(timestamp_type)._to_proto()
            )
            if timestamp_column is not None and timestamp_type is not None
            else None
        )
        self._pending.append(
            _ContainerizedItem(
                extractor_rid=rid_from_instance_or_string(extractor),
                sources={
                    name: _PendingFile(Path(source), FileType.from_path(Path(source)))
                    for name, source in sources.items()
                },
                # Copied so a caller mutating their dict after registration cannot alter the request.
                arguments=None if arguments is None else dict(arguments),
                timestamp_metadata=timestamp_meta,
                tags=dict(tags or {}),
            )
        )
        return self

    def submit(self, *, allow_partial: bool = False) -> IngestionJob:
        """Upload all registered files and trigger one ingest job.

        Uploads run in parallel, with transient failures (network weather, throttling) retried
        by the uploader's own per-file budget. By default the call is atomic and fail-fast: the
        first file that fails permanently cancels the batch and raises, and no ingest is
        triggered. With `allow_partial=True`, every file runs to settlement instead — items
        whose files failed are logged and pruned, and one ingest job is triggered for the items
        that uploaded cleanly.

        Single-use: one `submit()` consumes the builder, whether it succeeds or fails. A
        failed trigger request can have been committed server-side (a timeout, say), so there
        is no retry that cannot double-ingest — build a new builder instead.

        Args:
            allow_partial: If true, items whose files failed to upload are dropped from the
                job (each failure logged as an error) instead of failing the whole batch.
                Deliberately reported through logs, not the return value — the job always
                covers exactly the items that uploaded.

        Returns:
            The created ingest job. Track it by polling `job.refresh().status`, or block on its
            produced files with `list(job.as_files_ingested())`.

        Raises:
            NominalIngestError: this builder was already submitted.
            NominalIngestUploadFailed: a file failed to upload — any file when
                `allow_partial` is false, or every file when it is true. Members carry
                per-file detail.
            ValueError: if no files have been added.
        """
        if self._submitted:
            raise NominalIngestError(
                "this IngestBuilder was already submitted; builders are single-use — "
                "create a new builder to ingest more files"
            )
        if not self._pending:
            raise ValueError("cannot submit an ingest job with no files; add at least one file first")
        self._submitted = True

        all_files = [file for pending in self._pending for file in pending.files]
        try:
            outcomes = _upload_all(all_files, self._client, allow_partial=allow_partial)
        finally:
            # Builder-generated files (video timestamp manifests) are dead once the upload phase
            # is over — uploaded or failed, the single-use builder never needs them again.
            for temp_file in self._temp_files:
                temp_file.unlink(missing_ok=True)

        locations = {file: out for file, out in outcomes.items() if isinstance(out, str)}
        failures = {file: out for file, out in outcomes.items() if isinstance(out, BaseException)}
        items: list[ingest_service_pb2.IngestItem] = []
        for pending in self._pending:
            failed = [file for file in pending.files if file in failures]
            if not failed:
                items.append(pending.build(locations))
                continue
            # allow_partial=False cannot reach here: _upload_all raised on the first failure.
            for file in failed:
                logger.error("failed to upload '%s'; dropping its item from the ingest job", file.path)
            for file in pending.files:
                if file in locations:
                    logger.warning("'%s' uploaded but its item was dropped because a sibling file failed", file.path)
        if not items:
            raise _upload_failure_group(failures, total=len(all_files))
        if failures:
            logger.error(
                "ingesting %d of %d items; %d file(s) failed to upload and their items were dropped",
                len(items),
                len(self._pending),
                len(failures),
            )

        request = ingest_service_pb2.IngestRequest(
            dataset_rid=self._dataset_rid,
            items=items,
            tags=self._tags,
        )
        with translate_grpc_errors():
            response = self._client._clients.ingest_v2.Ingest(request)

        return self._client.get_ingestion_job(response.ingest_job_rid)
