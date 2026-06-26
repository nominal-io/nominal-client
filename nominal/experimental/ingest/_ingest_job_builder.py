"""Experimental builder for submitting many files as a single ingest job.

EXPERIMENTAL / UNSTABLE. This is backed by the in-development v2 gRPC IngestService.
Its caller-facing request contract changed as recently as 2026-06-25 (scout #15558,
"require log/avro field locators from caller") and may break without notice. It targets
an existing dataset (the v2 endpoint does not create datasets). Use at your own risk.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Sequence

from google.protobuf.timestamp_pb2 import Timestamp
from typing_extensions import Self

from nominal.core import Dataset, NominalClient
from nominal.core._clientsbunch import ClientsBunch
from nominal.core._types import PathLike
from nominal.core._utils.api_tools import rid_from_instance_or_string
from nominal.core._utils.grpc_tools import create_grpc_channel
from nominal.core._utils.multipart import upload_multipart_file
from nominal.core.filetype import FileType, FileTypes
from nominal.protos.ingest.v2 import (
    common_pb2,
    file_ingest_pb2,
    ingest_service_pb2,
    ingest_service_pb2_grpc,
    log_ingest_pb2,
    mcap_ingest_pb2,
)
from nominal.protos.types.time import timestamp_parsers_pb2 as tp
from nominal.ts import (
    Custom,
    Epoch,
    Iso8601,
    Relative,
    _AnyTimestampType,
    _SecondsNanos,
    _to_typed_timestamp_type,
)


def _timestamp_type_to_proto(timestamp_type: _AnyTimestampType) -> tp.TimestampType:
    """Convert a client timestamp type to the proto `nominal.types.time.TimestampType`.

    Mirror of `nominal.ts.*._to_conjure_ingest_api`, but emits the proto type the v2
    FileIngestOptions expects. The proto `time_unit` is the uppercase enum-name string
    (e.g. "SECONDS"), matching scout's v2 ingest parser.
    """
    typed = _to_typed_timestamp_type(timestamp_type)
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


@dataclass(frozen=True)
class _PendingItem:
    """One registered file: where it is, how to upload it, and how to build its wire item."""

    path: Path
    file_type: FileType
    build_item: Callable[[common_pb2.IngestSource], ingest_service_pb2.IngestItem]


def _upload_all(
    items: Sequence[_PendingItem],
    workspace_rid: str | None,
    clients: ClientsBunch,
) -> list[common_pb2.IngestSource]:
    """Upload every pending file in parallel and return an `IngestSource` per item, in input order.

    Reassembling with `executor.map` preserves order and re-raises the first upload error when the
    results are materialized, so a failure aborts before any ingest is triggered (atomic).
    """

    def _upload(item: _PendingItem) -> common_pb2.IngestSource:
        s3_path = upload_multipart_file(
            clients.auth_header,
            workspace_rid,
            item.path,
            clients.upload,
            file_type=item.file_type,
            header_provider=clients.header_provider,
        )
        return common_pb2.IngestSource(s3=common_pb2.S3IngestSource(path=s3_path))

    with ThreadPoolExecutor(max_workers=min(8, len(items))) as executor:
        return list(executor.map(_upload, items))


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
        tags: Mapping[str, str] | None = None,
    ) -> Self:
        """Register a CSV or Parquet file (csv/parquet/parquet-archive extensions)."""
        file_path = Path(path)
        file_type = FileType.from_tabular(file_path)
        ts_proto = _timestamp_type_to_proto(timestamp_type)
        cols = dict(tag_columns or {})
        item_tags = dict(tags or {})

        def build(source: common_pb2.IngestSource) -> ingest_service_pb2.IngestItem:
            timestamp_metadata = common_pb2.TimestampMetadata(column=timestamp_column, type=ts_proto)
            wide = file_ingest_pb2.WideFormat(tag_columns=cols)
            if file_type.is_parquet():
                ingest = file_ingest_pb2.FileIngestOptions(
                    timestamp_metadata=timestamp_metadata,
                    parquet=file_ingest_pb2.ParquetIngestOptions(
                        format=file_ingest_pb2.ParquetFormat(wide=wide),
                        is_archive=file_type.is_parquet_archive(),
                    ),
                )
            else:
                ingest = file_ingest_pb2.FileIngestOptions(
                    timestamp_metadata=timestamp_metadata,
                    csv=file_ingest_pb2.CsvIngestOptions(format=file_ingest_pb2.CsvFormat(wide=wide)),
                )
            return ingest_service_pb2.IngestItem(
                file=file_ingest_pb2.FileIngestItem(source=source, ingest=ingest), tags=item_tags
            )

        self._items.append(_PendingItem(file_path, file_type, build))
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
        include = list(include_topics) if include_topics is not None else None
        exclude = list(exclude_topics) if exclude_topics is not None else None
        item_tags = dict(tags or {})

        def build(source: common_pb2.IngestSource) -> ingest_service_pb2.IngestItem:
            item = mcap_ingest_pb2.McapIngestItem(source=source)
            if include is not None:
                item.channels.include_topics.topics.extend(include)
            elif exclude is not None:
                item.channels.exclude_topics.topics.extend(exclude)
            if ignore_invalid_topics is not None:
                item.ignore_invalid_topics = ignore_invalid_topics
            return ingest_service_pb2.IngestItem(mcap=item, tags=item_tags)

        self._items.append(_PendingItem(file_path, FileTypes.MCAP, build))
        return self

    def add_journal_json(
        self,
        path: PathLike,
        *,
        channel: str | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> Self:
        """Register a journald-style .jsonl / .jsonl.gz log file."""
        file_path = Path(path)
        file_type = FileType.from_path_journal_json(file_path)
        item_tags = dict(tags or {})

        def build(source: common_pb2.IngestSource) -> ingest_service_pb2.IngestItem:
            item = log_ingest_pb2.LogIngestItem(source=source)
            if channel is not None:
                item.channel = channel
            return ingest_service_pb2.IngestItem(log=item, tags=item_tags)

        self._items.append(_PendingItem(file_path, file_type, build))
        return self

    def add_avro_stream(self, path: PathLike, *, tags: Mapping[str, str] | None = None) -> Self:
        """Register an Avro stream (.avro) file."""
        file_path = Path(path)
        item_tags = dict(tags or {})

        def build(source: common_pb2.IngestSource) -> ingest_service_pb2.IngestItem:
            ingest = file_ingest_pb2.FileIngestOptions(avro=file_ingest_pb2.AvroIngestOptions())
            return ingest_service_pb2.IngestItem(
                file=file_ingest_pb2.FileIngestItem(source=source, ingest=ingest), tags=item_tags
            )

        self._items.append(_PendingItem(file_path, FileTypes.AVRO_STREAM, build))
        return self

    def add_dataflash(self, path: PathLike, *, tags: Mapping[str, str] | None = None) -> Self:
        """Register an ArduPilot Dataflash (.bin) file."""
        file_path = Path(path)
        item_tags = dict(tags or {})

        def build(source: common_pb2.IngestSource) -> ingest_service_pb2.IngestItem:
            return ingest_service_pb2.IngestItem(
                dataflash=mcap_ingest_pb2.DataflashIngestItem(source=source), tags=item_tags
            )

        self._items.append(_PendingItem(file_path, FileTypes.DATAFLASH, build))
        return self
