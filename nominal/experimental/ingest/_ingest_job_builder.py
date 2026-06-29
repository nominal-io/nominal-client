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
from typing import Mapping, Sequence

from google.protobuf.timestamp_pb2 import Timestamp
from typing_extensions import Self

from nominal.core import Dataset, IngestionJob, NominalClient
from nominal.core._clientsbunch import ClientsBunch
from nominal.core._types import PathLike
from nominal.core._utils.api_tools import rid_from_instance_or_string
from nominal.core._utils.grpc_tools import create_grpc_channel, translate_grpc_errors
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
    TypedTimestampType,
    _AnyTimestampType,
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


@dataclass(frozen=True)
class _PendingItem:
    """A registered file: where it is, how to upload it, and the wire item awaiting its source.

    `item` is fully built at registration time except for its `source`, which is injected after
    upload (the only value not known until then).
    """

    path: Path
    file_type: FileType
    item: ingest_service_pb2.IngestItem


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


def _inject_source(item: ingest_service_pb2.IngestItem, source: common_pb2.IngestSource) -> None:
    """Populate `source` on the item's single set oneof arm.

    Every item kind this builder produces (file/mcap/log/dataflash) carries a `source` field, so the
    one set arm always has somewhere to put it.
    """
    arm = item.WhichOneof("item")
    assert arm is not None, "every ingest item built by this module sets exactly one oneof arm"
    getattr(item, arm).source.CopyFrom(source)


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
        timestamp_metadata = common_pb2.TimestampMetadata(
            column=timestamp_column,
            type=_timestamp_type_to_proto(_to_typed_timestamp_type(timestamp_type)),
        )
        wide = file_ingest_pb2.WideFormat(tag_columns=tag_columns or {})
        if file_type.is_parquet():
            options = file_ingest_pb2.FileIngestOptions(
                timestamp_metadata=timestamp_metadata,
                parquet=file_ingest_pb2.ParquetIngestOptions(
                    format=file_ingest_pb2.ParquetFormat(wide=wide),
                    is_archive=file_type.is_parquet_archive(),
                ),
            )
        else:
            options = file_ingest_pb2.FileIngestOptions(
                timestamp_metadata=timestamp_metadata,
                csv=file_ingest_pb2.CsvIngestOptions(format=file_ingest_pb2.CsvFormat(wide=wide)),
            )
        item = ingest_service_pb2.IngestItem(file=file_ingest_pb2.FileIngestItem(ingest=options), tags=tags or {})
        self._items.append(_PendingItem(file_path, file_type, item))
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
        self._items.append(_PendingItem(file_path, FileTypes.MCAP, item))
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
        log = log_ingest_pb2.LogIngestItem()
        if channel is not None:
            log.channel = channel
        item = ingest_service_pb2.IngestItem(log=log, tags=tags or {})
        self._items.append(_PendingItem(file_path, file_type, item))
        return self

    def add_avro_stream(self, path: PathLike, *, tags: Mapping[str, str] | None = None) -> Self:
        """Register an Avro stream (.avro) file."""
        file_path = Path(path)
        options = file_ingest_pb2.FileIngestOptions(avro=file_ingest_pb2.AvroIngestOptions())
        item = ingest_service_pb2.IngestItem(file=file_ingest_pb2.FileIngestItem(ingest=options), tags=tags or {})
        self._items.append(_PendingItem(file_path, FileTypes.AVRO_STREAM, item))
        return self

    def add_dataflash(self, path: PathLike, *, tags: Mapping[str, str] | None = None) -> Self:
        """Register an ArduPilot Dataflash (.bin) file."""
        file_path = Path(path)
        item = ingest_service_pb2.IngestItem(dataflash=mcap_ingest_pb2.DataflashIngestItem(), tags=tags or {})
        self._items.append(_PendingItem(file_path, FileTypes.DATAFLASH, item))
        return self

    def submit(self) -> IngestionJob:
        """Upload all registered files and trigger one ingest job; returns it for tracking.

        Uploads run in parallel and the call is atomic: if any upload fails, no ingest is
        triggered. Returns immediately with the job in flight; await it with
        `job.wait_until_complete()`. Raises ValueError if no files were added.
        """
        if not self._items:
            raise ValueError("cannot submit an ingest job with no files; add at least one file first")
        clients = self._client._clients
        workspace_rid = clients.resolve_default_workspace_rid()

        sources = _upload_all(self._items, workspace_rid, clients)
        for pending, source in zip(self._items, sources):
            _inject_source(pending.item, source)
        request = ingest_service_pb2.IngestRequest(
            dataset_rid=self._dataset_rid,
            items=[pending.item for pending in self._items],
            tags=self._tags,
        )
        with translate_grpc_errors():
            response = self._ingest_stub().Ingest(request)

        return self._client.get_ingestion_job(response.ingest_job_rid)
