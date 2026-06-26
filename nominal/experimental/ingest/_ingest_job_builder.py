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
from typing import Callable, Sequence

from google.protobuf.timestamp_pb2 import Timestamp

from nominal.core._clientsbunch import ClientsBunch
from nominal.core._utils.multipart import upload_multipart_file
from nominal.core.filetype import FileType
from nominal.protos.ingest.v2 import (
    common_pb2,
    ingest_service_pb2,
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
