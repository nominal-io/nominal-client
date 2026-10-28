from __future__ import annotations

import io
from typing import TYPE_CHECKING, BinaryIO, Mapping, Protocol, Sequence, cast

from google.protobuf.timestamp_pb2 import Timestamp
from nominal_api import scout_compute_api, scout_dataexport_api
from typing_extensions import Buffer

from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.protos.compute.v1 import expressions_pb2
from nominal.protos.dataexports.v1 import data_exports_pb2, data_exports_pb2_grpc
from nominal.protos.types import common_pb2
from nominal.protos.types.time import time_pb2
from nominal.ts import (
    Epoch,
    Iso8601,
    Relative,
    _AnyExportableTimestampType,
    _SecondsNanos,
    _to_export_timestamp_format,
    _to_typed_timestamp_type,
)

if TYPE_CHECKING:
    from nominal.core.channel import Channel

MAX_EXPORT_COLUMNS = 1000


class _ExportClients(Protocol):
    @property
    def auth_header(self) -> str: ...
    @property
    def dataexport(self) -> scout_dataexport_api.DataExportService: ...
    @property
    def grpc_dataexport(self) -> data_exports_pb2_grpc.DataExportServiceStub: ...


def _requires_legacy_export(channels: Sequence[Channel], tags: Mapping[str, str] | None) -> bool:
    # Channel names can contain 4096 characters, but proto export headers allow only 1000.
    # The frozen Conjure endpoint also accepts tag shapes outside the proto expression limits.
    return (
        not 1 <= len(channels) <= MAX_EXPORT_COLUMNS
        or any(not 1 <= len(channel.name) <= 1000 for channel in channels)
        or (
            tags is not None
            and (len(tags) > 256 or any(not 1 <= len(key) <= 1000 or len(value) > 10000 for key, value in tags.items()))
        )
    )


def stream_channel_csv(
    clients: _ExportClients,
    channels: Sequence[Channel],
    start: _SecondsNanos,
    end: _SecondsNanos,
    *,
    timestamp_type: _AnyExportableTimestampType = "iso_8601",
    tags: Mapping[str, str] | None = None,
    enable_gzip: bool = True,
    buckets: int | None = None,
    resolution: int | None = None,
) -> BinaryIO:
    """Export CSV, retaining the legacy endpoint only for inputs outside proto validation limits."""
    if buckets is not None and resolution is not None:
        raise ValueError("Only one of buckets or resolution may be provided")
    if _requires_legacy_export(channels, tags):
        request = scout_dataexport_api.ExportDataRequest(
            channels=scout_dataexport_api.ExportChannels(
                time_domain=scout_dataexport_api.ExportTimeDomainChannels(
                    channels=[channel._to_time_domain_channel(tags) for channel in channels],
                    merge_timestamp_strategy=scout_dataexport_api.MergeTimestampStrategy(
                        none=scout_dataexport_api.NoneStrategy()
                    ),
                    output_timestamp_format=_to_export_timestamp_format(timestamp_type),
                )
            ),
            context=scout_compute_api.Context(dataset_references={}, variables={}, function_variables={}),
            start_time=start.to_api(),
            end_time=end.to_api(),
            format=scout_dataexport_api.ExportFormat(csv=scout_dataexport_api.Csv()),
            compression=scout_dataexport_api.CompressionFormat.GZIP if enable_gzip else None,
            resolution=scout_dataexport_api.ResolutionOption(
                buckets=buckets,
                nanoseconds=resolution,
                undecimated=scout_dataexport_api.UndecimatedResolution()
                if buckets is None and resolution is None
                else None,
            ),
        )
        return cast(BinaryIO, clients.dataexport.export_channel_data(clients.auth_header, request))

    # Legacy negative bucket counts produce max(range / buckets, 1) == 1 ns for every valid range.
    if buckets is not None and buckets < 0:
        buckets, resolution = None, 1
    if buckets is not None:
        export_resolution = data_exports_pb2.ExportResolution(bucket_count=buckets)
    elif resolution is not None:
        seconds, nanos = divmod(resolution, 1_000_000_000)
        export_resolution = data_exports_pb2.ExportResolution(step=common_pb2.Duration(seconds=seconds, nanos=nanos))
    else:
        export_resolution = data_exports_pb2.ExportResolution(full=data_exports_pb2.FullResolution())
    definition = data_exports_pb2.ExportDefinition(
        time_domain=data_exports_pb2.TimeDomainColumns(
            columns=[
                data_exports_pb2.ExportColumn(name=channel.name, series=channel._to_proto_compute_series(tags=tags))
                for channel in channels
            ],
            gap_fill=data_exports_pb2.GapFill(leave_empty=data_exports_pb2.LeaveEmpty()),
            timestamp_format=export_timestamp_format(timestamp_type),
        ),
        context=expressions_pb2.ComputeContext(),
        start_time=Timestamp(seconds=start.seconds, nanos=start.nanos),
        end_time=Timestamp(seconds=end.seconds, nanos=end.nanos),
        resolution=export_resolution,
        format=data_exports_pb2.ExportFileFormat(csv=data_exports_pb2.CsvFormat()),
        compression=data_exports_pb2.EXPORT_COMPRESSION_GZIP
        if enable_gzip
        else data_exports_pb2.EXPORT_COMPRESSION_UNSPECIFIED,
    )
    return stream_export(clients.grpc_dataexport, definition)


class _ExportCall(Protocol):
    def __next__(self) -> data_exports_pb2.StreamExportResponse: ...
    def cancel(self) -> bool: ...


class _ExportStream(io.RawIOBase):
    """Adapt an export RPC to a binary stream, retaining at most one unread response chunk."""

    def __init__(self, call: _ExportCall) -> None:
        super().__init__()
        self._call = call
        self._pending = memoryview(b"")

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Buffer) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed export stream")
        target = memoryview(buffer).cast("B")
        if not target:
            return 0
        while not self._pending:
            with translate_grpc_errors():
                response = next(self._call, None)
            if response is None:
                return 0
            self._pending = memoryview(response.data)
        size = min(len(target), len(self._pending))
        target[:size] = self._pending[:size]
        self._pending = self._pending[size:]
        return size

    def close(self) -> None:
        if not self.closed:
            self._call.cancel()
            self._pending = memoryview(b"")
        super().close()


def stream_export(
    service: data_exports_pb2_grpc.DataExportServiceStub, definition: data_exports_pb2.ExportDefinition
) -> BinaryIO:
    """Open an export as a buffered binary stream; the caller owns closing and cancellation."""
    with translate_grpc_errors():
        call = service.StreamExport(data_exports_pb2.StreamExportRequest(definition=definition))
    return cast(BinaryIO, io.BufferedReader(_ExportStream(call)))


def export_timestamp_format(timestamp_type: _AnyExportableTimestampType) -> data_exports_pb2.ExportTimestampFormat:
    timestamp = _to_typed_timestamp_type(timestamp_type)
    if isinstance(timestamp, Iso8601):
        return data_exports_pb2.ExportTimestampFormat(iso8601=data_exports_pb2.Iso8601Timestamps())
    if isinstance(timestamp, (Relative, Epoch)):
        offset = (
            _SecondsNanos.from_flexible(timestamp.start) if isinstance(timestamp, Relative) else _SecondsNanos(0, 0)
        )
        return data_exports_pb2.ExportTimestampFormat(
            relative=data_exports_pb2.RelativeTimestamps(
                relative_to=Timestamp(seconds=offset.seconds, nanos=offset.nanos),
                unit=time_pb2.TimeUnit.Value(timestamp.unit.upper()),
            )
        )
    raise TypeError(f"Unsupported timestamp type for data export: {timestamp_type}")
