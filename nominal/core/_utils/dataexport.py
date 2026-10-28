from __future__ import annotations

import io
from typing import BinaryIO, Protocol, cast

from google.protobuf.timestamp_pb2 import Timestamp
from typing_extensions import Buffer

from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.protos.dataexports.v1 import data_exports_pb2, data_exports_pb2_grpc
from nominal.protos.types.time import time_pb2
from nominal.ts import Epoch, Iso8601, Relative, _AnyExportableTimestampType, _SecondsNanos, _to_typed_timestamp_type


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
