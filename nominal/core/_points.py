from __future__ import annotations

from collections.abc import Iterator
from threading import Event

import grpc

from nominal.core._utils.grpc_tools import grpc_error, translate_grpc_errors
from nominal.exceptions import NominalError
from nominal.protos.channel_writer.v2 import nominal_channel_writer_pb2 as writer_pb2
from nominal.protos.channel_writer.v2 import nominal_channel_writer_pb2_grpc


def _write_points(
    writer: nominal_channel_writer_pb2_grpc.NominalChannelWriterServiceStub,
    schema: writer_pb2.Schema,
    points: writer_pb2.PointData,
) -> None:
    """Declare one schema, send one chunk, and return when the chunk is accepted."""
    declared = Event()
    stopped = Event()

    def requests() -> Iterator[writer_pb2.WriteStreamRequest]:
        """Wait for schema acknowledgement before yielding its dependent chunk."""
        yield writer_pb2.WriteStreamRequest(
            sequence_number=1, declare_schemas=writer_pb2.DeclareSchemas(schemas=[schema])
        )
        declared.wait()
        if not stopped.is_set():
            yield writer_pb2.WriteStreamRequest(
                sequence_number=2,
                chunk=writer_pb2.WriteChunk(
                    channels=[writer_pb2.ChannelData(schema_id=schema.schema_id, points=points)]
                ),
            )

    pending = 1
    call = None
    try:
        with translate_grpc_errors():
            call = writer.WriteStream(requests())
            for response in call:
                kind = response.WhichOneof("response")
                if kind == "accepted" and not response.accepted.sequence_numbers:
                    continue  # An Accepted with no sequence numbers acknowledges nothing.
                elif kind == "accepted" and list(response.accepted.sequence_numbers) == [pending]:
                    if pending == 1:
                        pending = 2
                        declared.set()
                    else:
                        return
                elif kind == "rejected" and response.rejected.sequence_number == pending:
                    status = response.rejected.status
                    code = next((code for code in grpc.StatusCode if code.value[0] == status.code), None)
                    if code is None or code == grpc.StatusCode.OK:
                        raise NominalError("Write stream returned an invalid rejection status")
                    raise grpc_error(code, status.message)
                else:
                    raise NominalError("Write stream returned an unexpected acknowledgement")
            raise NominalError("Write stream ended before acknowledging all messages")
    finally:
        # Unblock gRPC's request iterator on rejection, deadline, or an interrupted response loop.
        stopped.set()
        declared.set()
        if call is not None:
            call.cancel()
