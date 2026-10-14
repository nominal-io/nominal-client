from __future__ import annotations

from contextlib import contextmanager
from typing import Iterable, Iterator, cast

import grpc
from google.protobuf.message import DecodeError
from google.rpc import error_details_pb2, status_pb2
from ibis.common.exceptions import IbisError

from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.exceptions import NominalError


class NominalSqlError(IbisError):
    """A catalog type or query result that cannot be represented by this backend."""


@contextmanager
def _translate_sql_errors() -> Iterator[None]:
    """Add SQL response details to the SDK's standard gRPC exceptions.

    The shared translator owns status-code mapping and chaining the original RPC error.
    """
    try:
        with translate_grpc_errors():
            yield
    except NominalError as error:
        # The shared handler keeps the original gRPC error in __cause__.
        # Read that error to access the server's extra SQL error details.
        rpc_error = error.__cause__
        if not isinstance(rpc_error, grpc.RpcError):
            raise
        lines = [str(error)]
        metadata = rpc_error.trailing_metadata() or ()
        # gRPC returns iterable key/value pairs, despite the non-iterable entries declared in its stubs.
        for key, value in cast(Iterable[tuple[str, str | bytes]], metadata):
            if key != "grpc-status-details-bin" or not isinstance(value, bytes):
                continue
            try:
                status = status_pb2.Status.FromString(value)
                for detail in status.details:
                    info = error_details_pb2.ErrorInfo()
                    if detail.Unpack(info):
                        lines.append(f"{info.domain}/{info.reason}")
                        lines.extend(f"{name}: {text}" for name, text in sorted(info.metadata.items()))
            except DecodeError:
                # Malformed error details must not hide the original RPC failure.
                continue
        error.args = ("\n".join(lines),)
        raise
