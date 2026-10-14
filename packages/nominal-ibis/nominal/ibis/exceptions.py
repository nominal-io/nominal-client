from __future__ import annotations

from contextlib import contextmanager
from typing import Iterable, Iterator, cast

import grpc
from google.protobuf.message import DecodeError
from google.rpc import error_details_pb2, status_pb2
from ibis.common.exceptions import IbisError

from nominal.core._utils.grpc_tools import translate_grpc_errors


class NominalSqlError(IbisError):
    """A SQL service error, unsupported catalog type, or invalid query result.

    Server errors include SQL details and keep the original gRPC error as their cause.
    Errors without SQL details use the standard SDK exceptions, as do authentication and permission errors.
    """


@contextmanager
def _translate_sql_errors() -> Iterator[None]:
    """Raise NominalSqlError for SQL failures with details from the server."""
    with translate_grpc_errors():
        try:
            yield
        except grpc.RpcError as error:
            message = _sql_error_message(error)
            if message is None:
                raise
            raise NominalSqlError(message) from error


def _sql_error_message(error: grpc.RpcError) -> str | None:
    """Return a SQL error message, or None to use the standard SDK error."""
    if error.code() in (grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.PERMISSION_DENIED):
        return None
    lines: list[str] = []
    metadata = error.trailing_metadata() or ()
    # The gRPC stubs don't type these (key, value) pairs correctly.
    for key, value in cast(Iterable[tuple[str, str | bytes]], metadata):
        if key != "grpc-status-details-bin" or not isinstance(value, bytes):
            continue
        try:
            status = status_pb2.Status.FromString(value)
        except DecodeError:
            # Keep the query error if its details can't be decoded.
            continue
        for detail in status.details:
            info = error_details_pb2.ErrorInfo()
            try:
                if not detail.Unpack(info):
                    continue
            except DecodeError:
                continue
            # Older servers use the "sql" domain.
            if info.domain not in ("sql", "nominal.sql.v1"):
                continue
            lines.append(f"{info.domain}/{info.reason}")
            lines.extend(f"{name}: {text}" for name, text in sorted(info.metadata.items()))
    if not lines:
        return None
    return "\n".join([f"{error.code()}: {error.details()}", *lines])
