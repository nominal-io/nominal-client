from __future__ import annotations

from typing import Callable

import grpc
import pytest
from google.rpc import error_details_pb2, status_pb2

from nominal.exceptions import (
    NominalAuthenticationError,
    NominalError,
    NominalInvalidArgumentError,
    NominalPermissionDeniedError,
)
from nominal.ibis.exceptions import _translate_sql_errors


@pytest.mark.parametrize(
    "code,domain,exception_type",
    [
        (grpc.StatusCode.INVALID_ARGUMENT, "nominal.other.v1", NominalInvalidArgumentError),
        (grpc.StatusCode.UNAUTHENTICATED, "nominal.sql.v1", NominalAuthenticationError),
        (grpc.StatusCode.PERMISSION_DENIED, "nominal.sql.v1", NominalPermissionDeniedError),
    ],
)
def test_non_sql_and_authentication_errors_use_standard_sdk_exceptions(
    code: grpc.StatusCode, domain: str, exception_type: type[NominalError], fake_rpc_error: Callable[..., grpc.RpcError]
) -> None:
    """Authentication, permission, and non-SQL errors keep their SDK exception types."""
    status = status_pb2.Status(message="request failed")
    status.details.add().Pack(error_details_pb2.ErrorInfo(domain=domain, reason="REQUEST_FAILED"))
    error = fake_rpc_error(code, status.message, (("grpc-status-details-bin", status.SerializeToString()),))

    with pytest.raises(exception_type, match="request failed") as exc_info:
        with _translate_sql_errors():
            raise error

    assert exc_info.value.__cause__ is error


@pytest.mark.parametrize(
    "metadata",
    [
        None,
        (("unrelated-trailer", "value"),),
        (("grpc-status-details-bin", b"\xff"),),
        (("grpc-status-details-bin", "not binary"),),
    ],
)
def test_sql_error_survives_missing_or_invalid_metadata(
    metadata: tuple[tuple[str, str | bytes], ...] | None, fake_rpc_error: Callable[..., grpc.RpcError]
) -> None:
    """Missing or unreadable details preserve the original error."""
    error = fake_rpc_error(grpc.StatusCode.INTERNAL, "original failure", metadata)
    with pytest.raises(NominalError) as exc_info:
        with _translate_sql_errors():
            raise error
    assert "INTERNAL" in str(exc_info.value)
    assert "original failure" in str(exc_info.value)
    assert exc_info.value.__cause__ is error


@pytest.mark.parametrize("malformed", [False, True])
def test_sql_error_survives_unknown_or_malformed_detail(
    malformed: bool, fake_rpc_error: Callable[..., grpc.RpcError]
) -> None:
    """Unknown or malformed details preserve the original error."""
    status = status_pb2.Status(code=13, message="original failure")
    if malformed:
        status.details.add(type_url="type.googleapis.com/google.rpc.ErrorInfo", value=b"\xff")
    else:
        status.details.add().Pack(error_details_pb2.RetryInfo())
    error = fake_rpc_error(
        grpc.StatusCode.INTERNAL, status.message, (("grpc-status-details-bin", status.SerializeToString()),)
    )
    with pytest.raises(NominalError) as exc_info:
        with _translate_sql_errors():
            raise error
    assert "original failure" in str(exc_info.value)
    assert exc_info.value.__cause__ is error
