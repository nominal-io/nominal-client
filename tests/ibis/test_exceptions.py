from __future__ import annotations

from typing import Callable

import grpc
import pytest
from google.rpc import error_details_pb2, status_pb2

from nominal.exceptions import NominalError
from nominal.ibis.exceptions import _translate_sql_errors


@pytest.mark.parametrize(
    "metadata",
    [
        None,
        (),
        (("unrelated-trailer", "value"),),
        (("grpc-status-details-bin", b"\xff"),),
        (("grpc-status-details-bin", "not binary"),),
    ],
)
def test_sql_error_survives_missing_or_invalid_metadata(
    metadata: tuple[tuple[str, str | bytes], ...] | None, fake_rpc_error: Callable[..., grpc.RpcError]
) -> None:
    """Missing or unreadable SQL details never hide the original failure or its cause."""
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
    """Unknown protobuf details and malformed ErrorInfo entries preserve the SQL failure."""
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
