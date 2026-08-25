"""Transient-vs-permanent classification and backoff behavior for migration retries.

Each transient case mirrors a failure observed in a real tenant migration that the conjure
client's built-in retry did not cover (502 not in its forcelist, read errors excluded, raw
streaming transfers bypassing it entirely).
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest
import requests
import urllib3.exceptions

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

import grpc
from conjure_python_client import ConjureHTTPError

from nominal.core.exceptions import (
    NominalError,
    NominalIngestFailed,
    NominalInvalidArgumentError,
    NominalMultipartUploadError,
    NominalMultipartUploadFailed,
)
from nominal.experimental.migration.utils.retry_utils import is_transient_error, retry_transient


def _http_error(status_code: int) -> requests.exceptions.HTTPError:
    return requests.exceptions.HTTPError(f"{status_code} error", response=MagicMock(status_code=status_code))


def _conjure_http_error(status_code: int, body: dict | None = None) -> ConjureHTTPError:
    """Build a ConjureHTTPError the way the conjure client raises it, so the classification
    test exercises the real class rather than a mock stand-in.
    """
    response = MagicMock()
    response.status_code = status_code
    response.headers = {"X-B3-TraceId": "abc123"}
    if body is None:
        response.json.side_effect = ValueError("no json body")
        response.text = "gateway error"
    else:
        response.json.return_value = body
    return ConjureHTTPError(requests.exceptions.HTTPError(f"{status_code} error", response=response))


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode) -> None:
        self._code = code

    def code(self) -> grpc.StatusCode:
        return self._code


def _grpc_translated_error(code: grpc.StatusCode, exc_type: type[NominalError] = NominalError) -> NominalError:
    """Build the shape translate_grpc_errors raises: a NominalError chaining the grpc.RpcError."""
    error = exc_type(f"{code}: boom")
    error.__cause__ = _FakeRpcError(code)
    return error


def _multipart_upload_failed(root_cause: BaseException) -> NominalMultipartUploadFailed:
    """Build the shape put_multipart_upload raises: an ExceptionGroup of per-attempt wrappers,
    each chaining the real failure via __cause__; the group itself has no __cause__.
    """
    attempt_error = NominalMultipartUploadError(f"part 1, attempt 3: {root_cause}")
    attempt_error.__cause__ = root_cause
    return NominalMultipartUploadFailed("Multipart upload failed after 3 attempts", [attempt_error])


@pytest.mark.parametrize(
    "error",
    [
        urllib3.exceptions.ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')"),
        requests.exceptions.ReadTimeout("HTTPSConnectionPool(host='api.example.com', port=443): Read timed out."),
        requests.exceptions.ConnectionError("connection aborted"),
        requests.exceptions.ChunkedEncodingError("Connection broken"),
        ConnectionResetError(104, "Connection reset by peer"),
        _http_error(502),
        _http_error(503),
        _http_error(429),
        # The ingest status poll goes through conjure — if ConjureHTTPError ever stops carrying
        # `.response`, 502s during polling silently become permanent. Pin the real class.
        _conjure_http_error(502),
        _conjure_http_error(503, body={"errorCode": "UNAVAILABLE", "errorName": "Default:Unavailable"}),
        # The multipart upload leg is gRPC: connection refused arrives as a NominalError
        # chaining an UNAVAILABLE grpc.RpcError (via translate_grpc_errors).
        _grpc_translated_error(grpc.StatusCode.UNAVAILABLE),
        _grpc_translated_error(grpc.StatusCode.DEADLINE_EXCEEDED),
        _FakeRpcError(grpc.StatusCode.UNAVAILABLE),
        # A 502 on sign_part surfaces as an ExceptionGroup with the real error two levels down.
        _multipart_upload_failed(_conjure_http_error(502)),
        # The multipart session's own urllib3 Retry exhausting its budget on sustained S3 5xx
        # raises RetryError with no __cause__ — it must be transient by type alone.
        requests.exceptions.RetryError("too many 503 error responses"),
        urllib3.exceptions.MaxRetryError(MagicMock(), "https://s3.example.com/part"),
        _multipart_upload_failed(requests.exceptions.RetryError("too many 503 error responses")),
    ],
)
def test_transient_errors_are_classified_transient(error: BaseException) -> None:
    assert is_transient_error(error)


@pytest.mark.parametrize(
    "error",
    [
        _http_error(400),
        _http_error(403),
        _http_error(404),
        _conjure_http_error(400, body={"errorCode": "INVALID_ARGUMENT", "errorName": "Default:InvalidArgument"}),
        _grpc_translated_error(grpc.StatusCode.INVALID_ARGUMENT, NominalInvalidArgumentError),
        _grpc_translated_error(grpc.StatusCode.NOT_FOUND),
        _multipart_upload_failed(_conjure_http_error(403)),
        NominalIngestFailed("Video failed to segment. (VideoSegmenter:Internal)"),
        ValueError("bad ingest options"),
    ],
)
def test_permanent_errors_are_classified_permanent(error: BaseException) -> None:
    assert not is_transient_error(error)


def test_retry_transient_returns_after_transient_failures() -> None:
    calls = {"n": 0}
    delays: list[float] = []

    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise _http_error(502)
        return "ok"

    result = retry_transient(flaky, description="test op", sleep=delays.append)

    assert result == "ok"
    assert calls["n"] == 3
    # Jittered exponential backoff: each delay falls inside its doubling window.
    assert len(delays) == 2
    assert 0 <= delays[0] <= 1.0
    assert 0 <= delays[1] <= 2.0


def test_retry_transient_raises_permanent_error_immediately() -> None:
    calls = {"n": 0}

    def failing() -> None:
        calls["n"] += 1
        raise _http_error(400)

    with pytest.raises(requests.exceptions.HTTPError):
        retry_transient(failing, description="test op", sleep=lambda _: None)
    assert calls["n"] == 1


def test_retry_transient_gives_up_after_max_attempts() -> None:
    calls = {"n": 0}

    def always_transient() -> None:
        calls["n"] += 1
        raise requests.exceptions.ReadTimeout("Read timed out.")

    with pytest.raises(requests.exceptions.ReadTimeout):
        retry_transient(always_transient, description="test op", max_attempts=3, sleep=lambda _: None)
    assert calls["n"] == 3


def test_retry_transient_caps_backoff_window() -> None:
    delays: list[float] = []
    calls = {"n": 0}

    def flaky() -> None:
        calls["n"] += 1
        if calls["n"] < 8:
            raise _http_error(502)

    retry_transient(flaky, description="test op", max_attempts=8, backoff_cap_seconds=4.0, sleep=delays.append)

    assert all(delay <= 4.0 for delay in delays)
