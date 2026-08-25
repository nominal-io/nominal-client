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

from nominal.core.exceptions import NominalIngestFailed
from nominal.experimental.migration.utils.retry_utils import is_transient_error, retry_transient


def _http_error(status_code: int) -> requests.exceptions.HTTPError:
    return requests.exceptions.HTTPError(f"{status_code} error", response=MagicMock(status_code=status_code))


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
