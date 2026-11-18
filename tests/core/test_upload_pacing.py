from __future__ import annotations

import io
import threading
import time
import weakref
from concurrent.futures import CancelledError, ThreadPoolExecutor
from email.utils import formatdate
from unittest.mock import Mock

import pytest
import requests

from nominal.core._utils.multipart import StreamUploader
from nominal.core._utils.upload_pacing import _ThrottleGate, _wait_for_retry
from nominal.exceptions import NominalRequestThrottledError


def _response(retry_after: str | None = None) -> requests.Response:
    response = requests.Response()
    response.status_code = 429
    if retry_after is not None:
        response.headers["Retry-After"] = retry_after
    return response


@pytest.mark.parametrize("transport_exhausted", [False, True], ids=["raw-429", "transport-retries"])
def test_gate_releases_the_previous_request_before_replay(transport_exhausted: bool) -> None:
    """Replaying an upload drops the previous attempt's prepared request without a GC cycle."""
    gate = _ThrottleGate(max_concurrency=1, deadline_seconds=10, sleep=lambda *_: None)
    previous: weakref.ReferenceType[requests.PreparedRequest] | None = None

    def upload() -> None:
        nonlocal previous
        if previous is None:
            request = requests.Request("POST", "https://example.test/file", data=b"payload").prepare()
            previous = weakref.ref(request)
            if transport_exhausted:
                raise requests.exceptions.RetryError("too many 429 error responses", request=request)
            raise requests.HTTPError("throttled", request=request, response=_response())
        assert previous() is None

    gate.call(upload)


@pytest.mark.parametrize(("retry_after", "expected_wait"), [("5", 5), ("invalid", 0.05), (None, 0.05)])
def test_gate_waits_for_retry_after_or_uses_local_backoff(retry_after: str | None, expected_wait: float) -> None:
    """Valid delay seconds override jitter, while missing or malformed headers use the local delay."""
    sleeps: list[float] = []
    gate = _ThrottleGate(
        max_concurrency=1,
        deadline_seconds=10,
        clock=lambda: sum(sleeps),
        sleep=lambda delay, _: sleeps.append(delay),
        jitter=lambda x: x,
    )
    operation = Mock(side_effect=[requests.HTTPError(response=_response(retry_after)), "uploaded"])

    assert gate.call(operation) == "uploaded"
    assert sleeps == [expected_wait]


def test_gate_accepts_a_long_http_date() -> None:
    """A server's HTTP date can delay a retry beyond urllib3's default six-hour limit."""
    sleeps: list[float] = []
    gate = _ThrottleGate(
        max_concurrency=1,
        deadline_seconds=100_000,
        clock=lambda: sum(sleeps),
        sleep=lambda delay, _: sleeps.append(delay),
    )
    before = time.time()
    retry_at = int(before) + 86_400
    operation = Mock(side_effect=[requests.HTTPError(response=_response(formatdate(retry_at, usegmt=True))), "done"])

    assert gate.call(operation) == "done"
    assert len(sleeps) == 1
    assert retry_at - time.time() <= sleeps[0] <= retry_at - before


def test_retry_after_outlives_one_uploads_budget_and_releases_admission() -> None:
    """A fresh upload on the same gate waits out the cooldown, with its ticket free during both sleeps."""
    sleeps: list[float] = []

    def sleep(seconds: float, _: threading.Event | None) -> None:
        assert gate._semaphore.acquire(blocking=False)
        gate._semaphore.release()
        sleeps.append(seconds)

    gate = _ThrottleGate(max_concurrency=1, deadline_seconds=3, clock=lambda: sum(sleeps), sleep=sleep)
    error = requests.HTTPError(response=_response("10"))
    upload = Mock(side_effect=error)
    with pytest.raises(NominalRequestThrottledError) as failure:
        gate.call(upload)

    upload.assert_called_once()
    assert sleeps == [3]
    assert failure.value.__cause__ is error
    assert gate.call(lambda: sum(sleeps), deadline_seconds=12) == 10
    assert sleeps == [3, 7]


def test_cancellation_wakes_a_throttled_request() -> None:
    """Closing an upload interrupts its Retry-After sleep while the transport is idle."""
    waiting = threading.Event()

    def wait(seconds: float, cancel_event: threading.Event | None) -> None:
        waiting.set()
        _wait_for_retry(seconds, cancel_event)

    gate = _ThrottleGate(max_concurrency=1, deadline_seconds=3, sleep=wait)
    client = Mock()
    client.upload_file.side_effect = requests.HTTPError(response=_response("60"))
    with (
        StreamUploader(client, "auth", None, max_workers=1, max_buffer_bytes=1, gate=gate) as up,
        ThreadPoolExecutor(1) as worker,
    ):
        result = worker.submit(up.upload_small_file, io.BytesIO(b"x"), "small.csv", size=1)
        try:
            assert waiting.wait(timeout=2)
        finally:
            up.cancel()
        with pytest.raises(CancelledError):
            result.result(timeout=2)
    client.upload_file.assert_called_once()
