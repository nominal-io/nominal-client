"""Share Nominal request concurrency, throttle backoff and Retry-After across one uploader."""

from __future__ import annotations

import logging
import random
import sys
import threading
import time
from concurrent.futures import CancelledError
from typing import Callable, TypeVar

import requests
from urllib3.exceptions import InvalidHeader
from urllib3.util import Retry

from nominal.exceptions import NominalRequestThrottledError

logger = logging.getLogger(__name__)

T = TypeVar("T")

NOMINAL_MAX_CONCURRENCY = 5
DEFAULT_THROTTLE_DEADLINE_S = 120.0
DEFAULT_MAX_BACKOFF_DURATION_S = 2.0
_ABORT_THROTTLE_DEADLINE_S = 5.0
_BACKOFF_BASE_S = 0.05
_BACKOFF_DECAY = 0.9
# Use urllib3's HTTP-date and delta-seconds parser. Each caller has its own deadline,
# but a long server cooldown must still apply to future calls on this uploader.
_RETRY_AFTER = Retry(retry_after_max=sys.maxsize)


def _throttle_retry_after(exc: requests.HTTPError | requests.exceptions.RetryError) -> float | None:
    """Return the server's minimum delay, zero for an unspecified delay, or None for an unrelated error."""
    if isinstance(exc, requests.exceptions.RetryError):
        # Requests translates exhausted transport status retries without the final response.
        return 0.0
    if exc.response is None or exc.response.status_code not in (429, 503):
        return None
    if retry_after := exc.response.headers.get("Retry-After"):
        try:
            return _RETRY_AFTER.parse_retry_after(retry_after)
        except (InvalidHeader, ValueError, OverflowError):
            pass
    return 0.0


def _wait_for_retry(seconds: float, cancel_event: threading.Event | None) -> None:
    if cancel_event is None:
        time.sleep(seconds)
    elif cancel_event.wait(seconds):
        raise CancelledError("uploader is closing")


class _ThrottleGate:
    """Bound concurrent requests and share backoff across files, including whole-file retries."""

    def __init__(
        self,
        *,
        max_concurrency: int,
        deadline_seconds: float,
        max_backoff_duration: float = DEFAULT_MAX_BACKOFF_DURATION_S,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float, threading.Event | None], None] = _wait_for_retry,
        jitter: Callable[[float], float] | None = None,
    ) -> None:
        self._semaphore = threading.Semaphore(max_concurrency)
        self._backoff_lock = threading.Lock()
        self._max_backoff_duration = max_backoff_duration
        self._backoff = 0.0
        self._retry_at = float("-inf")
        self._deadline_seconds = deadline_seconds
        self._clock = clock
        self._sleep = sleep
        self._jitter = jitter if jitter is not None else (lambda delay: random.uniform(0.0, delay))

    def _admit(self, deadline_at: float, cancel_event: threading.Event | None) -> bool:
        while (remaining := deadline_at - self._clock()) > 0 and self._semaphore.acquire(timeout=remaining):
            now = self._clock()
            with self._backoff_lock:
                cooldown = self._retry_at - now
            if cooldown <= 0 and now < deadline_at:
                return True
            self._semaphore.release()
            if now < deadline_at:
                self._sleep(min(cooldown, deadline_at - now), cancel_event)
        return False

    def call(
        self, op: Callable[[], T], *, deadline_seconds: float | None = None, cancel_event: threading.Event | None = None
    ) -> T:
        """Run `op` under lane admission, retrying for as long as the server throttles it.

        Args:
            op: The request to run. It is retried verbatim on a throttle, so it must be safe
                to repeat.
            deadline_seconds: Budget for admission and retry waits, including elapsed time
                in earlier attempts. An in-flight transport request can outlast the deadline.
            cancel_event: Interrupt gate-owned retry waits and stop the next admitted request.
                Cleanup requests can omit this event to run during cancellation.

        Returns:
            Whatever `op` returns on the first attempt the server does not throttle.

        Raises:
            NominalRequestThrottledError: The budget ran out — either the server kept
                throttling the request, or the lane could not admit it in time. `__cause__`
                is the last throttle seen, or None if the budget expired before any attempt
                was made.
            Exception: Any non-throttle error from `op`, re-raised unchanged from the attempt
                that produced it (never retried).
        """
        budget = self._deadline_seconds if deadline_seconds is None else deadline_seconds
        started = self._clock()
        deadline_at = started + budget
        attempt = 0
        last_throttle: requests.RequestException | None = None
        while True:
            if not self._admit(deadline_at, cancel_event):
                raise self._exhausted(
                    "the nominal lane could not admit this request", started, budget, attempt
                ) from last_throttle
            # Admission succeeded. The previous request (including its body and stack)
            # no longer needs to be kept as the cause of an admission timeout.
            last_throttle = None
            try:
                if cancel_event is not None and cancel_event.is_set():
                    raise CancelledError("uploader is closing")
                result = op()
            except (requests.HTTPError, requests.exceptions.RetryError) as exc:
                retry_after = _throttle_retry_after(exc)
                if retry_after is None:
                    raise
                # Publish the cooldown before releasing this ticket to another caller.
                with self._backoff_lock:
                    self._retry_at = max(self._retry_at, self._clock() + retry_after)
                    backoff = min(self._max_backoff_duration, max(_BACKOFF_BASE_S, self._backoff * 2))
                    self._backoff = backoff
                last_throttle = exc
            else:
                with self._backoff_lock:
                    self._backoff *= _BACKOFF_DECAY
                    if self._backoff <= _BACKOFF_BASE_S / 2:
                        self._backoff = 0.0
                return result
            finally:
                self._semaphore.release()

            attempt += 1
            elapsed = self._clock() - started
            logger.debug(
                "Upload request throttled (attempt %d, %.1fs of %.0fs, local backoff %.3fs)",
                attempt,
                elapsed,
                budget,
                backoff,
            )
            if elapsed >= budget:
                raise self._exhausted(
                    "server kept throttling this request", started, budget, attempt
                ) from last_throttle
            delay = max(retry_after, self._jitter(backoff))
            if delay > 0.0:
                self._sleep(min(delay, budget - elapsed), cancel_event)

    def _exhausted(self, detail: str, started: float, budget: float, attempts: int) -> NominalRequestThrottledError:
        elapsed = self._clock() - started
        return NominalRequestThrottledError(
            f"{detail} after {elapsed:.1f}s of a {budget}s budget across {attempts} attempts; giving up"
        )
