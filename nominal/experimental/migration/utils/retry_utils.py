"""Retry helpers for transient network failures during migration.

The conjure client already retries connect errors and 308/429/503 responses with jittered
exponential backoff, but its coverage has holes that have killed real migrations: 502s are
not in its status forcelist, read errors are explicitly not retried (``read=0``), and the
raw ``requests`` streaming used to transfer video files bypasses it entirely. These helpers
close those gaps at the migration layer.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Callable, TypeVar

import requests
import urllib3.exceptions

logger = logging.getLogger(__name__)

T = TypeVar("T")

DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BACKOFF_BASE_SECONDS = 1.0
DEFAULT_BACKOFF_CAP_SECONDS = 60.0

# 429 is throttling; the 5xx set covers transient server-side failures. 4xx (other than 429)
# means the request itself is wrong and will fail identically on every attempt.
_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


def is_transient_error(error: BaseException) -> bool:
    """Whether an exception is a transient network/server failure worth retrying."""
    # Covers ConjureHTTPError too: it subclasses HTTPError and keeps `.response`.
    if isinstance(error, requests.exceptions.HTTPError):
        return error.response is not None and error.response.status_code in _RETRYABLE_STATUS_CODES
    if isinstance(
        error,
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ),
    ):
        return True
    # Streaming a source download straight into an upload surfaces urllib3/socket errors
    # without the requests wrappers (e.g. ProtocolError(ConnectionResetError(104, ...))).
    if isinstance(
        error,
        (urllib3.exceptions.ProtocolError, urllib3.exceptions.TimeoutError, ConnectionError, TimeoutError),
    ):
        return True
    return False


def retry_transient(
    fn: Callable[[], T],
    *,
    description: str,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    backoff_base_seconds: float = DEFAULT_BACKOFF_BASE_SECONDS,
    backoff_cap_seconds: float = DEFAULT_BACKOFF_CAP_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Run ``fn``, retrying transient failures with jittered exponential backoff.

    Non-transient exceptions, and transient ones on the final attempt, propagate unchanged.

    Args:
        fn: Zero-arg callable to run. Must be safe to re-run from scratch on failure.
        description: What is being attempted, for retry log lines.
        max_attempts: Total attempts including the first.
        backoff_base_seconds: Backoff before the second attempt; doubles per attempt.
        backoff_cap_seconds: Upper bound on the backoff window.
        sleep: Injectable for tests.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as error:
            if attempt >= max_attempts or not is_transient_error(error):
                raise
            # Full jitter (uniform over the window), matching the conjure client's style.
            delay = random.uniform(0, min(backoff_cap_seconds, backoff_base_seconds * 2 ** (attempt - 1)))
            logger.warning(
                "Transient failure in %s (attempt %d/%d), retrying in %.1fs: %s",
                description,
                attempt,
                max_attempts,
                delay,
                error,
            )
            sleep(delay)
    raise AssertionError("unreachable: loop either returns or raises")
