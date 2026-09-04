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

import grpc
import requests
import urllib3.exceptions

# Remove this import once the minimum supported Python version is 3.11+.
from exceptiongroup import BaseExceptionGroup

logger = logging.getLogger(__name__)

T = TypeVar("T")

DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BACKOFF_BASE_SECONDS = 1.0
DEFAULT_BACKOFF_CAP_SECONDS = 60.0

# 408/429 are timeout and throttling; every 5xx is server-side, including codes proxies invent
# (e.g. CDN 520–524). Enumerating specific 5xx codes is what left 502 unretried in the conjure
# client's forcelist — match on the class instead. Any other 4xx means the request itself is
# wrong and will fail identically on every attempt. Mirrors _is_transient_upload_error in
# nominal/experimental/ingest/_multipart_uploader.py.
_RETRYABLE_EXACT_STATUS_CODES = frozenset({408, 429})

# The standard retryable gRPC statuses: server unreachable, quota/throttle exhausted, or the
# deadline hit.
_RETRYABLE_GRPC_STATUS_CODES = frozenset(
    {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.RESOURCE_EXHAUSTED}
)


def is_transient_error(error: BaseException) -> bool:
    """Whether an exception is a transient network/server failure worth retrying."""
    # Covers ConjureHTTPError too: it subclasses HTTPError and keeps `.response`.
    if isinstance(error, requests.exceptions.HTTPError):
        return error.response is not None and (
            error.response.status_code in _RETRYABLE_EXACT_STATUS_CODES or error.response.status_code >= 500
        )
    # Requests-level connection/timeout failures, plus the urllib3/socket errors that
    # streaming a source download straight into an upload surfaces without the requests
    # wrappers (e.g. ProtocolError(ConnectionResetError(104, ...))).
    if isinstance(
        error,
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
            # Raised when a session's own urllib3 Retry exhausts its status_forcelist budget
            # (e.g. sustained S3 503s on multipart part PUTs); requests raises it without
            # `from`, so there is no __cause__ to follow.
            requests.exceptions.RetryError,
            urllib3.exceptions.MaxRetryError,
            urllib3.exceptions.ProtocolError,
            urllib3.exceptions.TimeoutError,
            ConnectionError,
            TimeoutError,
        ),
    ):
        return True
    # gRPC legs surface either raw or as NominalError subclasses via translate_grpc_errors,
    # which chains the original grpc.RpcError — classify by its status.
    if isinstance(error, grpc.RpcError):
        return error.code() in _RETRYABLE_GRPC_STATUS_CODES
    # Wrapper shapes: multipart upload failures arrive as an ExceptionGroup of per-attempt
    # errors (the group itself has no __cause__), and each member — like translate_grpc_errors'
    # output — chains the real failure via __cause__. Classify by what's underneath.
    if isinstance(error, BaseExceptionGroup):
        # A group is a set of failed attempts: transient only if every attempt was, matching
        # _is_transient_upload_error — a permanent failure anywhere (e.g. a 403 from rotated
        # credentials) means a retry would spend the whole transfer to fail the same way.
        return all(is_transient_error(inner) for inner in error.exceptions)
    if error.__cause__ is not None:
        return is_transient_error(error.__cause__)
    return False


def retry_transient(
    fn: Callable[[], T],
    *,
    description: str,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    backoff_base_seconds: float = DEFAULT_BACKOFF_BASE_SECONDS,
    backoff_cap_seconds: float = DEFAULT_BACKOFF_CAP_SECONDS,
    sleep: Callable[[float], None] | None = None,
) -> T:
    """Run ``fn``, retrying transient failures with jittered exponential backoff.

    Non-transient exceptions, and transient ones on the final attempt, propagate unchanged.

    Args:
        fn: Zero-arg callable to run. Must be safe to re-run from scratch on failure.
        description: What is being attempted, for retry log lines.
        max_attempts: Total attempts including the first.
        backoff_base_seconds: Backoff before the second attempt; doubles per attempt.
        backoff_cap_seconds: Upper bound on the backoff window.
        sleep: Injectable for tests; defaults to time.sleep, resolved at call time so
            monkeypatching the time module works for callers that can't pass this through.
    """
    sleep_fn = time.sleep if sleep is None else sleep
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
            sleep_fn(delay)
    raise AssertionError("unreachable: loop either returns or raises")
