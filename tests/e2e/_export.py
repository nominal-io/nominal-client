"""Readiness polling for exports of freshly ingested E2E datasets."""

from __future__ import annotations

import time
from typing import Callable, Sized, TypeVar

from conjure_python_client import ConjureHTTPError

_Export = TypeVar("_Export", bound=Sized)

# Avoid hammering the export endpoint while its ingestion commit becomes visible.
EXPORT_POLL_INTERVAL_SECONDS = 2
DEFAULT_EXPORT_TIMEOUT_SECONDS = 120


def wait_for_export(
    export: Callable[[], _Export],
    expected_rows: int,
    *,
    is_ready: Callable[[_Export], bool] | None = None,
    timeout_seconds: float = DEFAULT_EXPORT_TIMEOUT_SECONDS,
    now: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> _Export:
    """Poll a fixed dataset's export until its rows and optional readiness check match.

    Args:
        export: Read-only export operation that is safe to retry.
        expected_rows: Exact row count of the fixed input dataset.
        is_ready: Additional readiness check, such as matching DataFrame column names.
        timeout_seconds: Remaining polling budget; zero still allows one immediate read.
        now: Monotonic clock used to measure the polling budget.
        sleep: Wait function used between attempts.

    Returns:
        The first export matching both readiness checks.

    Raises:
        AssertionError: The export has excess rows or exhausts its readiness budget.
        ConjureHTTPError: An unrelated HTTP error, or the last missing-table error at timeout.

    The budget bounds retries; an individual export retains its own request timeout.
    """
    # Ingestion completion can precede Iceberg table creation and data visibility.
    # Missing tables currently produce Default:InvalidArgument; subsequent reads
    # can be empty or partial until the commit becomes visible.
    deadline = now() + timeout_seconds
    while True:
        try:
            result = export()
        except ConjureHTTPError as exc:
            if (
                exc.response.status_code != 400
                or getattr(exc, "error_name", None) != "Default:InvalidArgument"
                or now() >= deadline
            ):
                raise
        else:
            rows = len(result)
            if rows > expected_rows:
                raise AssertionError(f"Export has excess rows: expected {expected_rows} rows, got {rows}")
            if rows == expected_rows and (is_ready is None or is_ready(result)):
                return result
            if now() >= deadline:
                raise AssertionError(
                    f"Export readiness budget exhausted: expected {expected_rows} rows, got {rows}"
                    + ("; additional readiness check failed" if rows == expected_rows else "")
                )
        sleep(min(EXPORT_POLL_INTERVAL_SECONDS, max(0, deadline - now())))
