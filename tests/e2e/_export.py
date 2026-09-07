"""Readiness polling for exports of freshly ingested E2E datasets."""

from __future__ import annotations

import time
from typing import Callable, Sized, TypeVar

from conjure_python_client import ConjureHTTPError

_Export = TypeVar("_Export", bound=Sized)


def _wait_for_export(export: Callable[[], _Export], expected_rows: int, *, timeout_seconds: float = 120) -> _Export:
    # Ingestion completion can precede Iceberg table creation and data visibility.
    # Missing tables currently produce Default:InvalidArgument; subsequent reads
    # can be empty or partial until the commit becomes visible.
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            result = export()
        except ConjureHTTPError as exc:
            if (
                exc.response.status_code != 400
                or getattr(exc, "error_name", None) != "Default:InvalidArgument"
                or time.monotonic() >= deadline
            ):
                raise
        else:
            if len(result) == expected_rows:
                return result
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"Export did not become readable: expected {expected_rows} rows, got {len(result)}"
                )
        time.sleep(2)
