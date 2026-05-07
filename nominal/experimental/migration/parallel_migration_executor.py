"""Execution helpers for parallel resource migration."""

from __future__ import annotations

import concurrent.futures
import logging
import os
from dataclasses import dataclass
from typing import Callable

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MigrationTask:
    rid: str
    label: str
    fn: Callable[[], None]


def validate_max_workers(max_workers: int) -> int:
    """Validate and clamp max_workers to [1, cpu_count]."""
    cpu_count = os.cpu_count() or 4
    return max(1, min(max_workers, cpu_count))


def run_concurrent(
    executor: concurrent.futures.ThreadPoolExecutor,
    tasks: list[MigrationTask],
) -> None:
    """Submit tasks concurrently and raise a RuntimeError listing all failures."""
    if not tasks:
        return

    errors: list[Exception] = []
    futures = {executor.submit(task.fn): task for task in tasks}
    for future in concurrent.futures.as_completed(futures):
        task = futures[future]
        try:
            future.result()
            logger.info("Completed migration for %s (rid: %s)", task.label, task.rid)
        except Exception as exc:  # pragma: no cover - exercised by production callers
            logger.error("Failed to migrate %s (rid: %s)", task.label, task.rid, exc_info=exc)
            errors.append(exc)
    if errors:
        error_summary = "; ".join(str(e) for e in errors)
        raise RuntimeError(f"Parallel migration had {len(errors)} failure(s): {error_summary}")
