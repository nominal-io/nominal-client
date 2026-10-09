"""Execution helpers for parallel resource migration."""

from __future__ import annotations

import concurrent.futures
import logging
import os
from dataclasses import dataclass
from typing import Callable

from nominal.experimental.migration.utils.retry_utils import retry_transient

logger = logging.getLogger(__name__)

# Total attempts per task, including the first. A retried task resumes from migration state,
# so it re-runs only what the failed attempt did not complete. This is the safety net for
# transient failures on calls the per-operation retries don't wrap (e.g. a connection reset
# mid-body on a workbook read, observed killing asset tasks in a production migration).
TASK_ATTEMPTS = 3
_TASK_BACKOFF_BASE_SECONDS = 2.0


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
    on_task_complete: Callable[[], None] | None = None,
    sleep: Callable[[float], None] | None = None,
) -> None:
    """Submit tasks concurrently and raise a RuntimeError listing all failures.

    Each task runs under ``retry_transient``: a transient network failure retries the whole
    task in place with jittered backoff, up to ``TASK_ATTEMPTS`` total attempts. Copies are
    state-resumable, so a retry re-runs only what the failed attempt did not complete. The
    backoff holds that task's worker slot; other tasks keep running on theirs. Non-transient
    failures, and transient ones that exhaust the budget, are collected and raised together.

    Args:
        executor: The thread pool to submit tasks to.
        tasks: The migration tasks to run.
        on_task_complete: Called after every task settles (success or failure) — used to
            persist migration state incrementally. The parallel runner passes a debounced
            save, so persistence may lag by up to one debounce interval; unconditional
            saves happen at the signal flush and the runner's final `finally`.
        sleep: Injectable backoff sleep for tests; defaults to time.sleep.
    """
    if not tasks:
        return

    errors: list[Exception] = []
    futures = {
        executor.submit(
            retry_transient,
            task.fn,
            description=f"migration of {task.label} (rid: {task.rid})",
            max_attempts=TASK_ATTEMPTS,
            backoff_base_seconds=_TASK_BACKOFF_BASE_SECONDS,
            sleep=sleep,
        ): task
        for task in tasks
    }
    for future in concurrent.futures.as_completed(futures):
        task = futures[future]
        try:
            future.result()
            logger.info("Completed migration for %s (rid: %s)", task.label, task.rid)
        except Exception as exc:
            logger.error("Failed to migrate %s (rid: %s)", task.label, task.rid, exc_info=exc)
            errors.append(exc)
        if on_task_complete is not None:
            on_task_complete()
    if errors:
        error_summary = "; ".join(str(e) for e in errors)
        raise RuntimeError(f"Parallel migration had {len(errors)} failure(s): {error_summary}")
