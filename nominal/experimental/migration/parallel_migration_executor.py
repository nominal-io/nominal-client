"""Execution helpers for parallel resource migration."""

from __future__ import annotations

import concurrent.futures
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Callable

from nominal.experimental.migration.utils.retry_utils import DEFAULT_BACKOFF_CAP_SECONDS, is_transient_error

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
) -> None:
    """Submit tasks concurrently and raise a RuntimeError listing all failures.

    A task that fails with a transient network error is resubmitted with jittered backoff, up
    to ``TASK_ATTEMPTS`` total attempts — copies are state-resumable, so a retried task skips
    children the failed attempt already migrated. Non-transient failures, and transient ones
    that exhaust the budget, are collected and raised together at the end.

    Args:
        executor: The thread pool to submit tasks to.
        tasks: The migration tasks to run.
        on_task_complete: Called after every task attempt settles (success or failure) — used
            to persist migration state incrementally. The parallel runner passes a debounced
            save, so persistence may lag by up to one debounce interval; unconditional
            saves happen at the signal flush and the runner's final `finally`.
    """
    if not tasks:
        return

    errors: list[Exception] = []
    attempts_by_task_id: dict[int, int] = {}
    pending: dict[concurrent.futures.Future[None], MigrationTask] = {executor.submit(task.fn): task for task in tasks}
    while pending:
        done, _ = concurrent.futures.wait(pending, return_when=concurrent.futures.FIRST_COMPLETED)
        for future in done:
            task = pending.pop(future)
            try:
                future.result()
                logger.info("Completed migration for %s (rid: %s)", task.label, task.rid)
            except Exception as exc:
                attempt = attempts_by_task_id.get(id(task), 1)
                if is_transient_error(exc) and attempt < TASK_ATTEMPTS:
                    attempts_by_task_id[id(task)] = attempt + 1
                    # Full jitter over a doubling window, matching the per-operation retries.
                    delay = random.uniform(
                        0, min(DEFAULT_BACKOFF_CAP_SECONDS, _TASK_BACKOFF_BASE_SECONDS * 2 ** (attempt - 1))
                    )
                    logger.warning(
                        "Transient failure migrating %s (rid: %s) on attempt %d/%d — retrying in %.1fs, "
                        "resuming from migration state: %s",
                        task.label,
                        task.rid,
                        attempt,
                        TASK_ATTEMPTS,
                        delay,
                        exc,
                    )
                    # The delay runs inside the worker so this loop never blocks other tasks.
                    pending[executor.submit(_delayed(task.fn, delay))] = task
                else:
                    logger.error("Failed to migrate %s (rid: %s)", task.label, task.rid, exc_info=exc)
                    errors.append(exc)
            if on_task_complete is not None:
                on_task_complete()
    if errors:
        error_summary = "; ".join(str(e) for e in errors)
        raise RuntimeError(f"Parallel migration had {len(errors)} failure(s): {error_summary}")


def _delayed(fn: Callable[[], None], delay_seconds: float) -> Callable[[], None]:
    def run() -> None:
        time.sleep(delay_seconds)
        fn()

    return run
