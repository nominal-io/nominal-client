"""Parallel helpers for resource migration."""

from __future__ import annotations

import concurrent.futures
import logging
import signal
import threading
import time
from contextlib import contextmanager
from types import FrameType
from typing import Callable, Iterator

from nominal.core.checklist import Checklist
from nominal.experimental.migration.config.migration_resources import AssetResources
from nominal.experimental.migration.migration_runner import MigrationRunner, log_skipped_resources
from nominal.experimental.migration.migrator.asset_migrator import AssetCopyOptions, AssetMigrator
from nominal.experimental.migration.migrator.checklist_migrator import ChecklistCopyOptions, ChecklistMigrator
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.migrator.workbook_template_migrator import WorkbookTemplateMigrator
from nominal.experimental.migration.parallel_migration_executor import (
    MigrationTask,
    run_concurrent,
    validate_max_workers,
)
from nominal.experimental.migration.parallel_migration_state import ThreadSafeMigrationState

logger = logging.getLogger(__name__)


def _make_asset_fn(
    asset_resources: AssetResources, asset_migrator: AssetMigrator, asset_copy_options: AssetCopyOptions
) -> Callable[[], None]:
    def fn() -> None:
        asset_migrator.copy_from(asset_resources.asset, asset_copy_options)

    return fn


def _make_template_fn(template: object, template_migrator: WorkbookTemplateMigrator) -> Callable[[], None]:
    def fn() -> None:
        template_migrator.clone(template)  # type: ignore[arg-type]

    return fn


def _make_checklist_fn(checklist: Checklist, checklist_migrator: ChecklistMigrator) -> Callable[[], None]:
    def fn() -> None:
        # ChecklistMigrator.clone() raises NotImplementedError; use copy_from() to clone the definition.
        checklist_migrator.copy_from(checklist, ChecklistCopyOptions())

    return fn


class _DebouncedSave:
    """Rate-limit state saves triggered by per-mapping persist hooks.

    Serializing the state is O(state size), and file-heavy assets record a mapping per
    dataset file — saving on every mapping would be quadratic over a large migration.

    The interval is measured from when the previous save *finished* and scales with how
    long that save took (``interval_scale`` x duration, floored at ``min_interval_seconds``
    and capped at ``max_interval_seconds``), so serialization is bounded to a fraction of
    wall-clock time no matter how large the state grows. Timing from save *start* would
    mean that once a save outlasts the interval, every mutation retriggers a save
    back-to-back and the migration spends all its time serializing. Callers never block:
    if a save is already in flight on another thread, the call returns immediately.

    Debouncing bounds the SIGKILL data-loss window to one interval plus one save of
    mappings; the cap keeps that window bounded even when a single save takes minutes
    (past it, persistence knowingly exceeds ~1/interval_scale of wall-clock). All other
    exits still save unconditionally (signal flush, finally).
    """

    def __init__(
        self,
        save: Callable[[], None],
        min_interval_seconds: float = 1.0,
        time_fn: Callable[[], float] = time.monotonic,
        interval_scale: float = 9.0,
        max_interval_seconds: float = 60.0,
    ) -> None:
        self._save = save
        self._min_interval_seconds = min_interval_seconds
        self._max_interval_seconds = max_interval_seconds
        self._current_interval_seconds = min_interval_seconds
        self._time_fn = time_fn
        self._interval_scale = interval_scale
        self._lock = threading.Lock()
        self._last_save = float("-inf")

    def __call__(self) -> None:
        if not self._lock.acquire(blocking=False):
            # A save is already in flight on another thread; its post-save timestamp
            # covers this mutation's debounce window.
            return
        try:
            started = self._time_fn()
            if started - self._last_save < self._current_interval_seconds:
                return
            try:
                self._save()
            finally:
                # Debounce failed saves too — otherwise a persistently failing save
                # (ENOSPC, ...) retries a full O(state size) serialization on every
                # subsequent mutation, the exact pathology debouncing exists to prevent.
                finished = self._time_fn()
                self._last_save = finished
                # interval_scale=9 caps serialization at ~10% of wall-clock time.
                scaled_interval = self._interval_scale * (finished - started)
                self._current_interval_seconds = min(
                    max(self._min_interval_seconds, scaled_interval), self._max_interval_seconds
                )
        finally:
            self._lock.release()


@contextmanager
def _flush_state_on_termination(runner: MigrationRunner) -> Iterator[None]:
    """Save migration state immediately on SIGINT/SIGTERM before the process dies.

    CI cancellation (e.g. GitHub Actions) sends SIGINT and hard-kills the process a few
    seconds later — too short for in-flight copies to finish and reach a normal save. The
    handler persists whatever has been recorded so far, then restores the original handler
    and re-raises the signal so exit semantics are unchanged. No-op outside the main thread
    (signal handlers can only be installed there).
    """
    if threading.current_thread() is not threading.main_thread():
        yield
        return

    originals: dict[int, object] = {}

    def _handler(signum: int, frame: FrameType | None) -> None:
        logger.warning(
            "Received signal %d — saving migration state to %s before exiting", signum, runner.migration_state_path
        )
        runner.save_state()
        signal.signal(signum, originals[signum])  # type: ignore[arg-type]
        signal.raise_signal(signum)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            originals[sig] = signal.signal(sig, _handler)
        except (ValueError, OSError):  # pragma: no cover - non-main thread / unsupported platform
            pass
    try:
        yield
    finally:
        for sig_num, original in originals.items():
            try:
                signal.signal(sig_num, original)  # type: ignore[arg-type]
            except (ValueError, OSError):  # pragma: no cover
                pass


def run_parallel_migration(runner: MigrationRunner, max_workers: int) -> None:
    """Run resource migration with a shared thread pool.

    Migration state is persisted incrementally (debounced) as tasks settle and mappings are
    recorded, flushed by a SIGINT/SIGTERM handler, and saved one final time in a `finally`
    that is reachable even while copies are still in flight: on interruption the executor
    is shut down without waiting (queued tasks cancelled), so the last save happens
    immediately instead of blocking behind in-flight work until the process is hard-killed.
    """
    max_workers = validate_max_workers(max_workers)
    thread_safe_state = ThreadSafeMigrationState.from_state(runner.migration_state)
    # Persist child-resource mappings (runs, dataset files, workbooks, ...) as they are
    # recorded mid-asset — per-task saves alone would lose everything inside a long-running
    # asset on a hard kill. Debounced; dry runs skip the write inside save_state itself.
    debounced_save = _DebouncedSave(runner.save_state)
    thread_safe_state.set_persist_hook(debounced_save)
    runner.migration_state = thread_safe_state

    ctx = MigrationContext(
        destination_client=runner.destination_client,
        migration_state=runner.migration_state,
        source_asset_rids=frozenset(runner.migration_resources.source_assets.keys()),
        dry_run=runner.dry_run,
        video_ingest_timeout=runner.video_ingest_timeout,
    )
    if getattr(runner, "destination_client_resolver", None) is not None:
        setattr(ctx, "destination_client_resolver", runner.destination_client_resolver)
    asset_migrator = AssetMigrator(ctx)
    template_migrator = WorkbookTemplateMigrator(ctx)
    checklist_migrator = ChecklistMigrator(ctx)
    asset_tasks = [
        MigrationTask(
            rid=rid,
            label="asset",
            fn=_make_asset_fn(
                asset_resources,
                asset_migrator,
                AssetCopyOptions(
                    dataset_config=runner.dataset_config,
                    include_attachments=runner.asset_inclusion_config.include_attachments,
                    include_events=runner.asset_inclusion_config.include_events,
                    include_runs=runner.asset_inclusion_config.include_runs,
                    include_video=runner.asset_inclusion_config.include_video,
                    include_checklists=runner.asset_inclusion_config.include_checklists,
                    include_workbooks=runner.asset_inclusion_config.include_workbooks,
                    workbook_rids_allowlist=asset_resources.source_workbook_rids,
                ),
            ),
        )
        for rid, asset_resources in runner.migration_resources.source_assets.items()
    ]
    template_tasks = [
        MigrationTask(
            rid=template.rid,
            label="template",
            fn=_make_template_fn(template, template_migrator),
        )
        for template in runner.migration_resources.source_standalone_templates
    ]
    checklist_tasks = [
        MigrationTask(
            rid=checklist.rid,
            label="checklist",
            fn=_make_checklist_fn(checklist, checklist_migrator),
        )
        for checklist in runner.migration_resources.source_standalone_checklists
    ]
    tasks = asset_tasks + template_tasks + checklist_tasks

    logger.info(
        "Running migration with %d worker(s) across %d asset(s), %d template(s), and %d checklist(s)",
        max_workers,
        len(asset_tasks),
        len(template_tasks),
        len(checklist_tasks),
    )
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    try:
        with _flush_state_on_termination(runner):
            # State is saved (debounced) as tasks settle so a killed run resumes from
            # recent progress instead of losing everything. Unconditional per-task saves
            # would serialize the full O(state size) tree once per task — quadratic when
            # many small template/checklist tasks settle in a burst.
            run_concurrent(executor, tasks, on_task_complete=debounced_save)
        executor.shutdown(wait=True)
    except BaseException:
        # KeyboardInterrupt/SystemExit included: don't block the unwind behind in-flight
        # copies — cancel queued tasks, leave running ones behind, and reach the final
        # save below immediately (the process may be hard-killed seconds later).
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    finally:
        runner.save_state()
        log_skipped_resources(runner.migration_state)

    logger.info("Completed parallel migration")
