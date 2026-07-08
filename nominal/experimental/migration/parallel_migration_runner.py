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
from nominal.experimental.migration.migration_runner import MigrationRunner
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
    Debouncing bounds the SIGKILL data-loss window to ``min_interval_seconds`` of mappings;
    all other exits still save unconditionally (per-task callback, signal flush, finally).
    """

    def __init__(
        self,
        save: Callable[[], None],
        min_interval_seconds: float = 1.0,
        time_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self._save = save
        self._min_interval_seconds = min_interval_seconds
        self._time_fn = time_fn
        self._lock = threading.Lock()
        self._last_save = float("-inf")

    def __call__(self) -> None:
        with self._lock:
            now = self._time_fn()
            if now - self._last_save < self._min_interval_seconds:
                return
            self._last_save = now
        self._save()


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

    Migration state is persisted after every settled task, flushed by a SIGINT/SIGTERM
    handler, and saved one final time in a `finally` that is reachable even while copies
    are still in flight: on interruption the executor is shut down without waiting
    (queued tasks cancelled), so the last save happens immediately instead of blocking
    behind in-flight work until the process is hard-killed.
    """
    max_workers = validate_max_workers(max_workers)
    thread_safe_state = ThreadSafeMigrationState(rid_mapping=runner.migration_state.rid_mapping)
    # Persist child-resource mappings (runs, dataset files, workbooks, ...) as they are
    # recorded mid-asset — per-task saves alone would lose everything inside a long-running
    # asset on a hard kill. Debounced; dry runs skip the write inside save_state itself.
    thread_safe_state.set_persist_hook(_DebouncedSave(runner.save_state))
    runner.migration_state = thread_safe_state

    ctx = MigrationContext(
        destination_client=runner.destination_client,
        migration_state=runner.migration_state,
        source_asset_rids=frozenset(runner.migration_resources.source_assets.keys()),
        dry_run=runner.dry_run,
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
            # State is saved after every settled task so a killed run resumes from the
            # last completed resource instead of losing everything.
            run_concurrent(executor, tasks, on_task_complete=runner.save_state)
        executor.shutdown(wait=True)
    except BaseException:
        # KeyboardInterrupt/SystemExit included: don't block the unwind behind in-flight
        # copies — cancel queued tasks, leave running ones behind, and reach the final
        # save below immediately (the process may be hard-killed seconds later).
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    finally:
        runner.save_state()

    logger.info("Completed parallel migration")
