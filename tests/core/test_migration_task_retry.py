"""Task-level transient retry in the parallel migration executor.

A production migration lost two whole asset tasks to a connection reset on a single workbook
read — a call outside every per-operation retry. The executor is the safety net: transient
task failures retry in place (copies resume from migration state), everything else still fails.
"""

from __future__ import annotations

import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

import urllib3.exceptions

from nominal.experimental.migration.parallel_migration_executor import TASK_ATTEMPTS, MigrationTask, run_concurrent

_NO_SLEEP = lambda _delay: None  # noqa: E731


def _reset() -> urllib3.exceptions.ProtocolError:
    return urllib3.exceptions.ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')")


def test_transient_task_failure_is_retried_to_success() -> None:
    """A transient failure retries the task, and the second attempt's success settles it cleanly."""
    calls = {"n": 0}

    def flaky() -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise _reset()

    with ThreadPoolExecutor(max_workers=2) as executor:
        run_concurrent(executor, [MigrationTask(rid="rid-1", label="asset", fn=flaky)], sleep=_NO_SLEEP)

    assert calls["n"] == 2


def test_transient_task_failure_exhausts_budget_then_raises() -> None:
    """A transient failure that outlasts the whole budget surfaces as a task failure."""
    calls = {"n": 0}

    def always_reset() -> None:
        calls["n"] += 1
        raise _reset()

    with ThreadPoolExecutor(max_workers=2) as executor:
        with pytest.raises(RuntimeError, match="1 failure"):
            run_concurrent(executor, [MigrationTask(rid="rid-1", label="asset", fn=always_reset)], sleep=_NO_SLEEP)

    assert calls["n"] == TASK_ATTEMPTS


def test_non_transient_task_failure_is_not_retried() -> None:
    """A permanent failure spends exactly one attempt — retrying cannot help."""
    calls = {"n": 0}

    def broken() -> None:
        calls["n"] += 1
        raise ValueError("permanent")

    with ThreadPoolExecutor(max_workers=2) as executor:
        with pytest.raises(RuntimeError, match="1 failure"):
            run_concurrent(executor, [MigrationTask(rid="rid-1", label="asset", fn=broken)], sleep=_NO_SLEEP)

    assert calls["n"] == 1


def test_retrying_task_does_not_block_other_tasks() -> None:
    """While one task is parked in its retry backoff, other tasks keep completing.

    The backoff sleep is gated on the healthy task's completion event: if a retry blocked the
    pool, the healthy task could never finish and the gate would time out the test.
    """
    healthy_done = threading.Event()
    calls = {"flaky": 0, "ok": 0}

    def gated_sleep(_delay: float) -> None:
        assert healthy_done.wait(timeout=10), "healthy task did not complete while the retry was parked"

    def flaky() -> None:
        calls["flaky"] += 1
        if calls["flaky"] == 1:
            raise _reset()

    def ok() -> None:
        calls["ok"] += 1
        healthy_done.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        run_concurrent(
            executor,
            [
                MigrationTask(rid="rid-flaky", label="asset", fn=flaky),
                MigrationTask(rid="rid-ok", label="asset", fn=ok),
            ],
            sleep=gated_sleep,
        )

    assert calls == {"flaky": 2, "ok": 1}


def test_all_terminal_failures_are_collected_and_hook_fires_per_task() -> None:
    """Mixed outcomes: the retried task recovers, the permanent one is reported exactly once,
    and the persist hook fires once per settled task.
    """
    calls = {"flaky": 0, "ok": 0, "broken": 0}
    settled = {"n": 0}

    def flaky() -> None:
        calls["flaky"] += 1
        if calls["flaky"] < TASK_ATTEMPTS:
            raise _reset()

    def ok() -> None:
        calls["ok"] += 1

    def broken() -> None:
        calls["broken"] += 1
        raise ValueError("permanent")

    with ThreadPoolExecutor(max_workers=2) as executor:
        with pytest.raises(RuntimeError, match="1 failure"):
            run_concurrent(
                executor,
                [
                    MigrationTask(rid="rid-flaky", label="asset", fn=flaky),
                    MigrationTask(rid="rid-ok", label="asset", fn=ok),
                    MigrationTask(rid="rid-broken", label="asset", fn=broken),
                ],
                on_task_complete=lambda: settled.__setitem__("n", settled["n"] + 1),
                sleep=_NO_SLEEP,
            )

    assert calls == {"flaky": TASK_ATTEMPTS, "ok": 1, "broken": 1}
    assert settled["n"] == 3


# ---------------------------------------------------------------------------
# Workbook failure containment inside an asset task
# ---------------------------------------------------------------------------

from unittest.mock import MagicMock  # noqa: E402

from nominal.experimental.migration.migration_state import MigrationState  # noqa: E402
from nominal.experimental.migration.migrator.asset_migrator import AssetMigrator  # noqa: E402
from nominal.experimental.migration.migrator.context import MigrationContext  # noqa: E402
from nominal.experimental.migration.migrator.workbook_migrator import WorkbookCopyOptions  # noqa: E402
from nominal.experimental.migration.resource_type import ResourceType  # noqa: E402

_WORKBOOK_RID = "ri.scout.cerulean-staging.notebook.00000001-0000-0000-0000-000000000000"


def _asset_migrator_fixture() -> tuple[AssetMigrator, MagicMock, MagicMock]:
    ctx = MigrationContext(destination_client=MagicMock(), migration_state=MigrationState())
    workbook = MagicMock()
    workbook.rid = _WORKBOOK_RID
    return AssetMigrator(ctx), MagicMock(), workbook


def test_non_transient_workbook_failure_is_contained_as_skip() -> None:
    """A broken workbook records a skip and the asset task lives on; a rerun re-attempts it."""
    migrator, workbook_migrator, workbook = _asset_migrator_fixture()
    workbook_migrator.copy_from.side_effect = ValueError("corrupt workbook content")

    migrator._copy_workbook_containing_failures(workbook_migrator, workbook, WorkbookCopyOptions())

    assert migrator.ctx.migration_state.get_mapped_rid(ResourceType.WORKBOOK, _WORKBOOK_RID) is None
    assert [skip.reason for skip in migrator.ctx.migration_state.skipped_resources] == [
        "copy failed: corrupt workbook content"
    ]


def test_transient_workbook_failure_propagates_for_task_retry() -> None:
    """A connection reset must reach the executor, whose task-level retry resumes from state —
    converting it to a skip here would leave a healthy workbook behind for no reason.
    """
    migrator, workbook_migrator, workbook = _asset_migrator_fixture()
    workbook_migrator.copy_from.side_effect = _reset()

    with pytest.raises(urllib3.exceptions.ProtocolError):
        migrator._copy_workbook_containing_failures(workbook_migrator, workbook, WorkbookCopyOptions())

    assert migrator.ctx.migration_state.skipped_resources == []


def test_workbook_success_clears_stale_copy_failure_skip() -> None:
    """A workbook that failed transiently in an earlier run stops being reported once it copies."""
    migrator, workbook_migrator, workbook = _asset_migrator_fixture()
    migrator.ctx.migration_state.set_skip(ResourceType.WORKBOOK, _WORKBOOK_RID, "copy failed: transient")

    migrator._copy_workbook_containing_failures(workbook_migrator, workbook, WorkbookCopyOptions())

    assert migrator.ctx.migration_state.skipped_resources == []
