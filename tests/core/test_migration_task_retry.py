"""Task-level transient retry in the parallel migration executor.

A production migration lost two whole asset tasks to a connection reset on a single workbook
read — a call outside every per-operation retry. The executor is the safety net: transient
task failures resubmit (copies resume from migration state), everything else still fails.
"""

from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

import urllib3.exceptions

from nominal.experimental.migration.parallel_migration_executor import TASK_ATTEMPTS, MigrationTask, run_concurrent


def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("nominal.experimental.migration.parallel_migration_executor.time.sleep", lambda _seconds: None)


def _reset() -> urllib3.exceptions.ProtocolError:
    return urllib3.exceptions.ProtocolError("Connection broken: ConnectionResetError(104, 'Connection reset by peer')")


def test_transient_task_failure_is_retried_to_success(monkeypatch: pytest.MonkeyPatch) -> None:
    _no_sleep(monkeypatch)
    calls = {"n": 0}

    def flaky() -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise _reset()

    with ThreadPoolExecutor(max_workers=2) as executor:
        run_concurrent(executor, [MigrationTask(rid="rid-1", label="asset", fn=flaky)])

    assert calls["n"] == 2


def test_transient_task_failure_exhausts_budget_then_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _no_sleep(monkeypatch)
    calls = {"n": 0}

    def always_reset() -> None:
        calls["n"] += 1
        raise _reset()

    with ThreadPoolExecutor(max_workers=2) as executor:
        with pytest.raises(RuntimeError, match="1 failure"):
            run_concurrent(executor, [MigrationTask(rid="rid-1", label="asset", fn=always_reset)])

    assert calls["n"] == TASK_ATTEMPTS


def test_non_transient_task_failure_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    _no_sleep(monkeypatch)
    calls = {"n": 0}

    def broken() -> None:
        calls["n"] += 1
        raise ValueError("permanent")

    with ThreadPoolExecutor(max_workers=2) as executor:
        with pytest.raises(RuntimeError, match="1 failure"):
            run_concurrent(executor, [MigrationTask(rid="rid-1", label="asset", fn=broken)])

    assert calls["n"] == 1


def test_retries_do_not_block_other_tasks_and_all_failures_are_collected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One retrying task and one permanent failure: the healthy task completes, the permanent
    one is reported once, and the summary counts each task's terminal outcome exactly once.
    """
    _no_sleep(monkeypatch)
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
            )

    assert calls == {"flaky": TASK_ATTEMPTS, "ok": 1, "broken": 1}
    # Every attempt settles the persist hook: flaky's retries included.
    assert settled["n"] == TASK_ATTEMPTS + 2


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
    migrator, workbook_migrator, workbook = _asset_migrator_fixture()
    migrator.ctx.migration_state.set_skip(ResourceType.WORKBOOK, _WORKBOOK_RID, "copy failed: transient")

    migrator._copy_workbook_containing_failures(workbook_migrator, workbook, WorkbookCopyOptions())

    assert migrator.ctx.migration_state.skipped_resources == []
