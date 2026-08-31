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
