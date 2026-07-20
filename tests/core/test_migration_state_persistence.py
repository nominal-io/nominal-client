"""Tests for crash-safe migration state persistence (incremental saves + signal flush)."""

from __future__ import annotations

import concurrent.futures
import signal
import sys
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.experimental.migration.config.migration_data_config import MigrationDatasetConfig
from nominal.experimental.migration.config.migration_resources import MigrationResources
from nominal.experimental.migration.migration_runner import MigrationRunner
from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.parallel_migration_executor import MigrationTask, run_concurrent
from nominal.experimental.migration.parallel_migration_runner import _DebouncedSave, _flush_state_on_termination
from nominal.experimental.migration.parallel_migration_state import ThreadSafeMigrationState
from nominal.experimental.migration.resource_type import ResourceType


def _make_runner(tmp_path: Path, state_name: str = "state.json") -> MigrationRunner:
    return MigrationRunner(
        migration_resources=MigrationResources(source_assets={}, source_standalone_templates=[]),
        dataset_config=MigrationDatasetConfig(include_dataset_files=False, preserve_dataset_uuid=True),
        destination_client=MagicMock(),
        migration_state_path=tmp_path / state_name,
    )


class TestRunConcurrentCallback:
    def test_callback_invoked_after_every_task(self) -> None:
        """State must be persisted after each settled task, so the callback fires once per task."""
        calls: list[str] = []
        tasks = [MigrationTask(rid=f"rid-{i}", label="asset", fn=lambda: None) for i in range(3)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            run_concurrent(executor, tasks, on_task_complete=lambda: calls.append("save"))
        assert calls == ["save"] * 3

    def test_callback_invoked_for_failed_tasks_too(self) -> None:
        """A failing task must still trigger a save — earlier progress inside it may be recorded."""
        calls: list[str] = []

        def boom() -> None:
            raise RuntimeError("boom")

        tasks = [
            MigrationTask(rid="ok", label="asset", fn=lambda: None),
            MigrationTask(rid="bad", label="asset", fn=boom),
        ]
        with pytest.raises(RuntimeError, match="1 failure"):
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                run_concurrent(executor, tasks, on_task_complete=lambda: calls.append("save"))
        assert calls == ["save"] * 2

    def test_callback_optional(self) -> None:
        """Omitting on_task_complete must not change task execution."""
        ran: list[str] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            run_concurrent(executor, [MigrationTask(rid="r", label="asset", fn=lambda: ran.append("r"))])
        assert ran == ["r"]


class TestSaveStateAtomicity:
    def test_save_writes_valid_resumable_json_and_no_tmp_residue(self, tmp_path: Path) -> None:
        """The atomic write must leave a loadable state file and clean up its temp file."""
        runner = _make_runner(tmp_path)
        runner.migration_state.record_mapping(ResourceType.ASSET, "old", "new")
        runner.save_state()
        state_file = tmp_path / "state.json"
        assert state_file.exists()
        assert not list(tmp_path.glob("*.tmp"))
        restored = MigrationState.from_json(state_file.read_text(encoding="utf-8"))
        assert restored.get_mapped_rid(ResourceType.ASSET, "old") == "new"

    def test_repeated_saves_overwrite(self, tmp_path: Path) -> None:
        """Incremental saving calls save_state many times; each write must land completely."""
        runner = _make_runner(tmp_path)
        for i in range(5):
            runner.migration_state.record_mapping(ResourceType.RUN, f"old-{i}", f"new-{i}")
            runner.save_state()
        restored = MigrationState.from_json((tmp_path / "state.json").read_text(encoding="utf-8"))
        assert len(restored.rid_mapping[ResourceType.RUN.value]) == 5


class TestSignalFlush:
    def test_sigint_saves_state_before_propagating(self, tmp_path: Path) -> None:
        """Cancellation (SIGINT) must persist recorded state before the process unwinds."""
        runner = _make_runner(tmp_path)
        runner.migration_state.record_mapping(ResourceType.ASSET, "old", "new")
        with pytest.raises(KeyboardInterrupt):
            with _flush_state_on_termination(runner):
                signal.raise_signal(signal.SIGINT)
        state_file = tmp_path / "state.json"
        assert state_file.exists()
        restored = MigrationState.from_json(state_file.read_text(encoding="utf-8"))
        assert restored.get_mapped_rid(ResourceType.ASSET, "old") == "new"

    def test_handlers_restored_after_context(self, tmp_path: Path) -> None:
        """Leaving the flush context must restore whatever handlers were installed before it."""
        runner = _make_runner(tmp_path)
        before_int = signal.getsignal(signal.SIGINT)
        before_term = signal.getsignal(signal.SIGTERM)
        with _flush_state_on_termination(runner):
            assert signal.getsignal(signal.SIGINT) is not before_int
        assert signal.getsignal(signal.SIGINT) is before_int
        assert signal.getsignal(signal.SIGTERM) is before_term

    def test_no_save_when_no_signal(self, tmp_path: Path) -> None:
        """The flush context itself must not write state — only a signal triggers it."""
        runner = _make_runner(tmp_path)
        with _flush_state_on_termination(runner):
            pass
        assert not (tmp_path / "state.json").exists()


class TestInterruptReachesFinalSave:
    def test_interrupt_saves_state_without_waiting_for_in_flight_tasks(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """On KeyboardInterrupt the final save must happen immediately, not block behind
        an in-flight copy until the process is hard-killed.
        """
        from nominal.experimental.migration import parallel_migration_runner

        runner = _make_runner(tmp_path)
        release_worker = threading.Event()

        def fake_run_concurrent(
            executor: concurrent.futures.ThreadPoolExecutor,
            tasks: object,
            on_task_complete: object = None,
        ) -> None:
            executor.submit(release_worker.wait)
            runner.migration_state.record_mapping(ResourceType.ASSET, "old", "new")
            raise KeyboardInterrupt

        monkeypatch.setattr(parallel_migration_runner, "run_concurrent", fake_run_concurrent)
        try:
            start = time.monotonic()
            with pytest.raises(KeyboardInterrupt):
                parallel_migration_runner.run_parallel_migration(runner, max_workers=1)
            elapsed = time.monotonic() - start
            assert elapsed < 5, "unwind must not block on the in-flight (Event-gated) task"
            state_file = tmp_path / "state.json"
            assert state_file.exists()
            restored = MigrationState.from_json(state_file.read_text(encoding="utf-8"))
            assert restored.get_mapped_rid(ResourceType.ASSET, "old") == "new"
        finally:
            release_worker.set()


class TestPersistHook:
    def test_every_mutation_triggers_the_hook(self) -> None:
        """Child-resource mappings recorded mid-asset must reach the hook, not just task ends."""
        saves: list[str] = []
        state = ThreadSafeMigrationState()
        state.set_persist_hook(lambda: saves.append("save"))
        state.record_mapping(ResourceType.DATASET_FILE, "old", "new")
        state.record_pending_multi_asset_workbook("wb", ["a1"])
        state.clear_pending_multi_asset_workbook("wb")
        state.record_pending_multi_run_workbook("wb", ["r1"])
        state.clear_pending_multi_run_workbook("wb")
        state.record_skip(ResourceType.WORKBOOK, "wb2", "out of scope")
        assert len(saves) == 6

    def test_compound_workbook_queue_mutations_trigger_the_hook(self) -> None:
        """Atomic workbook queue/skip helpers must persist the same way as primitive mutators."""
        saves: list[str] = []
        state = ThreadSafeMigrationState()
        state.set_persist_hook(lambda: saves.append("save"))

        assert state.record_pending_multi_asset_workbook_unless_skipped("wb", ["a1"]) is True
        assert state.record_pending_multi_run_workbook_unless_skipped("wb", ["r1"]) is True
        assert state.record_workbook_skip_and_clear_pending("wb", "out of scope") is True
        assert state.record_pending_multi_asset_workbook_unless_skipped("wb", ["a1"]) is False

        assert saves == ["save", "save", "save"]

    def test_reads_do_not_trigger_the_hook(self) -> None:
        """Only mutations persist — lookups happen constantly and must stay write-free."""
        saves: list[str] = []
        state = ThreadSafeMigrationState()
        state.set_persist_hook(lambda: saves.append("save"))
        state.get_mapped_rid(ResourceType.ASSET, "missing")
        state.to_json()
        assert saves == []

    def test_hook_may_serialize_state(self, tmp_path: Path) -> None:
        """The hook calls save_state -> to_json, which re-takes the state lock — must not deadlock."""
        runner = _make_runner(tmp_path)
        state = ThreadSafeMigrationState()
        runner.migration_state = state
        state.set_persist_hook(runner.save_state)
        state.record_mapping(ResourceType.RUN, "old", "new")
        restored = MigrationState.from_json((tmp_path / "state.json").read_text(encoding="utf-8"))
        assert restored.get_mapped_rid(ResourceType.RUN, "old") == "new"


class TestDebouncedSave:
    def test_rapid_calls_collapse(self) -> None:
        """Per-mapping saves are O(state size); rapid mutations must not each hit the disk."""
        saves: list[str] = []
        clock = [0.0]
        debounced = _DebouncedSave(lambda: saves.append("save"), min_interval_seconds=1.0, time_fn=lambda: clock[0])
        debounced()
        debounced()
        clock[0] = 0.5
        debounced()
        assert len(saves) == 1

    def test_saves_again_after_interval(self) -> None:
        """Once the interval elapses the next mutation must persist promptly."""
        saves: list[str] = []
        clock = [0.0]
        debounced = _DebouncedSave(lambda: saves.append("save"), min_interval_seconds=1.0, time_fn=lambda: clock[0])
        debounced()
        clock[0] = 1.5
        debounced()
        assert len(saves) == 2


class TestThreadSafeToJson:
    def test_to_json_matches_plain_state(self) -> None:
        """Taking the lock during serialization must not change the serialized output."""
        plain = MigrationState()
        safe = ThreadSafeMigrationState()
        for state in (plain, safe):
            state.record_mapping(ResourceType.ASSET, "a", "b")
        assert safe.to_json() == plain.to_json()

    def test_to_json_is_reentrant_on_same_thread(self) -> None:
        """The signal flush handler may fire while the main thread already holds the lock
        (mid incremental save); serialization must not deadlock in that case.
        """
        safe = ThreadSafeMigrationState()
        safe.record_mapping(ResourceType.ASSET, "a", "b")
        with safe._lock:
            assert "rid_mapping" in safe.to_json()
