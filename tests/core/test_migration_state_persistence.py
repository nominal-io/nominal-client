"""Tests for crash-safe migration state persistence (incremental saves + signal flush)."""

from __future__ import annotations

import concurrent.futures
import signal
import sys
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
from nominal.experimental.migration.parallel_migration_runner import _flush_state_on_termination
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
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            run_concurrent(executor, [MigrationTask(rid="r", label="asset", fn=lambda: None)])


class TestSaveStateAtomicity:
    def test_save_writes_valid_resumable_json_and_no_tmp_residue(self, tmp_path: Path) -> None:
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
        runner = _make_runner(tmp_path)
        before_int = signal.getsignal(signal.SIGINT)
        before_term = signal.getsignal(signal.SIGTERM)
        with _flush_state_on_termination(runner):
            assert signal.getsignal(signal.SIGINT) is not before_int
        assert signal.getsignal(signal.SIGINT) is before_int
        assert signal.getsignal(signal.SIGTERM) is before_term

    def test_no_save_when_no_signal(self, tmp_path: Path) -> None:
        runner = _make_runner(tmp_path)
        with _flush_state_on_termination(runner):
            pass
        assert not (tmp_path / "state.json").exists()


class TestThreadSafeToJson:
    def test_to_json_matches_plain_state(self) -> None:
        plain = MigrationState()
        safe = ThreadSafeMigrationState()
        for state in (plain, safe):
            state.record_mapping(ResourceType.ASSET, "a", "b")
        assert safe.to_json() == plain.to_json()
