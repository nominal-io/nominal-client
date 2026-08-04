"""Tests for crash-safe migration state persistence (incremental saves + signal flush)."""

from __future__ import annotations

import concurrent.futures
import dataclasses
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


class TestSaveOrdering:
    def test_stale_save_cannot_clobber_newer_save(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """An in-flight save holding an older snapshot must not overwrite a newer save's
        file — e.g. a debounced worker save racing the SIGINT/SIGTERM flush. save_state
        serializes snapshot -> write -> replace, so replaces land in snapshot order.
        """
        runner = _make_runner(tmp_path)
        state = ThreadSafeMigrationState()
        runner.migration_state = state
        state.record_mapping(ResourceType.ASSET, "old", "new")

        snapshot_taken = threading.Event()
        release_stale_save = threading.Event()
        stale = [True]
        original_to_json = ThreadSafeMigrationState.to_json

        def gated_to_json(self: ThreadSafeMigrationState) -> str:
            serialized = original_to_json(self)
            if stale[0]:
                stale[0] = False
                snapshot_taken.set()
                # Hold the now-stale snapshot while newer state is recorded and saved.
                release_stale_save.wait(timeout=10)
            return serialized

        monkeypatch.setattr(ThreadSafeMigrationState, "to_json", gated_to_json)
        stale_save = threading.Thread(target=runner.save_state)
        stale_save.start()
        try:
            assert snapshot_taken.wait(timeout=10)
            state.record_mapping(ResourceType.RUN, "newer", "mapping")
            newer_save = threading.Thread(target=runner.save_state)
            newer_save.start()
            time.sleep(0.1)  # give the newer save a head start it must not be able to use
        finally:
            release_stale_save.set()
        stale_save.join(timeout=10)
        newer_save.join(timeout=10)

        restored = MigrationState.from_json((tmp_path / "state.json").read_text(encoding="utf-8"))
        assert restored.get_mapped_rid(ResourceType.RUN, "newer") == "mapping", (
            "a stale in-flight save overwrote a newer save's file"
        )


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

    def test_interval_measured_from_save_completion(self) -> None:
        """A save slower than the interval must not make the very next mutation save again.

        Timing from save start meant that once serialization outlasted the interval, every
        mutation retriggered a save and the migration serialized back-to-back forever.
        """
        saves: list[str] = []
        clock = [0.0]

        def slow_save() -> None:
            saves.append("save")
            clock[0] += 5.0  # save takes 5s, well past the 1s interval

        debounced = _DebouncedSave(slow_save, min_interval_seconds=1.0, time_fn=lambda: clock[0], interval_scale=1.0)
        debounced()  # finishes at t=5
        clock[0] = 5.5
        debounced()  # 0.5s after completion: must be debounced despite 5.5s since start
        assert len(saves) == 1
        clock[0] = 10.5
        debounced()
        assert len(saves) == 2

    def test_interval_scales_with_save_duration(self) -> None:
        """The interval must grow with save cost so serialization stays a bounded fraction
        of wall-clock time as the state file grows.
        """
        saves: list[str] = []
        clock = [0.0]

        def slow_save() -> None:
            saves.append("save")
            clock[0] += 2.0

        debounced = _DebouncedSave(slow_save, min_interval_seconds=1.0, time_fn=lambda: clock[0], interval_scale=9.0)
        debounced()  # finishes at t=2 -> next interval is 18s
        clock[0] = 15.0
        debounced()
        assert len(saves) == 1
        clock[0] = 20.5  # 18.5s after completion
        debounced()
        assert len(saves) == 2

    def test_failed_save_is_debounced_like_a_successful_one(self) -> None:
        """A raising save must not retry on every subsequent mutation — that is the same
        back-to-back-serialization pathology debouncing exists to prevent, triggered by a
        failing save (ENOSPC, ...) instead of a slow one.
        """
        attempts: list[str] = []
        clock = [0.0]

        def failing_save() -> None:
            attempts.append("save")
            raise OSError("disk full")

        debounced = _DebouncedSave(failing_save, min_interval_seconds=1.0, time_fn=lambda: clock[0])
        with pytest.raises(OSError, match="disk full"):
            debounced()
        clock[0] = 0.5
        debounced()  # within the interval: must not retry the failing save
        assert len(attempts) == 1
        clock[0] = 1.5
        with pytest.raises(OSError, match="disk full"):
            debounced()  # after the interval: retried
        assert len(attempts) == 2

    def test_interval_is_capped(self) -> None:
        """The scaled interval must not grow without bound — a multi-minute save would
        otherwise stretch the SIGKILL data-loss window to interval_scale times that.
        """
        saves: list[str] = []
        clock = [0.0]

        def very_slow_save() -> None:
            saves.append("save")
            clock[0] += 20.0

        debounced = _DebouncedSave(
            very_slow_save,
            min_interval_seconds=1.0,
            time_fn=lambda: clock[0],
            interval_scale=9.0,
            max_interval_seconds=60.0,
        )
        debounced()  # finishes at t=20; unclamped interval would be 180s
        clock[0] = 20.0 + 30.0
        debounced()  # below the 60s cap: still debounced
        assert len(saves) == 1
        clock[0] = 20.0 + 61.0
        debounced()  # past the cap: must save even though 9 x 20s has not elapsed
        assert len(saves) == 2

    def test_calls_during_in_flight_save_return_without_blocking(self) -> None:
        """Worker threads must never queue behind an in-flight save — they record their
        mapping and move on; the next post-interval mutation persists it.
        """
        save_started = threading.Event()
        release_save = threading.Event()
        saves: list[str] = []

        def blocking_save() -> None:
            saves.append("save")
            save_started.set()
            release_save.wait(timeout=10)

        debounced = _DebouncedSave(blocking_save, min_interval_seconds=0.0)
        first = threading.Thread(target=debounced)
        first.start()
        try:
            assert save_started.wait(timeout=10)
            start = time.monotonic()
            debounced()  # must return immediately, not wait for the in-flight save
            assert time.monotonic() - start < 1.0
            assert saves == ["save"]
        finally:
            release_save.set()
            first.join()


class TestParallelSaveThroughput:
    def test_large_state_saves_do_not_stall_parallel_workers(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """End-to-end regression test for incremental saves starving parallel workers.

        Runs the real parallel runner — ThreadSafeMigrationState, debounced persist hook,
        atomic save_state to disk, thread pool — with only the network-touching copy_from
        mocked, against a state pre-seeded large enough (~250 MB of JSON) that one full
        serialization takes ~0.7s.

        The regression metric is the worst-case latency of a single state *read* from a
        worker thread. Serializing under the state lock blocked every worker's
        get_mapped_rid/record_mapping for the full serialization (~0.7s here, tens of
        seconds at real-migration state sizes, back-to-back due to the debounce timing
        flaw); with the snapshot fix, workers only ever wait on the C-level snapshot copy
        (~0.02s). Reads are timed rather than writes because a write may legitimately run
        the debounced save synchronously on its own thread — the fix's contract is that
        one thread saving never stalls the *other* workers.
        """
        from nominal.experimental.migration import parallel_migration_runner
        from nominal.experimental.migration.config.migration_resources import AssetResources, MigrationResources

        num_assets = 4
        mappings_per_asset = 50
        runner = MigrationRunner(
            migration_resources=MigrationResources(
                source_assets={
                    f"ri.scout.asset.{i}": AssetResources(
                        asset=MagicMock(rid=f"ri.scout.asset.{i}"), source_workbook_templates=[]
                    )
                    for i in range(num_assets)
                },
                source_standalone_templates=[],
            ),
            dataset_config=MigrationDatasetConfig(include_dataset_files=False, preserve_dataset_uuid=True),
            destination_client=MagicMock(),
            migration_state_path=tmp_path / "state.json",
        )
        runner.migration_state = MigrationState(
            rid_mapping={
                ResourceType.DATASET_FILE.value: {
                    f"ri.catalog.file.{i:040d}": f"ri.catalog.file.dest.{i:040d}" for i in range(2_000_000)
                }
            }
        )
        worst_read_seconds = [0.0]
        worst_read_lock = threading.Lock()

        def fake_copy_from(self: object, asset: MagicMock, options: object) -> None:
            # Each worker alternates reads and writes with small gaps, like real
            # per-resource migration work between API calls.
            for i in range(mappings_per_asset):
                time.sleep(0.005)
                read_start = time.monotonic()
                runner.migration_state.get_mapped_rid(ResourceType.RUN, f"{asset.rid}-run-{i}")
                read_elapsed = time.monotonic() - read_start
                with worst_read_lock:
                    worst_read_seconds[0] = max(worst_read_seconds[0], read_elapsed)
                runner.migration_state.record_mapping(ResourceType.RUN, f"{asset.rid}-run-{i}", f"new-{asset.rid}-{i}")

        monkeypatch.setattr(parallel_migration_runner.AssetMigrator, "copy_from", fake_copy_from)

        parallel_migration_runner.run_parallel_migration(runner, max_workers=num_assets)

        # Snapshot copy at this scale is ~0.02s; serializing under the lock was ~0.7s.
        # 0.3s splits the two by an order of magnitude in each direction.
        assert worst_read_seconds[0] < 0.3, (
            f"a worker's state read stalled {worst_read_seconds[0]:.2f}s — "
            "state serialization is blocking parallel workers"
        )

        restored = MigrationState.from_json((tmp_path / "state.json").read_text(encoding="utf-8"))
        assert len(restored.rid_mapping[ResourceType.RUN.value]) == num_assets * mappings_per_asset
        assert len(restored.rid_mapping[ResourceType.DATASET_FILE.value]) == 2_000_000


class TestThreadSafeToJson:
    def test_to_json_matches_plain_state(self) -> None:
        """Snapshotting for serialization must not change the serialized output."""
        plain = MigrationState()
        safe = ThreadSafeMigrationState()
        for state in (plain, safe):
            state.record_mapping(ResourceType.ASSET, "a", "b")
            state.record_pending_multi_asset_workbook("wb-a", ["a1", "a2"])
            state.record_pending_multi_run_workbook("wb-r", ["r1"])
            state.record_skip(ResourceType.WORKBOOK, "wb-s", "out of scope")
        assert safe.to_json() == plain.to_json()

    def test_snapshot_covers_all_state_fields(self) -> None:
        """ThreadSafeMigrationState.to_json copies each MigrationState field explicitly;
        a new field must be added to that snapshot or it would silently vanish from the
        persisted state file.
        """
        assert {f.name for f in dataclasses.fields(MigrationState)} == {
            "rid_mapping",
            "pending_multi_asset_workbooks",
            "pending_multi_run_workbooks",
            "skipped_resources",
        }, "MigrationState fields changed: update ThreadSafeMigrationState.to_json's snapshot"

    def test_snapshot_is_isolated_from_concurrent_mutation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Serialization runs outside the lock, so workers mutate the live state while
        asdict/json.dumps walk the snapshot. The snapshot must be a genuine copy — shared
        inner dicts would make asdict crash with 'dictionary changed size during iteration'
        or leak mid-save mutations into the file.
        """
        safe = ThreadSafeMigrationState()
        safe.record_mapping(ResourceType.ASSET, "before", "b")
        original_to_json = MigrationState.to_json

        def mutating_to_json(self: MigrationState) -> str:
            # Runs on the snapshot, outside the state lock: mutate the live state from
            # another thread mid-serialization, exactly like a worker recording a mapping.
            mutator = threading.Thread(target=lambda: safe.record_mapping(ResourceType.ASSET, "during", "d"))
            mutator.start()
            mutator.join()
            return original_to_json(self)

        monkeypatch.setattr(MigrationState, "to_json", mutating_to_json)
        serialized = safe.to_json()
        assert "before" in serialized
        assert "during" not in serialized, "snapshot shares containers with the live state"
        assert safe.get_mapped_rid(ResourceType.ASSET, "during") == "d"

    def test_state_lock_is_released_during_serialization(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Serialization is O(state size); it must run outside the state lock so
        worker threads can keep recording mappings while a save is in progress.
        """
        safe = ThreadSafeMigrationState()
        safe.record_mapping(ResourceType.ASSET, "a", "b")
        lock_was_free: list[bool] = []
        original_to_json = MigrationState.to_json

        def probing_to_json(self: MigrationState) -> str:
            def probe() -> None:
                acquired = safe._lock.acquire(blocking=False)
                if acquired:
                    safe._lock.release()
                lock_was_free.append(acquired)

            probe_thread = threading.Thread(target=probe)
            probe_thread.start()
            probe_thread.join()
            return original_to_json(self)

        monkeypatch.setattr(MigrationState, "to_json", probing_to_json)
        safe.to_json()
        assert lock_was_free == [True]

    def test_to_json_is_reentrant_on_same_thread(self) -> None:
        """The signal flush handler may fire while the main thread already holds the lock
        (mid incremental save); serialization must not deadlock in that case.
        """
        safe = ThreadSafeMigrationState()
        safe.record_mapping(ResourceType.ASSET, "a", "b")
        with safe._lock:
            assert "rid_mapping" in safe.to_json()
