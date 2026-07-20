"""Thread-safe migration state helpers for parallel resource migration."""

from __future__ import annotations

import threading
from typing import Callable

from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.resource_type import ResourceType


class ThreadSafeMigrationState(MigrationState):
    """Thread-safe wrapper around MigrationState for parallel migrations."""

    # Plain instance attributes (this subclass is not itself a @dataclass), so they stay
    # out of dataclasses.asdict and therefore out of the serialized state JSON.
    _lock: threading.RLock
    _persist_hook: Callable[[], None] | None

    def __init__(self, rid_mapping: dict[str, dict[str, str]] | None = None) -> None:
        """Initialize the shared migration state with an internal lock.

        The lock is reentrant: the SIGINT/SIGTERM flush handler runs on the main thread and
        calls save_state -> to_json, which must not deadlock if the signal interrupted the
        main thread while it already held the lock inside an incremental save.
        """
        super().__init__(rid_mapping=rid_mapping if rid_mapping is not None else {})
        self._lock = threading.RLock()
        self._persist_hook = None

    def set_persist_hook(self, hook: Callable[[], None]) -> None:
        """Invoke ``hook`` after every state mutation.

        Mutations are the single choke point for migration progress — hooking here persists
        child-resource mappings (runs, dataset files, workbooks, ...) recorded mid-asset,
        not just completed top-level tasks.
        """
        self._persist_hook = hook

    def _persist(self) -> None:
        # Called after the mutator releases the lock: the hook serializes state (re-taking
        # the lock itself) and does file IO, which must not block other workers' mutations.
        if self._persist_hook is not None:
            self._persist_hook()

    def record_mapping(self, resource_type: ResourceType, old_rid: str, new_rid: str) -> None:
        with self._lock:
            super().record_mapping(resource_type, old_rid, new_rid)
        self._persist()

    def get_mapped_rid(self, resource_type: ResourceType, old_rid: str) -> str | None:
        with self._lock:
            return super().get_mapped_rid(resource_type, old_rid)

    def record_pending_multi_asset_workbook(self, workbook_rid: str, asset_rids: list[str]) -> None:
        with self._lock:
            super().record_pending_multi_asset_workbook(workbook_rid, asset_rids)
        self._persist()

    def record_pending_multi_run_workbook(self, workbook_rid: str, run_rids: list[str]) -> None:
        with self._lock:
            super().record_pending_multi_run_workbook(workbook_rid, run_rids)
        self._persist()

    def record_pending_multi_asset_workbook_unless_skipped(self, workbook_rid: str, asset_rids: list[str]) -> bool:
        with self._lock:
            recorded = super().record_pending_multi_asset_workbook_unless_skipped(workbook_rid, asset_rids)
        if recorded:
            self._persist()
        return recorded

    def record_pending_multi_run_workbook_unless_skipped(self, workbook_rid: str, run_rids: list[str]) -> bool:
        with self._lock:
            recorded = super().record_pending_multi_run_workbook_unless_skipped(workbook_rid, run_rids)
        if recorded:
            self._persist()
        return recorded

    def clear_pending_multi_asset_workbook(self, workbook_rid: str) -> None:
        with self._lock:
            super().clear_pending_multi_asset_workbook(workbook_rid)
        self._persist()

    def clear_pending_multi_run_workbook(self, workbook_rid: str) -> None:
        with self._lock:
            super().clear_pending_multi_run_workbook(workbook_rid)
        self._persist()

    def record_skip(self, resource_type: ResourceType, source_rid: str, reason: str) -> None:
        with self._lock:
            super().record_skip(resource_type, source_rid, reason)
        self._persist()

    def record_workbook_skip_and_clear_pending(self, workbook_rid: str, reason: str) -> bool:
        with self._lock:
            changed = super().record_workbook_skip_and_clear_pending(workbook_rid, reason)
        if changed:
            self._persist()
        return changed

    def workbook_was_skipped(self, workbook_rid: str) -> bool:
        with self._lock:
            return super().workbook_was_skipped(workbook_rid)

    def to_json(self) -> str:
        # Serialization walks every nested dict, so it must hold the same lock as the
        # mutators — state is saved incrementally while worker threads are still writing.
        with self._lock:
            return super().to_json()
