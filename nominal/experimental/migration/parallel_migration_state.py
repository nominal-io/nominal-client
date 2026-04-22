"""Thread-safe migration state helpers for parallel resource migration."""

from __future__ import annotations

import threading

from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.resource_type import ResourceType


class ThreadSafeMigrationState(MigrationState):
    """Thread-safe wrapper around MigrationState for parallel migrations."""

    def __init__(self, rid_mapping: dict[str, dict[str, str]] | None = None) -> None:
        """Initialize the shared migration state with an internal lock."""
        super().__init__(rid_mapping=rid_mapping if rid_mapping is not None else {})
        self._lock = threading.Lock()

    def record_mapping(self, resource_type: ResourceType, old_rid: str, new_rid: str) -> None:
        with self._lock:
            super().record_mapping(resource_type, old_rid, new_rid)

    def get_mapped_rid(self, resource_type: ResourceType, old_rid: str) -> str | None:
        with self._lock:
            return super().get_mapped_rid(resource_type, old_rid)
