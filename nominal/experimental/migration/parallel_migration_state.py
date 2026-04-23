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

    def record_pending_multi_asset_workbook(self, workbook_rid: str, asset_rids: list[str]) -> None:
        with self._lock:
            super().record_pending_multi_asset_workbook(workbook_rid, asset_rids)

    def record_pending_multi_run_workbook(self, workbook_rid: str, run_rids: list[str]) -> None:
        with self._lock:
            super().record_pending_multi_run_workbook(workbook_rid, run_rids)

    def clear_pending_multi_asset_workbook(self, workbook_rid: str) -> None:
        with self._lock:
            super().clear_pending_multi_asset_workbook(workbook_rid)

    def clear_pending_multi_run_workbook(self, workbook_rid: str) -> None:
        with self._lock:
            super().clear_pending_multi_run_workbook(workbook_rid)

    def record_skip(self, resource_type: ResourceType, source_rid: str, reason: str) -> None:
        with self._lock:
            super().record_skip(resource_type, source_rid, reason)

    def to_json(self, **encoder_kwargs: object) -> str:
        # dataclass_wizard cannot resolve forward references from migration_state.py when
        # introspecting this subclass, so serialize as a plain MigrationState instead.
        base = MigrationState(
            rid_mapping=self.rid_mapping,
            pending_multi_asset_workbooks=self.pending_multi_asset_workbooks,
            pending_multi_run_workbooks=self.pending_multi_run_workbooks,
            skipped_resources=self.skipped_resources,
        )
        return base.to_json(**encoder_kwargs)  # type: ignore[arg-type]
