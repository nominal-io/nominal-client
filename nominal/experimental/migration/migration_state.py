from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

from nominal.experimental.migration.resource_type import ResourceType


@dataclass
class SkippedResource:
    resource_type: str
    source_rid: str
    reason: str


@dataclass
class MigrationState:
    # resource_type -> old_rid -> new_rid
    rid_mapping: dict[str, dict[str, str]] = field(default_factory=dict)
    # source workbook_rid -> list of source asset_rids (for deferred multi-asset migration)
    pending_multi_asset_workbooks: dict[str, list[str]] = field(default_factory=dict)
    # source workbook_rid -> list of source run_rids (for deferred multi-run migration)
    pending_multi_run_workbooks: dict[str, list[str]] = field(default_factory=dict)
    # log of resources skipped due to missing dependencies or out-of-scope references
    skipped_resources: list[SkippedResource] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MigrationState:
        skipped_resources = [
            SkippedResource(**item)
            for item in data.get("skipped_resources", data.get("skippedResources", []))
            if isinstance(item, dict)
        ]
        return cls(
            rid_mapping=data.get("rid_mapping", data.get("ridMapping", {})),
            pending_multi_asset_workbooks=data.get(
                "pending_multi_asset_workbooks", data.get("pendingMultiAssetWorkbooks", {})
            ),
            pending_multi_run_workbooks=data.get(
                "pending_multi_run_workbooks", data.get("pendingMultiRunWorkbooks", {})
            ),
            skipped_resources=skipped_resources,
        )

    @classmethod
    def from_json(cls, data: str) -> MigrationState:
        return cls.from_dict(json.loads(data))

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    def record_mapping(self, resource_type: ResourceType, old_rid: str, new_rid: str) -> None:
        self.rid_mapping.setdefault(resource_type.value, {})[old_rid] = new_rid

    def get_mapped_rid(self, resource_type: ResourceType, old_rid: str) -> str | None:
        return self.rid_mapping.get(resource_type.value, {}).get(old_rid)

    def record_pending_multi_asset_workbook(self, workbook_rid: str, asset_rids: list[str]) -> None:
        """Record a multi-asset workbook for deferred migration. Overwrites any prior entry for idempotency."""
        self.pending_multi_asset_workbooks[workbook_rid] = asset_rids

    def record_pending_multi_run_workbook(self, workbook_rid: str, run_rids: list[str]) -> None:
        """Record a multi-run workbook for deferred migration. Overwrites any prior entry for idempotency."""
        self.pending_multi_run_workbooks[workbook_rid] = run_rids

    def record_pending_multi_asset_workbook_unless_skipped(self, workbook_rid: str, asset_rids: list[str]) -> bool:
        """Record a pending multi-asset workbook unless the workbook has already been skipped."""
        if self.workbook_was_skipped(workbook_rid):
            return False
        self.pending_multi_asset_workbooks[workbook_rid] = asset_rids
        return True

    def record_pending_multi_run_workbook_unless_skipped(self, workbook_rid: str, run_rids: list[str]) -> bool:
        """Record a pending multi-run workbook unless the workbook has already been skipped."""
        if self.workbook_was_skipped(workbook_rid):
            return False
        self.pending_multi_run_workbooks[workbook_rid] = run_rids
        return True

    def clear_pending_multi_asset_workbook(self, workbook_rid: str) -> None:
        self.pending_multi_asset_workbooks.pop(workbook_rid, None)

    def clear_pending_multi_run_workbook(self, workbook_rid: str) -> None:
        self.pending_multi_run_workbooks.pop(workbook_rid, None)

    def record_skip(self, resource_type: ResourceType, source_rid: str, reason: str) -> None:
        self.skipped_resources.append(SkippedResource(resource_type.value, source_rid, reason))

    def record_workbook_skip_and_clear_pending(self, workbook_rid: str, reason: str) -> bool:
        """Record a workbook skip as a terminal state and clear any stale pending entries."""
        changed = workbook_rid in self.pending_multi_asset_workbooks or workbook_rid in self.pending_multi_run_workbooks
        self.pending_multi_asset_workbooks.pop(workbook_rid, None)
        self.pending_multi_run_workbooks.pop(workbook_rid, None)
        if self.workbook_was_skipped(workbook_rid):
            return changed
        self.skipped_resources.append(SkippedResource(ResourceType.WORKBOOK.value, workbook_rid, reason))
        return True

    def workbook_was_skipped(self, workbook_rid: str) -> bool:
        return any(
            skipped.resource_type == ResourceType.WORKBOOK.value and skipped.source_rid == workbook_rid
            for skipped in self.skipped_resources
        )
