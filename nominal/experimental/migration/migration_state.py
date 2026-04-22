from __future__ import annotations

from dataclasses import dataclass, field

from dataclass_wizard import JSONWizard

from nominal.experimental.migration.resource_type import ResourceType


@dataclass
class SkippedResource:
    resource_type: str
    source_rid: str
    reason: str


@dataclass
class MigrationState(JSONWizard):
    # resource_type -> old_rid -> new_rid
    rid_mapping: dict[str, dict[str, str]] = field(default_factory=dict)
    # source workbook_rid -> list of source asset_rids (for deferred multi-asset migration)
    pending_multi_asset_workbooks: dict[str, list[str]] = field(default_factory=dict)
    # source workbook_rid -> list of source run_rids (for deferred multi-run migration)
    pending_multi_run_workbooks: dict[str, list[str]] = field(default_factory=dict)
    # log of resources skipped due to missing dependencies or out-of-scope references
    skipped_resources: list[SkippedResource] = field(default_factory=list)

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

    def clear_pending_multi_asset_workbook(self, workbook_rid: str) -> None:
        self.pending_multi_asset_workbooks.pop(workbook_rid, None)

    def clear_pending_multi_run_workbook(self, workbook_rid: str) -> None:
        self.pending_multi_run_workbooks.pop(workbook_rid, None)

    def record_skip(self, resource_type: ResourceType, source_rid: str, reason: str) -> None:
        self.skipped_resources.append(SkippedResource(resource_type.value, source_rid, reason))
