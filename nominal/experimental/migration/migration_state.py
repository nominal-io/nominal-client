from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from dataclass_wizard import JSONWizard

from nominal.experimental.migration.resource_type import ResourceType


@dataclass
class MigrationState(JSONWizard):
    # resource_type -> old_rid -> new_rid
    rid_mapping: dict[str, dict[str, str]] = field(default_factory=defaultdict)

    def record_mapping(self, resource_type: ResourceType, old_rid: str, new_rid: str) -> None:
        self.rid_mapping.setdefault(resource_type.value, {})[old_rid] = new_rid

    def get_mapped_rid(self, resource_type: ResourceType, old_rid: str) -> str | None:
        return self.rid_mapping.get(resource_type.value, {}).get(old_rid)
