from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from dataclass_wizard import JSONWizard


class ResourceType(Enum):
    ASSET = "ASSET"
    DATASET = "DATASET"
    WORKBOOK_TEMPLATE = "WORKBOOK_TEMPLATE"
    RUN = "RUN"
    EVENT = "EVENT"
    VIDEO = "VIDEO"
    CHECKLIST = "CHECKLIST"
    DATA_REVIEW = "DATA_REVIEW"
    WORKBOOK = "WORKBOOK"


@dataclass
class MigrationState(JSONWizard):
    # resource_type -> old_rid -> new_rid
    rid_mapping: dict[str, dict[str, str]] = field(default_factory=dict)

    def record_mapping(self, resource_type: str, old_rid: str, new_rid: str) -> None:
        # Validate resource type early to catch typos.
        resource_type_key = ResourceType[resource_type].value
        self.rid_mapping.setdefault(resource_type_key, {})[old_rid] = new_rid

    def get_mapped_rid(self, resource_type: str, old_rid: str) -> str | None:
        resource_type_key = ResourceType[resource_type].value
        return self.rid_mapping.get(resource_type_key, {}).get(old_rid)
