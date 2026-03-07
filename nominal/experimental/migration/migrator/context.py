from __future__ import annotations

from dataclasses import dataclass

from nominal.core import NominalClient
from nominal.experimental.migration.migration_state import MigrationState


@dataclass
class MigrationContext:
    """Shared context injected into migrators."""

    destination_client: NominalClient
    migration_state: MigrationState

    def record_mapping(self, resource_type: str, old_rid: str, new_rid: str) -> None:
        self.migration_state.record_mapping(resource_type=resource_type, old_rid=old_rid, new_rid=new_rid)
