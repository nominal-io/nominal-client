from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from nominal.core import NominalClient
from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.resource_type import ResourceType

DestinationClientResolver = Callable[[Any], NominalClient]


@dataclass
class MigrationContext:
    """Shared context injected into migrators."""

    destination_client: NominalClient
    migration_state: MigrationState
    destination_client_resolver: DestinationClientResolver | None = None

    def destination_client_for(self, source_resource: Any) -> NominalClient:
        if self.destination_client_resolver is None:
            return self.destination_client
        return self.destination_client_resolver(source_resource)

    def record_mapping(self, resource_type: ResourceType, old_rid: str, new_rid: str) -> None:
        self.migration_state.record_mapping(resource_type=resource_type, old_rid=old_rid, new_rid=new_rid)
