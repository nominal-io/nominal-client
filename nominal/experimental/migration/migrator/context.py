from __future__ import annotations

import concurrent.futures
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar, cast

from nominal.core import NominalClient
from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.resource_type import ResourceType

DestinationClientResolver = Callable[[Any], NominalClient]
Resource = TypeVar("Resource")


@dataclass
class MigrationContext:
    """Shared context injected into migrators."""

    destination_client: NominalClient
    migration_state: MigrationState
    destination_client_resolver: DestinationClientResolver | None = None
    _singleflight_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _singleflight_futures: dict[tuple[str, str, str], concurrent.futures.Future[Any]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )

    def destination_client_for(self, source_resource: Any) -> NominalClient:
        if self.destination_client_resolver is None:
            return self.destination_client
        return self.destination_client_resolver(source_resource)

    def record_mapping(self, resource_type: ResourceType, old_rid: str, new_rid: str) -> None:
        self.migration_state.record_mapping(resource_type=resource_type, old_rid=old_rid, new_rid=new_rid)

    def run_singleflight(
        self,
        *,
        resource_type: ResourceType,
        source_resource: Any,
        source_rid: str,
        fn: Callable[[], Resource],
    ) -> Resource:
        """Run migration work once per resource/workspace pair.

        Concurrent callers for the same source RID and destination workspace wait on
        the first in-flight migration and reuse its result.
        """
        destination_client = self.destination_client_for(source_resource)
        workspace_rid = destination_client._clients.workspace_rid
        if workspace_rid is None:
            raise ValueError("Destination client workspace RID is required for singleflight migrations.")
        key: tuple[str, str, str] = (resource_type.value, source_rid, workspace_rid)

        with self._singleflight_lock:
            future = self._singleflight_futures.get(key)
            if future is None:
                future = concurrent.futures.Future()
                self._singleflight_futures[key] = future
                is_owner = True
            else:
                is_owner = False

        if not is_owner:
            return cast(Resource, future.result())

        try:
            result = fn()
        except Exception as exc:
            future.set_exception(exc)
            raise
        else:
            future.set_result(result)
            return result
        finally:
            with self._singleflight_lock:
                self._singleflight_futures.pop(key, None)
