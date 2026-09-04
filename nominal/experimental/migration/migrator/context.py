from __future__ import annotations

import concurrent.futures
import logging
import threading
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Callable, Mapping, TypeVar, cast

from nominal.core import NominalClient
from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.resource_type import ResourceType
from nominal.experimental.migration.utils.video_file_utils import DEFAULT_INGEST_POLL_TIMEOUT

DestinationClientResolver = Callable[[Any], NominalClient]
Resource = TypeVar("Resource")

logger = logging.getLogger(__name__)


@dataclass
class MigrationContext:
    """Shared context injected into migrators."""

    destination_client: NominalClient
    migration_state: MigrationState
    destination_client_resolver: DestinationClientResolver | None = None
    user_rid_mapping: Mapping[str, str] = field(default_factory=dict)
    """Source-to-destination user RID mapping, used to translate user-valued fields (e.g. checklist
    assignee) that are set explicitly on requests rather than derived from the calling identity."""
    source_asset_rids: frozenset[str] = field(default_factory=frozenset)
    dry_run: bool = False
    video_ingest_timeout: timedelta | None = DEFAULT_INGEST_POLL_TIMEOUT
    """How long to wait for a copied video to finish ingesting before moving on. `None` waits forever."""
    _singleflight_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _singleflight_futures: dict[tuple[str, str], concurrent.futures.Future[Any]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )

    def destination_client_for(self, source_resource: Any) -> NominalClient:
        if self.destination_client_resolver is None:
            return self.destination_client
        return self.destination_client_resolver(source_resource)

    def map_user_rid(self, source_user_rid: str | None) -> str | None:
        """Translate a source user RID to its destination equivalent, or None if unmapped."""
        if source_user_rid is None:
            return None
        destination_user_rid = self.user_rid_mapping.get(source_user_rid)
        # Only warn when a mapping is configured — an empty mapping is the normal
        # non-impersonation case, not a misconfigured one.
        if destination_user_rid is None and self.user_rid_mapping:
            logger.warning("No mapped destination user RID for source user %s.", source_user_rid)
        return destination_user_rid

    def record_mapping(self, resource_type: ResourceType, old_rid: str, new_rid: str) -> None:
        self.migration_state.record_mapping(resource_type=resource_type, old_rid=old_rid, new_rid=new_rid)

    def run_singleflight(
        self,
        *,
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
        key: tuple[str, str] = (source_rid, workspace_rid)

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
        except BaseException as exc:
            future.set_exception(exc)
            raise
        else:
            future.set_result(result)
            return result
        finally:
            with self._singleflight_lock:
                self._singleflight_futures.pop(key, None)
