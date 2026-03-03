from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Mapping

from nominal.core._event_types import EventType
from nominal.core.asset import Asset
from nominal.core.event import Event
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.ts import IntegralNanosecondsDuration, IntegralNanosecondsUTC

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EventCopyOptions(ResourceCopyOptions):
    new_name: str | None = None
    new_type: EventType | None = None
    new_start: datetime | IntegralNanosecondsUTC | None = None
    new_duration: timedelta | IntegralNanosecondsDuration = timedelta()
    new_description: str | None = None
    new_assets: Iterable[Asset | str] = ()
    new_properties: Mapping[str, str] | None = None
    new_labels: Iterable[str] = ()


class EventMigrator(Migrator[Event, Event, EventCopyOptions]):
    def clone(self, source: Event) -> Event:
        return self.copy_from(source, EventCopyOptions())

    def copy_from(self, source: Event, options: EventCopyOptions) -> Event:
        log_extras = {
            "destination_client_workspace": self.ctx.destination_client.get_workspace(
                self.ctx.destination_client._clients.workspace_rid
            ).rid
        }
        logger.debug(
            "Copying event %s (rid: %s)",
            source.name,
            source.rid,
            extra=log_extras,
        )
        new_event = self.ctx.destination_client.create_event(
            name=options.new_name or source.name,
            type=options.new_type or source.type,
            start=options.new_start or source.start,
            duration=options.new_duration or source.duration,
            description=options.new_description or source.description,
            assets=options.new_assets or source.asset_rids,
            properties=options.new_properties or source.properties,
            labels=options.new_labels or source.labels,
        )
        logger.debug(
            "New event created: %s (rid: %s)",
            new_event.name,
            new_event.rid,
            extra=log_extras,
        )
        self.record_mapping("EVENT", source.rid, new_event.rid)
        return new_event
