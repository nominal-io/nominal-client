from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Mapping

from nominal.core._event_types import EventType
from nominal.core.asset import Asset
from nominal.core.event import Event
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.resource_type import ResourceType
from nominal.ts import IntegralNanosecondsDuration, IntegralNanosecondsUTC


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


class EventMigrator(Migrator[Event, EventCopyOptions]):
    resource_type = ResourceType.EVENT

    def default_copy_options(self) -> EventCopyOptions:
        return EventCopyOptions()

    def _copy_from_impl(self, source: Event, options: EventCopyOptions) -> Event:
        return self.ctx.destination_client.create_event(
            name=options.new_name or source.name,
            type=options.new_type or source.type,
            start=options.new_start or source.start,
            duration=options.new_duration or source.duration,
            description=options.new_description or source.description,
            assets=options.new_assets or source.asset_rids,
            properties=options.new_properties or source.properties,
            labels=options.new_labels or source.labels,
        )

    def _get_resource_name(self, resource: Event) -> str:
        return resource.name

    def _get_resource_rid(self, resource: Event) -> str:
        return resource.rid
