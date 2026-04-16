from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Mapping

from nominal.core import NominalClient
from nominal.core._event_types import EventType
from nominal.core.asset import Asset
from nominal.core.event import Event
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.resource_type import ResourceType
from nominal.ts import IntegralNanosecondsDuration, IntegralNanosecondsUTC

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EventCopyOptions(ResourceCopyOptions):
    new_name: str | None = None
    new_type: EventType | None = None
    new_start: datetime | IntegralNanosecondsUTC | None = None
    new_duration: timedelta | IntegralNanosecondsDuration | None = None
    new_description: str | None = None
    new_assets: Iterable[Asset | str] | None = None
    new_properties: Mapping[str, str] | None = None
    new_labels: Iterable[str] | None = None


class EventMigrator(Migrator[Event, EventCopyOptions]):
    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.EVENT

    def use_singleflight(self) -> bool:
        return True

    def default_copy_options(self) -> EventCopyOptions:
        return EventCopyOptions()

    def _get_existing_destination_resource(self, destination_client: NominalClient, mapped_rid: str) -> Event:
        return destination_client.get_event(mapped_rid)

    def _copy_from_impl(self, source: Event, options: EventCopyOptions) -> Event:
        existing_event = self.get_existing_destination_resource(source)
        if existing_event is not None:
            return existing_event

        destination_client = self.destination_client_for(source)
        new_event = destination_client.create_event(
            name=options.new_name if options.new_name is not None else source.name,
            type=options.new_type if options.new_type is not None else source.type,
            start=options.new_start if options.new_start is not None else source.start,
            duration=options.new_duration if options.new_duration is not None else source.duration,
            description=options.new_description if options.new_description is not None else source.description,
            assets=options.new_assets if options.new_assets is not None else source.asset_rids,
            properties=options.new_properties if options.new_properties is not None else source.properties,
            labels=options.new_labels if options.new_labels is not None else source.labels,
        )
        self.ctx.migration_state.record_mapping(self.resource_type, source.rid, new_event.rid)
        return new_event

    def _get_resource_name(self, resource: Event) -> str:
        return resource.name
