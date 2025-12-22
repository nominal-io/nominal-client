from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Iterable, Mapping, Protocol, Sequence

from nominal_api import event
from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid, RefreshableMixin, rid_from_instance_or_string
from nominal.core._utils.pagination_tools import search_events_paginated
from nominal.ts import IntegralNanosecondsDuration, IntegralNanosecondsUTC, _SecondsNanos, _to_api_duration

if TYPE_CHECKING:
    import nominal.core.asset as core_asset


@dataclass(frozen=True)
class Event(HasRid, RefreshableMixin[event.Event]):
    rid: str
    asset_rids: Sequence[str]
    name: str
    description: str
    start: IntegralNanosecondsUTC
    duration: IntegralNanosecondsDuration
    properties: Mapping[str, str]
    labels: Sequence[str]
    type: EventType

    _uuid: str = field(repr=False)

    # NOTE: may be missing for legacy events
    created_by_rid: str | None = field(repr=False)

    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def event(self) -> event.EventService: ...

    def _get_latest_api(self) -> event.Event:
        resp = self._clients.event.batch_get_events(self._clients.auth_header, [self.rid])
        if len(resp) != 0:
            raise ValueError(f"Expected exactly one event with rid {self.rid}, received {len(resp)}")

        return resp[0]

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        assets: Iterable[core_asset.Asset | str] | None = None,
        start: datetime | IntegralNanosecondsUTC | None = None,
        duration: timedelta | IntegralNanosecondsDuration | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Iterable[str] | None = None,
        type: EventType | None,
    ) -> Self:
        """Replace event metadata.
        Updates the current instance, and returns it.
        Only the metadata passed in will be replaced, the rest will remain untouched.

        Note: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
        calling this method. E.g.:

            new_labels = ["new-label-a", "new-label-b"]
            for old_label in event.labels:
                new_labels.append(old_label)
            event = event.update(labels=new_labels)
        """
        request = event.BatchUpdateEventRequest(
            requests=[
                event.UpdateEventRequest(
                    rid=self.rid,
                    asset_rids=None if assets is None else [rid_from_instance_or_string(asset) for asset in assets],
                    duration=None if duration is None else _to_api_duration(duration),
                    labels=None if labels is None else list(labels),
                    name=name,
                    description=description,
                    properties=None if properties is None else dict(properties),
                    timestamp=None if start is None else _SecondsNanos.from_flexible(start).to_api(),
                    type=None if type is None else type._to_api_event_type(),
                )
            ]
        )
        batch_updated = self._clients.event.batch_update_event(self._clients.auth_header, request)
        if len(batch_updated.events) != 1:
            raise ValueError(f"Expected exactly one updated rid, received {len(batch_updated.events)}")

        return self._refresh_from_api(batch_updated.events[0])

    def archive(self) -> None:
        """Archives the event, preventing it from showing up in workbooks."""
        self._clients.event.batch_archive_event(self._clients.auth_header, [self.rid])

    def unarchive(self) -> None:
        """Unarchives the event, allowing it to show up in workbooks."""
        self._clients.event.batch_unarchive_event(self._clients.auth_header, [self.rid])

    @classmethod
    def _from_conjure(cls, clients: _Clients, event: event.Event) -> Self:
        if event.duration.picos:
            warnings.warn(
                f"event '{event.name}' ({event.rid}) has a duration specified in picoseconds: "
                "currently, any sub-nanosecond precision will be truncated in nominal-client",
                UserWarning,
                stacklevel=2,
            )
        return cls(
            rid=event.rid,
            asset_rids=tuple(event.asset_rids),
            name=event.name,
            description=event.description,
            start=_SecondsNanos.from_api(event.timestamp).to_nanoseconds(),
            duration=event.duration.seconds * 1_000_000_000 + event.duration.nanos,
            type=EventType.from_api_event_type(event.type),
            properties=event.properties,
            labels=event.labels,
            created_by_rid=event.created_by,
            _uuid=event.uuid,
            _clients=clients,
        )


class EventType(Enum):
    INFO = "INFO"
    FLAG = "FLAG"
    ERROR = "ERROR"
    SUCCESS = "SUCCESS"
    UNKNOWN = "UNKNOWN"

    @classmethod
    def from_api_event_type(cls, event: event.EventType) -> EventType:
        if event.name == "INFO":
            return cls.INFO
        elif event.name == "FLAG":
            return cls.FLAG
        elif event.name == "ERROR":
            return cls.ERROR
        elif event.name == "SUCCESS":
            return cls.SUCCESS
        else:
            return cls.UNKNOWN

    def _to_api_event_type(self) -> event.EventType:
        if self.name == "INFO":
            return event.EventType.INFO
        elif self.name == "FLAG":
            return event.EventType.FLAG
        elif self.name == "ERROR":
            return event.EventType.ERROR
        elif self.name == "SUCCESS":
            return event.EventType.SUCCESS
        else:
            return event.EventType.UNKNOWN


def _create_event(
    clients: Event._Clients,
    *,
    name: str,
    type: EventType,
    start: datetime | IntegralNanosecondsUTC,
    duration: timedelta | IntegralNanosecondsDuration,
    assets: Iterable[core_asset.Asset | str] | None,
    description: str | None,
    properties: Mapping[str, str] | None,
    labels: Iterable[str] | None,
) -> Event:
    request = event.CreateEvent(
        name=name,
        description=description,
        asset_rids=[rid_from_instance_or_string(asset) for asset in (assets or [])],
        timestamp=_SecondsNanos.from_flexible(start).to_api(),
        duration=_to_api_duration(duration),
        origins=[],
        properties=dict(properties or {}),
        labels=list(labels or []),
        type=type._to_api_event_type(),
    )
    response = clients.event.create_event(clients.auth_header, request)
    return Event._from_conjure(clients, response)


def _iter_search_events(clients: Event._Clients, query: event.SearchQuery) -> Iterable[Event]:
    for e in search_events_paginated(clients.event, clients.auth_header, query):
        yield Event._from_conjure(clients, e)


def _search_events(
    clients: Event._Clients,
    *,
    search_text: str | None = None,
    after: str | datetime | IntegralNanosecondsUTC | None = None,
    before: str | datetime | IntegralNanosecondsUTC | None = None,
    assets: Iterable[str] | None = None,
    labels: Iterable[str] | None = None,
    properties: Mapping[str, str] | None = None,
    created_by: str | None = None,
    workbook: str | None = None,
    data_review: str | None = None,
    assignee: str | None = None,
    event_type: EventType | None = None,
    workspace_rid: str | None = None,
) -> Sequence[Event]:
    query = _create_search_events_query(
        assets=assets,
        search_text=search_text,
        after=after,
        before=before,
        labels=labels,
        properties=properties,
        created_by=rid_from_instance_or_string(created_by) if created_by else None,
        workbook=rid_from_instance_or_string(workbook) if workbook else None,
        data_review=rid_from_instance_or_string(data_review) if data_review else None,
        assignee=rid_from_instance_or_string(assignee) if assignee else None,
        event_type=event_type,
        workspace_rid=workspace_rid,
    )
    return list(_iter_search_events(clients, query))


def _create_search_events_query(  # noqa: PLR0912
    search_text: str | None = None,
    after: str | datetime | IntegralNanosecondsUTC | None = None,
    before: str | datetime | IntegralNanosecondsUTC | None = None,
    assets: Iterable[str] | None = None,
    labels: Iterable[str] | None = None,
    properties: Mapping[str, str] | None = None,
    created_by: str | None = None,
    workbook: str | None = None,
    data_review: str | None = None,
    assignee: str | None = None,
    event_type: EventType | None = None,
    workspace_rid: str | None = None,
) -> event.SearchQuery:
    queries = []
    if search_text is not None:
        queries.append(event.SearchQuery(search_text=search_text))
    if after is not None:
        queries.append(event.SearchQuery(after=_SecondsNanos.from_flexible(after).to_api()))
    if before is not None:
        queries.append(event.SearchQuery(before=_SecondsNanos.from_flexible(before).to_api()))
    if assets:
        for asset in assets:
            queries.append(event.SearchQuery(asset=asset))
    if labels:
        for label in labels:
            queries.append(event.SearchQuery(label=label))
    if properties:
        for name, value in properties.items():
            queries.append(event.SearchQuery(property=api.Property(name=name, value=value)))
    if created_by:
        queries.append(event.SearchQuery(created_by=created_by))
    if workbook is not None:
        queries.append(event.SearchQuery(workbook=workbook))
    if data_review is not None:
        queries.append(event.SearchQuery(data_review=data_review))
    if assignee is not None:
        queries.append(event.SearchQuery(assignee=assignee))
    if event_type is not None:
        queries.append(event.SearchQuery(event_type=event_type._to_api_event_type()))
    if workspace_rid is not None:
        queries.append(event.SearchQuery(workspace=workspace_rid))

    return event.SearchQuery(and_=queries)
