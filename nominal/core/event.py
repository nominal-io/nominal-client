from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Iterable, Mapping, Protocol, Sequence

from typing_extensions import Self

from nominal.core import asset as core_asset
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._event_types import EventType as EventType  # noqa: PLC0414
from nominal.core._event_types import SearchEventOriginType as SearchEventOriginType  # noqa: PLC0414
from nominal.core._utils.api_tools import HasRid, RefreshableGrpcMixin, rid_from_instance_or_string
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.pagination_tools import search_events_paginated
from nominal.core._utils.query_tools import ArchiveStatusFilter, AssetMatch, create_search_events_query
from nominal.core.exceptions import NominalNotFoundError
from nominal.protos.event.v2 import event_pb2, event_pb2_grpc
from nominal.protos.types import types_pb2
from nominal.ts import (
    IntegralNanosecondsDuration,
    IntegralNanosecondsUTC,
    _from_proto_duration,
    _SecondsNanos,
    _to_proto_duration,
)


@dataclass(frozen=True)
class Event(HasRid, RefreshableGrpcMixin[event_pb2.Event]):
    rid: str
    asset_rids: Sequence[str]
    name: str
    description: str
    start: IntegralNanosecondsUTC
    duration: IntegralNanosecondsDuration
    properties: Mapping[str, str]
    labels: Sequence[str]
    type: EventType
    is_archived: bool

    _uuid: str = field(repr=False)

    # NOTE: may be missing for legacy events
    created_by_rid: str | None = field(repr=False)

    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def event(self) -> event_pb2_grpc.EventServiceStub: ...

    def _get_latest_api(self) -> event_pb2.Event:
        return _get_event(self._clients, self.rid)

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
        """Replace event metadata, updating the current instance and returning it.

        Metadata is replaced rather than appended. To add to labels or properties, merge them before
        calling. E.g.:

            new_labels = ["new-label-a", "new-label-b"]
            for old_label in event.labels:
                new_labels.append(old_label)
            event = event.update(labels=new_labels)

        Args:
            name: New name for the event.
            description: New description for the event.
            assets: Assets or asset rids the event applies to. An empty sequence clears them.
            start: New starting timestamp for the event.
            duration: New duration for the event.
            properties: Key-value properties replacing the existing ones. An empty mapping clears them.
            labels: Labels replacing the existing ones. An empty sequence clears them.
            type: New verbosity level for the event.

        Returns:
            This event, updated in place.

        Note:
            Every argument left as None is omitted from the request and the corresponding field is left
            unchanged. That is distinct from passing an empty collection, which clears the field.

        Raises:
            ValueError: If the backend does not echo back exactly one updated event.
            NominalError: If the update request fails.
        """
        updated_asset_rids = (
            None
            if assets is None
            else event_pb2.AssetRidSet(asset_rids=[rid_from_instance_or_string(asset) for asset in assets])
        )
        updated_timestamp = None if start is None else _SecondsNanos.from_flexible(start).to_proto()
        updated_duration = None if duration is None else _to_proto_duration(duration)
        updated_labels = None if labels is None else types_pb2.LabelUpdateWrapper(labels=list(labels))
        updated_properties = (
            None if properties is None else types_pb2.PropertyUpdateWrapper(properties=dict(properties))
        )
        updated_type = None if type is None else type._to_proto()

        request = event_pb2.BatchUpdateEventRequest(
            updates=[
                event_pb2.EventUpdate(
                    rid=self.rid,
                    asset_rids=updated_asset_rids,
                    duration=updated_duration,
                    labels=updated_labels,
                    name=name,
                    description=description,
                    properties=updated_properties,
                    timestamp=updated_timestamp,
                    type=updated_type,
                )
            ]
        )
        with translate_grpc_errors():
            batch_updated = self._clients.event.BatchUpdateEvent(request)
        if len(batch_updated.events) != 1:
            raise ValueError(f"Expected exactly one updated rid, received {len(batch_updated.events)}")

        return self._refresh_from_api(batch_updated.events[0])

    def archive(self) -> None:
        """Archives the event, preventing it from showing up in workbooks.

        Note: this does not update the instance in place; call `refresh()` to see the change reflected.
        """
        with translate_grpc_errors():
            self._clients.event.BatchArchiveEvent(event_pb2.BatchArchiveEventRequest(event_rids=[self.rid]))

    def unarchive(self) -> None:
        """Unarchives the event, allowing it to show up in workbooks.

        Note: this does not update the instance in place; call `refresh()` to see the change reflected.
        """
        with translate_grpc_errors():
            self._clients.event.BatchUnarchiveEvent(event_pb2.BatchUnarchiveEventRequest(event_rids=[self.rid]))

    @classmethod
    def _from_proto(cls, clients: _Clients, event: event_pb2.Event) -> Self:
        return cls(
            rid=event.rid,
            asset_rids=tuple(event.asset_rids),
            name=event.name,
            description=event.description,
            start=_SecondsNanos.from_proto(event.timestamp).to_nanoseconds(),
            duration=_from_proto_duration(event.duration),
            type=EventType._from_proto(event.type),
            is_archived=event.is_archived,
            properties=dict(event.properties),
            labels=list(event.labels),
            created_by_rid=event.created_by or None,
            _uuid=event.uuid,
            _clients=clients,
        )


def _get_events(clients: Event._Clients, rids: Sequence[str]) -> Sequence[event_pb2.Event]:
    with translate_grpc_errors():
        return clients.event.BatchGetEvents(event_pb2.BatchGetEventsRequest(event_rids=list(rids))).events


def _get_event(clients: Event._Clients, rid: str) -> event_pb2.Event:
    """The event with the given rid.

    Raises:
        NominalNotFoundError: If no event has that rid.
        ValueError: If more than one event is returned for it.
    """
    events = _get_events(clients, [rid])
    if not events:
        raise NominalNotFoundError(f"no event found with RID {rid!r}")
    if len(events) != 1:
        raise ValueError(f"Expected exactly one event with rid {rid!r}, received {len(events)}")
    return events[0]


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
    request = event_pb2.CreateEventRequest(
        name=name,
        description=description,
        asset_rids=[rid_from_instance_or_string(asset) for asset in (assets or [])],
        timestamp=_SecondsNanos.from_flexible(start).to_proto(),
        duration=_to_proto_duration(duration),
        properties=dict(properties or {}),
        labels=list(labels or []),
        type=type._to_proto(),
    )
    with translate_grpc_errors():
        response = clients.event.CreateEvent(request)
    return Event._from_proto(clients, response.event)


def _iter_search_events(
    clients: Event._Clients,
    query: event_pb2.SearchQuery,
    archive_status: ArchiveStatusFilter = ArchiveStatusFilter.NOT_ARCHIVED,
) -> Iterable[Event]:
    for e in search_events_paginated(clients.event, query, archive_status):
        yield Event._from_proto(clients, e)


def _search_events(
    clients: Event._Clients,
    *,
    search_text: str | None = None,
    after: str | datetime | IntegralNanosecondsUTC | None = None,
    before: str | datetime | IntegralNanosecondsUTC | None = None,
    asset_rids: Iterable[str] | None = None,
    asset_match: AssetMatch = AssetMatch.ALL,
    labels: Iterable[str] | None = None,
    properties: Mapping[str, str] | None = None,
    created_by_rid: str | None = None,
    workbook_rid: str | None = None,
    data_review_rid: str | None = None,
    assignee_rid: str | None = None,
    event_type: EventType | None = None,
    origin_types: Iterable[SearchEventOriginType] | None = None,
    workspace_rid: str | None = None,
    archive_status: ArchiveStatusFilter = ArchiveStatusFilter.NOT_ARCHIVED,
) -> Sequence[Event]:
    query = create_search_events_query(
        asset_rids=asset_rids,
        asset_match=asset_match,
        search_text=search_text,
        after=after,
        before=before,
        labels=labels,
        properties=properties,
        created_by_rid=created_by_rid,
        workbook_rid=workbook_rid,
        data_review_rid=data_review_rid,
        assignee_rid=assignee_rid,
        event_type=event_type,
        origin_types=origin_types,
        workspace_rid=workspace_rid,
    )
    return list(_iter_search_events(clients, query, archive_status))
