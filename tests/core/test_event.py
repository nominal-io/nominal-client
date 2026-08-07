from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest

from nominal.core._event_types import EventType, SearchEventOriginType, SearchEventOriginTypes
from nominal.core.client import NominalClient
from nominal.core.event import Event
from nominal.core.exceptions import NominalNotFoundError
from nominal.protos.event.v2 import event_pb2
from nominal.protos.types.time import time_pb2


def _proto_event(rid: str = "ri.event.test", **kwargs: object) -> event_pb2.Event:
    defaults: dict[str, object] = {
        "uuid": "uuid-1",
        "name": "original",
        "description": "",
        "type": event_pb2.INFO,
        "timestamp": time_pb2.Timestamp(seconds=2, nanos=3),
        "duration": event_pb2.Duration(seconds=1, nanos=500),
    }
    defaults.update(kwargs)
    return event_pb2.Event(rid=rid, **defaults)  # type: ignore[arg-type]


def _event(clients: MagicMock, rid: str = "ri.event.test") -> Event:
    return Event._from_proto(clients, _proto_event(rid, labels=["keep"]))


def test_update_leaves_omitted_fields_absent_on_the_wire() -> None:
    """Fields not passed to update() are absent from the request, so the backend leaves them unchanged."""
    clients = MagicMock()
    event = _event(clients)
    clients.event.BatchUpdateEvent.return_value = event_pb2.BatchUpdateEventResponse(
        events=[_proto_event(event.rid, name="renamed", labels=["keep"])]
    )

    event.update(name="renamed", type=None)

    request = clients.event.BatchUpdateEvent.call_args.args[0]
    assert len(request.updates) == 1
    update = request.updates[0]
    assert update.rid == event.rid
    assert update.name == "renamed"
    assert not update.HasField("description")
    assert not update.HasField("labels")
    assert not update.HasField("properties")
    assert not update.HasField("asset_rids")
    assert not update.HasField("timestamp")
    assert not update.HasField("duration")
    assert event.name == "renamed"


def test_update_sends_empty_collections_as_explicit_clears() -> None:
    """Passing empty labels/properties/assets sends present-but-empty wrappers (clear), distinct from omission."""
    clients = MagicMock()
    event = _event(clients)
    clients.event.BatchUpdateEvent.return_value = event_pb2.BatchUpdateEventResponse(events=[_proto_event(event.rid)])

    event.update(labels=[], properties={}, assets=[], type=None)

    update = clients.event.BatchUpdateEvent.call_args.args[0].updates[0]
    assert update.HasField("labels")
    assert list(update.labels.labels) == []
    assert update.HasField("properties")
    assert dict(update.properties.properties) == {}
    assert update.HasField("asset_rids")
    assert list(update.asset_rids.asset_rids) == []


def test_update_raises_when_backend_returns_wrong_event_count() -> None:
    """A batch update that does not echo exactly one event is a protocol violation, not a silent no-op."""
    clients = MagicMock()
    event = _event(clients)
    clients.event.BatchUpdateEvent.return_value = event_pb2.BatchUpdateEventResponse(events=[])

    with pytest.raises(ValueError, match="Expected exactly one updated rid"):
        event.update(name="renamed", type=None)


def test_get_event_translates_not_found(fake_rpc_error) -> None:
    """A NOT_FOUND status from the event service surfaces as NominalNotFoundError, not grpc.RpcError."""
    clients = MagicMock()
    client = NominalClient(_clients=clients)
    clients.event.BatchGetEvents.side_effect = fake_rpc_error(grpc.StatusCode.NOT_FOUND)

    with pytest.raises(NominalNotFoundError):
        client.get_event("ri.event.missing")


def test_archive_and_unarchive_send_the_events_rid() -> None:
    """archive()/unarchive() address exactly this event."""
    clients = MagicMock()
    event = _event(clients)

    event.archive()
    event.unarchive()

    assert list(clients.event.BatchArchiveEvent.call_args.args[0].event_rids) == [event.rid]
    assert list(clients.event.BatchUnarchiveEvent.call_args.args[0].event_rids) == [event.rid]


def test_from_proto_reads_timestamp_and_duration_as_nanoseconds() -> None:
    """Start and duration decode from seconds+nanos into integral nanoseconds."""
    event = Event._from_proto(MagicMock(), _proto_event())

    assert event.start == 2 * 1_000_000_000 + 3
    assert event.duration == 1 * 1_000_000_000 + 500


def test_from_proto_maps_absent_created_by_to_none() -> None:
    """created_by is optional on the wire (legacy events), and absent means None rather than an empty rid."""
    assert Event._from_proto(MagicMock(), _proto_event()).created_by_rid is None
    assert Event._from_proto(MagicMock(), _proto_event(created_by="ri.user.1")).created_by_rid == "ri.user.1"


def test_from_proto_maps_unrecognized_event_type_to_unknown() -> None:
    """An event type this client does not know about degrades to UNKNOWN instead of raising."""
    assert Event._from_proto(MagicMock(), _proto_event(type=event_pb2.EVENT_TYPE_UNSPECIFIED)).type == EventType.UNKNOWN


def test_refresh_updates_fields_in_place() -> None:
    """refresh() re-fetches via BatchGetEvents and updates the same instance."""
    clients = MagicMock()
    event = _event(clients)
    clients.event.BatchGetEvents.return_value = event_pb2.BatchGetEventsResponse(
        events=[_proto_event(event.rid, name="renamed-elsewhere")]
    )

    event.refresh()

    assert event.name == "renamed-elsewhere"


def test_refresh_raises_not_found_when_the_event_is_gone() -> None:
    """An event that no longer resolves comes back as an empty batch, not a NOT_FOUND status."""
    clients = MagicMock()
    event = _event(clients)
    clients.event.BatchGetEvents.return_value = event_pb2.BatchGetEventsResponse(events=[])

    with pytest.raises(NominalNotFoundError, match="no event found with RID"):
        event.refresh()


def test_refresh_raises_when_backend_returns_too_many_events() -> None:
    """More than one event for a single rid is a protocol violation, not a partial refresh."""
    clients = MagicMock()
    event = _event(clients)
    clients.event.BatchGetEvents.return_value = event_pb2.BatchGetEventsResponse(
        events=[_proto_event(event.rid), _proto_event(event.rid)]
    )

    with pytest.raises(ValueError, match="Expected exactly one event"):
        event.refresh()


@pytest.mark.parametrize(
    ("event_type", "expected"),
    [
        (EventType.INFO, event_pb2.INFO),
        (EventType.FLAG, event_pb2.FLAG),
        (EventType.ERROR, event_pb2.ERROR),
        (EventType.SUCCESS, event_pb2.SUCCESS),
        (EventType.UNKNOWN, event_pb2.EVENT_TYPE_UNSPECIFIED),
    ],
)
def test_event_type_to_proto_maps_each_member(event_type: EventType, expected: int) -> None:
    """Pins the outbound mapping: this is what create/update/search put on the wire."""
    assert event_type._to_proto() == expected


@pytest.mark.parametrize("event_type", [EventType.INFO, EventType.FLAG, EventType.ERROR, EventType.SUCCESS])
def test_event_type_round_trips(event_type: EventType) -> None:
    """Every named type survives a trip to the wire and back."""
    assert EventType._from_proto(event_type._to_proto()) is event_type


@pytest.mark.parametrize(
    "origin_type",
    [
        SearchEventOriginTypes.WORKBOOK,
        SearchEventOriginTypes.TEMPLATE,
        SearchEventOriginTypes.API,
        SearchEventOriginTypes.DATA_REVIEW,
        SearchEventOriginTypes.PROCEDURE,
        SearchEventOriginTypes.STREAMING_CHECKLIST,
    ],
)
def test_search_event_origin_type_round_trips(origin_type: SearchEventOriginType) -> None:
    """Origin types convert by name, so every member must resolve in both directions."""
    assert SearchEventOriginType._from_proto(origin_type._to_proto()) is origin_type


def test_search_event_origin_type_from_proto_rejects_unspecified() -> None:
    """UNSPECIFIED names no origin type, so it is an error rather than a silent default."""
    with pytest.raises(ValueError, match="Unexpected Event Origin"):
        SearchEventOriginType._from_proto(event_pb2.SEARCH_EVENT_ORIGIN_TYPE_UNSPECIFIED)


def test_create_event_builds_the_request_from_domain_types() -> None:
    """Creation is the one path that puts a user-chosen EventType, timestamp and duration on the wire."""
    clients = MagicMock()
    client = NominalClient(_clients=clients)
    clients.event.CreateEvent.return_value = event_pb2.CreateEventResponse(event=_proto_event())

    client.create_event("launch", EventType.FLAG, start=1_000_000_002, duration=3_000_000_004)

    request = clients.event.CreateEvent.call_args.args[0]
    assert request.name == "launch"
    assert request.type == event_pb2.FLAG
    assert (request.timestamp.seconds, request.timestamp.nanos) == (1, 2)
    assert (request.duration.seconds, request.duration.nanos) == (3, 4)
    assert not request.HasField("description")


def test_create_event_associates_the_given_assets() -> None:
    """Asset rids are accepted as instances or bare rids and land on the request."""
    clients = MagicMock()
    client = NominalClient(_clients=clients)
    clients.event.CreateEvent.return_value = event_pb2.CreateEventResponse(event=_proto_event())

    client.create_event("launch", EventType.INFO, start=0, assets=["ri.asset.1", "ri.asset.2"])

    assert list(clients.event.CreateEvent.call_args.args[0].asset_rids) == ["ri.asset.1", "ri.asset.2"]
