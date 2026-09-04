from __future__ import annotations

from typing import Sequence
from unittest.mock import MagicMock

import pytest

from nominal.core._event_types import EventType, SearchEventOriginType, SearchEventOriginTypes
from nominal.core.client import NominalClient
from nominal.core.event import Event
from nominal.core.exceptions import NominalNotFoundError
from nominal.protos.event.v2 import event_pb2
from nominal.protos.types import common_pb2
from nominal.protos.types.time import time_pb2


def _proto_event(
    rid: str = "ri.event.test",
    *,
    name: str = "original",
    type: event_pb2.EventType.ValueType = event_pb2.INFO,
    labels: Sequence[str] = (),
    created_by: str | None = None,
    seconds: int = 2,
    nanos: int = 3,
) -> event_pb2.Event:
    return event_pb2.Event(
        rid=rid,
        uuid="uuid-1",
        name=name,
        type=type,
        labels=list(labels),
        created_by=created_by,
        timestamp=time_pb2.Timestamp(seconds=seconds, nanos=nanos),
        duration=common_pb2.Duration(seconds=1, nanos=500),
    )


def _event(clients: MagicMock) -> Event:
    return Event._from_proto(clients, _proto_event(labels=["keep"]))


def test_update_omits_absent_fields_so_the_backend_leaves_them_unchanged() -> None:
    """None must not reach the wire: an omitted field is how a caller says "leave this alone"."""
    clients = MagicMock()
    event = _event(clients)
    clients.event.BatchUpdateEvent.return_value = event_pb2.BatchUpdateEventResponse(
        events=[_proto_event(name="renamed")]
    )

    event.update(name="renamed", type=None)

    update = clients.event.BatchUpdateEvent.call_args.args[0].updates[0]
    assert update.rid == event.rid
    assert update.name == "renamed"
    for field in ("description", "labels", "properties", "asset_rids", "timestamp", "duration", "type"):
        assert not update.HasField(field), f"{field} should be absent when omitted"
    assert event.name == "renamed"


def test_update_sends_empty_collections_as_explicit_clears() -> None:
    """An empty collection is a clear, which the update wrappers make distinguishable from omission."""
    clients = MagicMock()
    event = _event(clients)
    clients.event.BatchUpdateEvent.return_value = event_pb2.BatchUpdateEventResponse(events=[_proto_event()])

    event.update(assets=[], labels=[], properties={}, type=None)

    update = clients.event.BatchUpdateEvent.call_args.args[0].updates[0]
    assert (update.HasField("asset_rids"), list(update.asset_rids.asset_rids)) == (True, [])
    assert (update.HasField("labels"), list(update.labels.labels)) == (True, [])
    assert (update.HasField("properties"), dict(update.properties.properties)) == (True, {})


def test_from_proto_decodes_seconds_and_nanos() -> None:
    """Event carries Nominal's own seconds+nanos Timestamp and Duration, neither of which self-converts."""
    event = Event._from_proto(MagicMock(), _proto_event(seconds=2, nanos=3))

    assert event.start == 2_000_000_003
    assert event.duration == 1_000_000_500


@pytest.mark.parametrize("created_by", [None, "ri.user.1"])
def test_from_proto_reads_optional_created_by(created_by: str | None) -> None:
    """created_by is optional on the wire for legacy events; absent must not read as an empty rid."""
    assert Event._from_proto(MagicMock(), _proto_event(created_by=created_by)).created_by_rid == created_by


def test_from_proto_degrades_unknown_event_types() -> None:
    """Event types are an open wire enum, so an unrecognized one must not raise on read."""
    event = Event._from_proto(MagicMock(), _proto_event(type=event_pb2.EVENT_TYPE_UNSPECIFIED))

    assert event.type is EventType.UNKNOWN


@pytest.mark.parametrize(
    ("event_type", "wire_value"),
    [
        (EventType.INFO, event_pb2.INFO),
        (EventType.FLAG, event_pb2.FLAG),
        (EventType.ERROR, event_pb2.ERROR),
        (EventType.SUCCESS, event_pb2.SUCCESS),
        (EventType.UNKNOWN, event_pb2.EVENT_TYPE_UNSPECIFIED),
    ],
)
def test_event_type_maps_to_and_from_the_wire(event_type: EventType, wire_value: int) -> None:
    """Both directions are hand-written tables, so a transposed pair would otherwise go unnoticed."""
    assert event_type._to_proto() == wire_value
    if event_type is not EventType.UNKNOWN:
        assert EventType._from_proto(wire_value) is event_type


# Derived rather than listed so a newly declared origin type is round-tripped without editing this file.
_DECLARED_ORIGIN_TYPES = [
    origin_type
    for origin_type in vars(SearchEventOriginTypes).values()
    if isinstance(origin_type, SearchEventOriginType)
]


def test_every_declared_origin_type_is_discovered() -> None:
    """Guards the reflection below: a broken derivation would silently parametrize nothing."""
    assert len(_DECLARED_ORIGIN_TYPES) >= 6


@pytest.mark.parametrize("origin_type", _DECLARED_ORIGIN_TYPES, ids=lambda o: o.name)
def test_search_event_origin_type_round_trips(origin_type: SearchEventOriginType) -> None:
    """_to_proto resolves by name while _from_proto is a hand-written table.

    A member added to SearchEventOriginTypes but not to that table fails here, which is what keeps the
    two sides from drifting now that no shared lookup derives one from the other.
    """
    assert SearchEventOriginType._from_proto(origin_type._to_proto()) is origin_type


def test_create_event_puts_the_domain_values_on_the_wire() -> None:
    """Creation is the only path that sends a caller-chosen type, timestamp and duration."""
    clients = MagicMock()
    client = NominalClient(_clients=clients)
    clients.event.CreateEvent.return_value = event_pb2.CreateEventResponse(event=_proto_event())

    client.create_event("launch", EventType.FLAG, start=1_000_000_002, duration=3_000_000_004, assets=["ri.asset.1"])

    request = clients.event.CreateEvent.call_args.args[0]
    assert request.name == "launch"
    assert request.type == event_pb2.FLAG
    assert (request.timestamp.seconds, request.timestamp.nanos) == (1, 2)
    assert (request.duration.seconds, request.duration.nanos) == (3, 4)
    assert list(request.asset_rids) == ["ri.asset.1"]
    assert not request.HasField("description")


@pytest.mark.parametrize(
    ("returned", "expected_error"),
    [
        pytest.param(0, NominalNotFoundError, id="absent"),
        pytest.param(2, ValueError, id="ambiguous"),
    ],
)
def test_get_event_rejects_a_batch_that_is_not_exactly_one(returned: int, expected_error: type[Exception]) -> None:
    """A rid that does not resolve comes back as an absent batch entry, never a NOT_FOUND status."""
    clients = MagicMock()
    client = NominalClient(_clients=clients)
    clients.event.BatchGetEvents.return_value = event_pb2.BatchGetEventsResponse(
        events=[_proto_event() for _ in range(returned)]
    )

    with pytest.raises(expected_error):
        client.get_event("ri.event.test")
