from __future__ import annotations

from typing import Callable
from unittest.mock import MagicMock

import grpc
import pytest

from nominal.core.client import NominalClient
from nominal.core.connection import StreamingConnection
from nominal.core.elements import Symbol
from nominal.core.exceptions import NominalError, NominalNotFoundError, NominalPermissionDeniedError
from nominal.core.marking import MarkableMixin, Marking
from nominal.core.video import Video
from nominal.protos.authorization.markings.v1 import markings_pb2


def _marking(rid: str = "ri.marking.a", id: str = "itar") -> markings_pb2.Marking:
    marking = markings_pb2.Marking(rid=rid, id=id, description="controlled", is_archived=False)
    marking.symbol.emoji = ":lock:"
    marking.color.hex_code = "#cc0000"
    marking.created_at.FromNanoseconds(1_000)
    marking.updated_at.FromNanoseconds(2_000)
    return marking


def _metadata(rid: str, id: str = "itar") -> markings_pb2.MarkingMetadata:
    return markings_pb2.MarkingMetadata(rid=rid, id=id, description="", is_archived=False)


class _Markable(MarkableMixin):
    """Minimal stand-in for a data source, exercising the mixin's own behavior."""

    def __init__(self, rid: str, clients: MagicMock) -> None:
        self.rid = rid
        self._clients = clients


@pytest.fixture
def client(mock_clients: MagicMock) -> NominalClient:
    """A NominalClient over the shared mock clients bunch."""
    return NominalClient(_clients=mock_clients)


@pytest.fixture
def markable(mock_clients: MagicMock) -> _Markable:
    """A data source stand-in carrying no markings until `applied` says otherwise."""
    _apply_to(mock_clients, "ri.dataset.a")
    return _Markable("ri.dataset.a", mock_clients)


def _apply_to(clients: MagicMock, resource: str, *marking_rids: str) -> None:
    """Make the service report `marking_rids` as applied to `resource`, echoing them back on lookup."""
    response = markings_pb2.GetMarkingsForResourcesResponse()
    for marking_rid in marking_rids:
        response.resource_to_markings[resource].applied_markings.add(marking_rid=marking_rid)
    clients.markings.GetMarkingsForResources.return_value = response
    clients.markings.BatchGetMarkingMetadata.side_effect = lambda request: (
        markings_pb2.BatchGetMarkingMetadataResponse(marking_metadatas=[_metadata(rid) for rid in request.marking_rids])
    )


def _build_video(clients: MagicMock, rid: str) -> Video:
    return Video(
        rid=rid,
        name="v",
        description=None,
        properties={},
        labels=[],
        created_at=0,
        is_archived=False,
        _clients=clients,
    )


def _build_streaming_connection(clients: MagicMock, rid: str) -> StreamingConnection:
    return StreamingConnection(
        rid=rid,
        name="c",
        description=None,
        _clients=clients,
        nominal_data_source_rid=rid,
    )


@pytest.mark.parametrize(
    ("id_substring", "expected_clauses"),
    [(None, []), ("ita", ["ita"])],
    ids=["unfiltered", "substring"],
)
def test_search_sends_only_the_filters_the_caller_asked_for(
    client: NominalClient, mock_clients: MagicMock, id_substring: str | None, expected_clauses: list[str]
) -> None:
    """Searching without a substring filters on nothing; passing one sends exactly that filter."""
    mock_clients.markings.SearchMarkings.return_value = markings_pb2.SearchMarkingsResponse(
        marking_metadatas=[_metadata("ri.marking.a")], next_page_token=""
    )

    client.search_markings(id_substring)

    query = mock_clients.markings.SearchMarkings.call_args.args[0].query
    assert [c.id_exact_substring_search for c in getattr(query, "and").queries] == expected_clauses


def test_search_returns_markings_from_every_page(client: NominalClient, mock_clients: MagicMock) -> None:
    """Pagination is invisible to the caller: markings from every page come back in one sequence."""
    mock_clients.markings.SearchMarkings.side_effect = [
        markings_pb2.SearchMarkingsResponse(marking_metadatas=[_metadata("a", id="first")], next_page_token="tok"),
        markings_pb2.SearchMarkingsResponse(marking_metadatas=[_metadata("b", id="second")], next_page_token=""),
    ]

    results = client.search_markings()

    assert [(m.rid, m.id) for m in results] == [("a", "first"), ("b", "second")]


@pytest.mark.parametrize("bad_id", ["ITAR", "1itar", "-itar", "it_ar", "it ar", ""])
@pytest.mark.parametrize("method", ["create_marking", "get_marking_by_id"], ids=["create", "get_by_id"])
def test_marking_ids_are_rejected_locally_when_the_service_would_reject_them(
    client: NominalClient, mock_clients: MagicMock, method: str, bad_id: str
) -> None:
    """An invalid id fails before the round trip, wherever an id is accepted."""
    with pytest.raises(ValueError, match="marking id"):
        getattr(client, method)(bad_id)

    assert mock_clients.markings.mock_calls == []


def test_create_marking_sends_its_metadata_and_returns_the_marking(
    client: NominalClient, mock_clients: MagicMock
) -> None:
    """Every field the caller supplies reaches the request, with an uppercase color lowercased."""
    mock_clients.markings.CreateMarking.return_value = markings_pb2.CreateMarkingResponse(marking=_marking())

    marking = client.create_marking(
        "itar",
        description="controlled",
        authorized_groups=["ri.group.a"],
        symbol=Symbol.emoji(":lock:"),
        color="#CC0000",
    )

    request = mock_clients.markings.CreateMarking.call_args.args[0]
    assert request.id == "itar"
    assert request.description == "controlled"
    assert list(request.authorized_groups.group_rids) == ["ri.group.a"]
    assert request.symbol.emoji == ":lock:"
    assert request.color.hex_code == "#cc0000"
    assert marking.rid == "ri.marking.a"


def test_get_marking_returns_a_fully_hydrated_marking(client: NominalClient, mock_clients: MagicMock) -> None:
    """A retrieved marking carries every field the service sent, not just its RID."""
    mock_clients.markings.GetMarking.return_value = markings_pb2.GetMarkingResponse(marking=_marking())

    marking = client.get_marking("ri.marking.a")

    assert marking.rid == "ri.marking.a"
    assert marking.id == "itar"
    assert marking.description == "controlled"
    assert marking.symbol == Symbol.emoji(":lock:")
    assert marking.color == "#cc0000"
    assert marking.created_at == 1_000
    assert marking.updated_at == 2_000
    assert marking.is_archived is False


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [
        (grpc.StatusCode.NOT_FOUND, NominalNotFoundError),
        (grpc.StatusCode.PERMISSION_DENIED, NominalPermissionDeniedError),
    ],
    ids=["missing", "unreadable"],
)
def test_get_marking_keeps_missing_and_unreadable_distinct(
    client: NominalClient,
    mock_clients: MagicMock,
    fake_rpc_error: Callable[[grpc.StatusCode], grpc.RpcError],
    status_code: grpc.StatusCode,
    expected: type[NominalError],
) -> None:
    """A marking that is absent and one the caller cannot read raise different errors."""
    mock_clients.markings.GetMarking.side_effect = fake_rpc_error(status_code)

    with pytest.raises(expected):
        client.get_marking("ri.marking.a")


def test_update_distinguishes_unchanged_from_cleared_symbol(mock_clients: MagicMock) -> None:
    """Omitting symbol leaves it alone; passing None clears it. The wrapper encodes that difference."""
    mock_clients.markings.UpdateMarking.return_value = markings_pb2.UpdateMarkingResponse(marking=_marking())
    marking = Marking._from_proto(mock_clients, _marking())

    marking.update(description="new")
    unchanged = mock_clients.markings.UpdateMarking.call_args.args[0]
    assert not unchanged.HasField("symbol")
    assert unchanged.description == "new"

    marking.update(symbol=None)
    cleared = mock_clients.markings.UpdateMarking.call_args.args[0]
    assert cleared.HasField("symbol")
    assert not cleared.symbol.HasField("value")

    marking.update(symbol=Symbol.icon("castle"))
    assigned = mock_clients.markings.UpdateMarking.call_args.args[0]
    assert assigned.symbol.value.icon == "castle"


def test_update_clears_authorized_groups_with_an_empty_sequence(mock_clients: MagicMock) -> None:
    """None leaves groups alone; an empty sequence clears them."""
    mock_clients.markings.UpdateMarking.return_value = markings_pb2.UpdateMarkingResponse(marking=_marking())
    marking = Marking._from_proto(mock_clients, _marking())

    marking.update(description="x")
    assert not mock_clients.markings.UpdateMarking.call_args.args[0].HasField("authorized_groups")

    marking.update(authorized_groups=[])
    cleared = mock_clients.markings.UpdateMarking.call_args.args[0]
    assert cleared.HasField("authorized_groups")
    assert list(cleared.authorized_groups.group_rids) == []


def test_authorized_group_rids_reads_this_markings_entry(mock_clients: MagicMock) -> None:
    """The batch-keyed response is indexed by this marking's own RID, not taken from the first entry."""
    response = markings_pb2.GetAuthorizedGroupsByMarkingResponse()
    response.authorized_groups_by_marking["ri.marking.other"].group_rids.append("ri.group.wrong")
    response.authorized_groups_by_marking["ri.marking.a"].group_rids.append("ri.group.a")
    mock_clients.markings.GetAuthorizedGroupsByMarking.return_value = response
    marking = Marking._from_proto(mock_clients, _marking())

    assert marking.authorized_group_rids() == ("ri.group.a",)


@pytest.mark.parametrize(
    "applied",
    [(), ("ri.marking.a",), ("ri.marking.a", "ri.marking.b")],
    ids=["none", "one", "several"],
)
def test_list_markings_returns_the_markings_applied_to_the_resource(
    mock_clients: MagicMock, applied: tuple[str, ...]
) -> None:
    """Listing hydrates exactly the applied markings, and is empty when the resource carries none."""
    _apply_to(mock_clients, "ri.dataset.a", *applied)

    markings = _Markable("ri.dataset.a", mock_clients).list_markings()

    assert tuple(m.rid for m in markings) == applied


@pytest.mark.parametrize(
    ("method", "expected_apply", "expected_remove"),
    [("apply_markings", ["ri.marking.a"], []), ("remove_markings", [], ["ri.marking.a"])],
    ids=["apply", "remove"],
)
def test_apply_and_remove_each_send_a_one_sided_update(
    markable: _Markable,
    mock_clients: MagicMock,
    method: str,
    expected_apply: list[str],
    expected_remove: list[str],
) -> None:
    """Applying never removes and removing never applies, so neither disturbs the other markings."""
    getattr(markable, method)(["ri.marking.a"])

    request = mock_clients.markings.UpdateMarkingsOnResource.call_args.args[0]
    assert request.resource == "ri.dataset.a"
    assert list(request.markings_to_apply) == expected_apply
    assert list(request.markings_to_remove) == expected_remove


@pytest.mark.parametrize("as_instance", [True, False], ids=["instance", "rid"])
def test_markings_may_be_given_as_instances_or_rids(
    markable: _Markable, mock_clients: MagicMock, as_instance: bool
) -> None:
    """A Marking and its RID are interchangeable wherever markings are accepted."""
    marking = Marking._from_proto(mock_clients, _marking(rid="ri.marking.a"))

    markable.apply_markings([marking if as_instance else "ri.marking.a"])

    request = mock_clients.markings.UpdateMarkingsOnResource.call_args.args[0]
    assert list(request.markings_to_apply) == ["ri.marking.a"]


@pytest.mark.parametrize(
    "build_resource",
    [_build_video, _build_streaming_connection],
    ids=["video", "streaming_connection"],
)
def test_every_data_source_type_exposes_its_markings(
    mock_clients: MagicMock, build_resource: Callable[[MagicMock, str], MarkableMixin]
) -> None:
    """Pins the mixin onto the real classes: `Video` and `DataSource` each carry it independently."""
    _apply_to(mock_clients, "ri.resource.a", "ri.marking.a")

    resource = build_resource(mock_clients, "ri.resource.a")

    assert [m.id for m in resource.list_markings()] == ["itar"]


@pytest.mark.parametrize(
    ("markings", "expected_rids"),
    [(None, []), (["ri.marking.b"], ["ri.marking.b"])],
    ids=["omitted", "given"],
)
def test_create_dataset_forwards_markings(
    client: NominalClient,
    mock_clients: MagicMock,
    markings: list[str] | None,
    expected_rids: list[str],
) -> None:
    """Markings reach the create request, and omitting them sends an empty list rather than null."""
    client.create_dataset("ds", markings=markings)

    assert mock_clients.catalog.create_dataset.call_args.args[1].marking_rids == expected_rids


def test_create_dataset_accepts_marking_instances(client: NominalClient, mock_clients: MagicMock) -> None:
    """A Marking instance is accepted at creation, not just its RID."""
    marking = Marking._from_proto(mock_clients, _marking(rid="ri.marking.a"))

    client.create_dataset("ds", markings=[marking, "ri.marking.b"])

    request = mock_clients.catalog.create_dataset.call_args.args[1]
    assert request.marking_rids == ["ri.marking.a", "ri.marking.b"]


def test_create_streaming_connection_forwards_markings(client: NominalClient, mock_clients: MagicMock) -> None:
    """Markings reach the connection create request even on the deprecated path."""
    with pytest.warns(DeprecationWarning, match="create_streaming_connection"):
        client.create_streaming_connection("ds-id", "conn", markings=["ri.marking.a"])

    request = mock_clients.connection.create_connection.call_args.args[1]
    assert request.marking_rids == ["ri.marking.a"]
