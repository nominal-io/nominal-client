from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest

from nominal.core._utils.pagination_tools import search_markings_paginated
from nominal.core._utils.query_tools import create_search_markings_query
from nominal.core.client import NominalClient
from nominal.core.connection import StreamingConnection
from nominal.core.elements import Symbol
from nominal.core.exceptions import NominalNotFoundError, NominalPermissionDeniedError
from nominal.core.marking import (
    MarkableMixin,
    Marking,
    _create_marking,
    _get_marking,
    _search_markings,
)
from nominal.core.video import Video
from nominal.protos.authorization.markings.v1 import markings_pb2


def _metadata(rid: str, id: str = "itar") -> markings_pb2.MarkingMetadata:
    return markings_pb2.MarkingMetadata(rid=rid, id=id, description="", is_archived=False)


def _clients() -> MagicMock:
    clients = MagicMock()
    clients.auth_header = "Bearer test-token"
    return clients


def _marking(rid: str = "ri.marking.a", id: str = "itar") -> markings_pb2.Marking:
    marking = markings_pb2.Marking(rid=rid, id=id, description="controlled", is_archived=False)
    marking.symbol.emoji = ":lock:"
    marking.color.hex_code = "#cc0000"
    marking.created_at.FromNanoseconds(1_000)
    marking.updated_at.FromNanoseconds(2_000)
    return marking


class _Markable(MarkableMixin):
    """Minimal stand-in for a data source, exercising the mixin's own behavior."""

    def __init__(self, rid: str, clients: MagicMock) -> None:
        self.rid = rid
        self._clients = clients


def _applied(clients: MagicMock, resource: str, *marking_rids: str) -> None:
    response = markings_pb2.GetMarkingsForResourcesResponse()
    for marking_rid in marking_rids:
        response.resource_to_markings[resource].applied_markings.add(marking_rid=marking_rid)
    clients.markings.GetMarkingsForResources.return_value = response


def test_empty_query_matches_everything() -> None:
    """No filters is expressed as an empty AND list, which the backend treats as match-all."""
    query = create_search_markings_query()

    assert query.WhichOneof("query") == "and"
    assert list(getattr(query, "and").queries) == []


def test_id_substring_becomes_a_substring_clause() -> None:
    """A substring filter becomes a single id-substring clause inside the AND list."""
    query = create_search_markings_query(id_substring="ita")

    clauses = list(getattr(query, "and").queries)
    assert [c.id_exact_substring_search for c in clauses] == ["ita"]


def test_search_pagination_follows_cursors_until_exhausted() -> None:
    """Pagination feeds each response's token into the next request and stops on an empty token."""
    markings = MagicMock()
    markings.SearchMarkings.side_effect = [
        markings_pb2.SearchMarkingsResponse(marking_metadatas=[_metadata("a")], next_page_token="tok"),
        markings_pb2.SearchMarkingsResponse(marking_metadatas=[_metadata("b")], next_page_token=""),
    ]

    results = list(search_markings_paginated(markings, create_search_markings_query()))

    assert [m.rid for m in results] == ["a", "b"]
    assert markings.SearchMarkings.call_count == 2
    assert markings.SearchMarkings.call_args_list[1].args[0].next_page_token == "tok"


def test_from_proto_reads_both_marking_shapes() -> None:
    """Search returns MarkingMetadata and gets return Marking; one dataclass covers both."""
    clients = _clients()
    metadata = markings_pb2.MarkingMetadata(rid="ri.marking.a", id="itar", description="controlled")
    metadata.symbol.emoji = ":lock:"

    from_full = Marking._from_proto(clients, _marking())
    from_metadata = Marking._from_proto(clients, metadata)

    assert from_full.id == from_metadata.id == "itar"
    assert from_full.symbol == from_metadata.symbol == Symbol.emoji(":lock:")
    assert from_full.color == "#cc0000"


def test_create_rejects_ids_the_server_would_reject() -> None:
    """Guard client-side, before the RPC: users cannot see the backend's validation rule."""
    clients = _clients()

    with pytest.raises(ValueError, match="lowercase"):
        _create_marking(clients, id="ITAR", description=None, authorized_groups=(), symbol=None, color=None)

    clients.markings.CreateMarking.assert_not_called()


def test_create_sends_symbol_and_color() -> None:
    """Create forwards id, symbol, color, and authorized groups, and returns the created marking."""
    clients = _clients()
    clients.markings.CreateMarking.return_value = markings_pb2.CreateMarkingResponse(marking=_marking())

    marking = _create_marking(
        clients,
        id="itar",
        description="controlled",
        authorized_groups=["ri.group.a"],
        symbol=Symbol.emoji(":lock:"),
        color="#cc0000",
    )

    request = clients.markings.CreateMarking.call_args.args[0]
    assert request.id == "itar"
    assert request.symbol.emoji == ":lock:"
    assert request.color.hex_code == "#cc0000"
    assert list(request.authorized_groups.group_rids) == ["ri.group.a"]
    assert marking.rid == "ri.marking.a"


def test_create_lowercases_an_uppercase_color() -> None:
    """Either case is accepted at the boundary; the wire value is always lowercase."""
    clients = _clients()
    clients.markings.CreateMarking.return_value = markings_pb2.CreateMarkingResponse(marking=_marking())

    _create_marking(clients, id="itar", description=None, authorized_groups=[], symbol=None, color="#CC0000")

    assert clients.markings.CreateMarking.call_args.args[0].color.hex_code == "#cc0000"


def test_get_marking_fetches_by_rid() -> None:
    """A single get, not a one-element batch: the service distinguishes missing from unreadable."""
    clients = _clients()
    clients.markings.GetMarking.return_value = markings_pb2.GetMarkingResponse(marking=_marking())

    marking = _get_marking(clients, "ri.marking.a")

    assert marking.rid == "ri.marking.a"
    assert clients.markings.GetMarking.call_args.args[0].rid == "ri.marking.a"


def test_get_marking_surfaces_a_missing_marking_as_not_found(fake_rpc_error) -> None:
    """A NOT_FOUND from the service is translated rather than leaking the raw RpcError."""
    clients = _clients()
    clients.markings.GetMarking.side_effect = fake_rpc_error(grpc.StatusCode.NOT_FOUND)

    with pytest.raises(NominalNotFoundError):
        _get_marking(clients, "ri.marking.missing")


def test_get_marking_surfaces_an_unreadable_marking_as_permission_denied(fake_rpc_error) -> None:
    """Distinct from not-found: a batch get would have conflated the two."""
    clients = _clients()
    clients.markings.GetMarking.side_effect = fake_rpc_error(grpc.StatusCode.PERMISSION_DENIED)

    with pytest.raises(NominalPermissionDeniedError):
        _get_marking(clients, "ri.marking.hidden")


def test_update_distinguishes_unchanged_from_cleared_symbol() -> None:
    """Omitting symbol leaves it alone; passing None clears it. The wrapper encodes that difference."""
    clients = _clients()
    clients.markings.UpdateMarking.return_value = markings_pb2.UpdateMarkingResponse(marking=_marking())
    marking = Marking._from_proto(clients, _marking())

    marking.update(description="new")
    unchanged = clients.markings.UpdateMarking.call_args.args[0]
    assert not unchanged.HasField("symbol")
    assert unchanged.description == "new"

    marking.update(symbol=None)
    cleared = clients.markings.UpdateMarking.call_args.args[0]
    assert cleared.HasField("symbol")
    assert not cleared.symbol.HasField("value")

    marking.update(symbol=Symbol.icon("castle"))
    assigned = clients.markings.UpdateMarking.call_args.args[0]
    assert assigned.symbol.value.icon == "castle"


def test_update_clears_authorized_groups_with_an_empty_sequence() -> None:
    """None leaves groups alone; an empty sequence clears them."""
    clients = _clients()
    clients.markings.UpdateMarking.return_value = markings_pb2.UpdateMarkingResponse(marking=_marking())
    marking = Marking._from_proto(clients, _marking())

    marking.update(description="x")
    assert not clients.markings.UpdateMarking.call_args.args[0].HasField("authorized_groups")

    marking.update(authorized_groups=[])
    cleared = clients.markings.UpdateMarking.call_args.args[0]
    assert cleared.HasField("authorized_groups")
    assert list(cleared.authorized_groups.group_rids) == []


def test_search_returns_markings_across_pages() -> None:
    """Search concatenates every page and applies the substring filter to the first request."""
    clients = _clients()
    clients.markings.SearchMarkings.side_effect = [
        markings_pb2.SearchMarkingsResponse(marking_metadatas=[_metadata("a")], next_page_token="tok"),
        markings_pb2.SearchMarkingsResponse(marking_metadatas=[_metadata("b")], next_page_token=""),
    ]

    results = _search_markings(clients, id_substring="ita")

    assert [m.rid for m in results] == ["a", "b"]
    query = clients.markings.SearchMarkings.call_args_list[0].args[0].query
    assert [c.id_exact_substring_search for c in getattr(query, "and").queries] == ["ita"]


def test_authorized_groups_reads_this_markings_entry() -> None:
    """The batch response is indexed by this marking's own rid rather than taking the first entry."""
    clients = _clients()
    marking = Marking._from_proto(clients, _marking())
    response = markings_pb2.GetAuthorizedGroupsByMarkingResponse()
    response.authorized_groups_by_marking["ri.marking.a"].group_rids.append("ri.group.a")
    clients.markings.GetAuthorizedGroupsByMarking.return_value = response

    assert marking.authorized_group_rids() == ("ri.group.a",)


def test_list_markings_hydrates_applied_rids() -> None:
    """Applied rids are resolved to full markings via a batch metadata get."""
    clients = _clients()
    _applied(clients, "ri.dataset.a", "ri.marking.a")
    clients.markings.BatchGetMarkingMetadata.return_value = markings_pb2.BatchGetMarkingMetadataResponse(
        marking_metadatas=[_metadata("ri.marking.a", id="itar")]
    )

    markings = _Markable("ri.dataset.a", clients).list_markings()

    assert [m.id for m in markings] == ["itar"]
    assert list(clients.markings.BatchGetMarkingMetadata.call_args.args[0].marking_rids) == ["ri.marking.a"]


def test_list_markings_on_unmarked_resource_is_empty_without_a_second_call() -> None:
    """With no applied markings the batch get is skipped entirely."""
    clients = _clients()
    _applied(clients, "ri.dataset.a")

    assert _Markable("ri.dataset.a", clients).list_markings() == ()
    clients.markings.BatchGetMarkingMetadata.assert_not_called()


def test_apply_and_remove_send_one_sided_updates() -> None:
    """Apply and remove each populate only their own side of the update request."""
    clients = _clients()
    markable = _Markable("ri.dataset.a", clients)

    markable.apply_markings(["ri.marking.a"])
    applied = clients.markings.UpdateMarkingsOnResource.call_args.args[0]
    assert applied.resource == "ri.dataset.a"
    assert list(applied.markings_to_apply) == ["ri.marking.a"]
    assert list(applied.markings_to_remove) == []

    markable.remove_markings(["ri.marking.a"])
    removed = clients.markings.UpdateMarkingsOnResource.call_args.args[0]
    assert list(removed.markings_to_apply) == []
    assert list(removed.markings_to_remove) == ["ri.marking.a"]


def test_set_markings_sends_the_diff_in_one_call() -> None:
    """Replacing the set adds what is missing and removes what is no longer wanted, atomically."""
    clients = _clients()
    _applied(clients, "ri.dataset.a", "ri.marking.keep", "ri.marking.drop")

    _Markable("ri.dataset.a", clients).set_markings(["ri.marking.keep", "ri.marking.add"])

    assert clients.markings.UpdateMarkingsOnResource.call_count == 1
    request = clients.markings.UpdateMarkingsOnResource.call_args.args[0]
    assert sorted(request.markings_to_apply) == ["ri.marking.add"]
    assert sorted(request.markings_to_remove) == ["ri.marking.drop"]


def test_set_markings_skips_the_call_when_nothing_changes() -> None:
    """An unchanged set sends no update request at all."""
    clients = _clients()
    _applied(clients, "ri.dataset.a", "ri.marking.keep")

    _Markable("ri.dataset.a", clients).set_markings(["ri.marking.keep"])

    clients.markings.UpdateMarkingsOnResource.assert_not_called()


def test_markings_accept_instances_as_well_as_rids() -> None:
    """A Marking instance is coerced to its rid on the wire."""
    clients = _clients()
    marking = Marking._from_proto(clients, _marking(rid="ri.marking.a"))

    _Markable("ri.dataset.a", clients).apply_markings([marking])

    request = clients.markings.UpdateMarkingsOnResource.call_args.args[0]
    assert list(request.markings_to_apply) == ["ri.marking.a"]


def test_video_lists_its_markings() -> None:
    """Pins that `Video` is actually wired to `MarkableMixin`: it does not inherit `DataSource`, so
    nothing else in the library would notice if that base were dropped.
    """
    clients = _clients()
    _applied(clients, "ri.video.a", "ri.marking.a")
    clients.markings.BatchGetMarkingMetadata.return_value = markings_pb2.BatchGetMarkingMetadataResponse(
        marking_metadatas=[_metadata("ri.marking.a", id="itar")]
    )
    video = Video(
        rid="ri.video.a",
        name="v",
        description=None,
        properties={},
        labels=[],
        created_at=0,
        is_archived=False,
        _clients=clients,
    )

    assert [m.id for m in video.list_markings()] == ["itar"]


def test_streaming_connection_lists_its_markings() -> None:
    """Pins that `DataSource` is actually wired to `MarkableMixin`, covering `StreamingConnection` (and by
    inheritance `Dataset`/`Connection`) via the real class rather than the `_Markable` stand-in.
    """
    clients = _clients()
    _applied(clients, "ri.connection.a", "ri.marking.a")
    clients.markings.BatchGetMarkingMetadata.return_value = markings_pb2.BatchGetMarkingMetadataResponse(
        marking_metadatas=[_metadata("ri.marking.a", id="itar")]
    )
    connection = StreamingConnection(
        rid="ri.connection.a",
        name="c",
        description=None,
        _clients=clients,
        nominal_data_source_rid="ri.connection.a",
    )

    assert [m.id for m in connection.list_markings()] == ["itar"]


def test_client_search_markings_passes_the_substring_through() -> None:
    """The client method forwards its substring to the query and returns hydrated markings."""
    clients = _clients()
    clients.markings.SearchMarkings.return_value = markings_pb2.SearchMarkingsResponse(
        marking_metadatas=[_metadata("ri.marking.a")], next_page_token=""
    )
    client = NominalClient(_clients=clients)

    results = client.search_markings(id_substring="ita")

    assert [m.rid for m in results] == ["ri.marking.a"]
    query = clients.markings.SearchMarkings.call_args.args[0].query
    assert [c.id_exact_substring_search for c in getattr(query, "and").queries] == ["ita"]


def test_client_create_marking_returns_the_created_marking() -> None:
    """The client method validates and forwards color, returning the created marking."""
    clients = _clients()
    clients.markings.CreateMarking.return_value = markings_pb2.CreateMarkingResponse(marking=_marking())
    client = NominalClient(_clients=clients)

    marking = client.create_marking("itar", description="controlled", color="#cc0000")

    assert marking.id == "itar"
    assert clients.markings.CreateMarking.call_args.args[0].color.hex_code == "#cc0000"


def test_create_dataset_forwards_marking_rids() -> None:
    """Markings may be given as instances or as RIDs."""
    clients = _clients()
    client = NominalClient(_clients=clients)
    marking = Marking._from_proto(clients, _marking(rid="ri.marking.a"))

    client.create_dataset("ds", markings=[marking, "ri.marking.b"])

    request = clients.catalog.create_dataset.call_args.args[1]
    assert request.marking_rids == ["ri.marking.a", "ri.marking.b"]


def test_create_dataset_without_markings_sends_an_empty_list() -> None:
    """Omitting markings sends an empty list, not null, to the catalog."""
    clients = _clients()
    NominalClient(_clients=clients).create_dataset("ds")

    assert clients.catalog.create_dataset.call_args.args[1].marking_rids == []


def test_create_streaming_connection_forwards_marking_rids() -> None:
    """Markings reach the connection create request even on the deprecated path."""
    clients = _clients()
    with pytest.warns(DeprecationWarning, match="create_streaming_connection"):
        NominalClient(_clients=clients).create_streaming_connection("ds-id", "conn", markings=["ri.marking.a"])

    request = clients.connection.create_connection.call_args.args[1]
    assert request.marking_rids == ["ri.marking.a"]
