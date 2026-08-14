from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core._utils.pagination_tools import search_markings_paginated
from nominal.core._utils.query_tools import create_search_markings_query
from nominal.core.elements import Color, Symbol
from nominal.core.marking import (
    MarkableMixin,
    Marking,
    _create_marking,
    _get_marking,
    _search_markings,
)
from nominal.protos.authorization.markings.v1 import markings_pb2


def _metadata(rid: str, id: str = "itar") -> markings_pb2.MarkingMetadata:
    return markings_pb2.MarkingMetadata(rid=rid, id=id, description="", is_archived=False)


def test_empty_query_matches_everything() -> None:
    """No filters is expressed as an empty AND list, which the backend treats as match-all."""
    query = create_search_markings_query()

    assert query.WhichOneof("query") == "and"
    assert list(getattr(query, "and").queries) == []


def test_id_substring_becomes_a_substring_clause() -> None:
    query = create_search_markings_query(id_substring="ita")

    clauses = list(getattr(query, "and").queries)
    assert [c.id_exact_substring_search for c in clauses] == ["ita"]


def test_search_pagination_follows_cursors_until_exhausted() -> None:
    markings = MagicMock()
    markings.SearchMarkings.side_effect = [
        markings_pb2.SearchMarkingsResponse(marking_metadatas=[_metadata("a")], next_page_token="tok"),
        markings_pb2.SearchMarkingsResponse(marking_metadatas=[_metadata("b")], next_page_token=""),
    ]

    results = list(search_markings_paginated(markings, create_search_markings_query()))

    assert [m.rid for m in results] == ["a", "b"]
    assert markings.SearchMarkings.call_count == 2
    assert markings.SearchMarkings.call_args_list[1].args[0].next_page_token == "tok"


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


def test_from_proto_reads_both_marking_shapes() -> None:
    """Search returns MarkingMetadata and gets return Marking; one dataclass covers both."""
    clients = _clients()
    metadata = markings_pb2.MarkingMetadata(rid="ri.marking.a", id="itar", description="controlled")
    metadata.symbol.emoji = ":lock:"

    from_full = Marking._from_proto(clients, _marking())
    from_metadata = Marking._from_proto(clients, metadata)

    assert from_full.id == from_metadata.id == "itar"
    assert from_full.symbol == from_metadata.symbol == Symbol.emoji(":lock:")
    assert from_full.color == Color("#cc0000")


def test_create_rejects_ids_the_server_would_reject() -> None:
    """Guard client-side, before the RPC: users cannot see the backend's validation rule."""
    clients = _clients()

    with pytest.raises(ValueError, match="lowercase"):
        _create_marking(clients, id="ITAR", description=None, authorized_group_rids=(), symbol=None, color=None)

    clients.markings.CreateMarking.assert_not_called()


def test_create_sends_symbol_and_color() -> None:
    clients = _clients()
    clients.markings.CreateMarking.return_value = markings_pb2.CreateMarkingResponse(marking=_marking())

    marking = _create_marking(
        clients,
        id="itar",
        description="controlled",
        authorized_group_rids=["ri.group.a"],
        symbol=Symbol.emoji(":lock:"),
        color=Color("#cc0000"),
    )

    request = clients.markings.CreateMarking.call_args.args[0]
    assert request.id == "itar"
    assert request.symbol.emoji == ":lock:"
    assert request.color.hex_code == "#cc0000"
    assert list(request.authorized_groups.group_rids) == ["ri.group.a"]
    assert marking.rid == "ri.marking.a"


def test_get_marking_raises_when_absent() -> None:
    """BatchGetMarkings filters out markings the user cannot see, so an empty result means not found."""
    clients = _clients()
    clients.markings.BatchGetMarkings.return_value = markings_pb2.BatchGetMarkingsResponse(markings=[])

    from nominal.core.exceptions import NominalNotFoundError

    with pytest.raises(NominalNotFoundError):
        _get_marking(clients, "ri.marking.missing")


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

    marking.update(authorized_group_rids=[])
    cleared = clients.markings.UpdateMarking.call_args.args[0]
    assert cleared.HasField("authorized_groups")
    assert list(cleared.authorized_groups.group_rids) == []


def test_search_returns_markings_across_pages() -> None:
    clients = _clients()
    clients.markings.SearchMarkings.side_effect = [
        markings_pb2.SearchMarkingsResponse(marking_metadatas=[_metadata("a")], next_page_token="tok"),
        markings_pb2.SearchMarkingsResponse(marking_metadatas=[_metadata("b")], next_page_token=""),
    ]

    results = _search_markings(clients, id_substring="ita")

    assert [m.rid for m in results] == ["a", "b"]
    query = clients.markings.SearchMarkings.call_args_list[0].args[0].query
    assert [c.id_exact_substring_search for c in getattr(query, "and").queries] == ["ita"]


def test_authorized_group_rids_reads_this_markings_entry() -> None:
    clients = _clients()
    marking = Marking._from_proto(clients, _marking())
    response = markings_pb2.GetAuthorizedGroupsByMarkingResponse()
    response.authorized_groups_by_marking["ri.marking.a"].group_rids.append("ri.group.a")
    clients.markings.GetAuthorizedGroupsByMarking.return_value = response

    assert marking.authorized_group_rids() == ("ri.group.a",)


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


def test_list_markings_hydrates_applied_rids() -> None:
    clients = _clients()
    _applied(clients, "ri.dataset.a", "ri.marking.a")
    clients.markings.BatchGetMarkingMetadata.return_value = markings_pb2.BatchGetMarkingMetadataResponse(
        marking_metadatas=[_metadata("ri.marking.a", id="itar")]
    )

    markings = _Markable("ri.dataset.a", clients).list_markings()

    assert [m.id for m in markings] == ["itar"]
    assert list(clients.markings.BatchGetMarkingMetadata.call_args.args[0].marking_rids) == ["ri.marking.a"]


def test_list_markings_on_unmarked_resource_is_empty_without_a_second_call() -> None:
    clients = _clients()
    _applied(clients, "ri.dataset.a")

    assert _Markable("ri.dataset.a", clients).list_markings() == ()
    clients.markings.BatchGetMarkingMetadata.assert_not_called()


def test_apply_and_remove_send_one_sided_updates() -> None:
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
    clients = _clients()
    _applied(clients, "ri.dataset.a", "ri.marking.keep")

    _Markable("ri.dataset.a", clients).set_markings(["ri.marking.keep"])

    clients.markings.UpdateMarkingsOnResource.assert_not_called()


def test_markings_accept_instances_as_well_as_rids() -> None:
    clients = _clients()
    marking = Marking._from_proto(clients, _marking(rid="ri.marking.a"))

    _Markable("ri.dataset.a", clients).apply_markings([marking])

    request = clients.markings.UpdateMarkingsOnResource.call_args.args[0]
    assert list(request.markings_to_apply) == ["ri.marking.a"]


def test_client_search_markings_passes_the_substring_through() -> None:
    from nominal.core.client import NominalClient

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
    from nominal.core.client import NominalClient

    clients = _clients()
    clients.markings.CreateMarking.return_value = markings_pb2.CreateMarkingResponse(marking=_marking())
    client = NominalClient(_clients=clients)

    marking = client.create_marking("itar", description="controlled", color=Color("#cc0000"))

    assert marking.id == "itar"
    assert clients.markings.CreateMarking.call_args.args[0].color.hex_code == "#cc0000"
