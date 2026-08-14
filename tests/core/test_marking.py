from __future__ import annotations

from unittest.mock import MagicMock

from nominal.core._utils.pagination_tools import search_markings_paginated
from nominal.core._utils.query_tools import create_search_markings_query
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
