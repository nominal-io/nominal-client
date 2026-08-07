from nominal_api import api

from nominal.core._utils.query_tools import AssetMatch, create_search_assets_query, create_search_events_query


def test_create_search_events_query_asset_match_all_ands_per_asset_clauses():
    """AssetMatch.ALL (default) emits one ANDed asset clause per rid."""
    query = create_search_events_query(asset_rids=["a", "b"])
    assert [sub.asset for sub in query.and_] == ["a", "b"]


def test_create_search_events_query_asset_match_any_ors_assets():
    """AssetMatch.ANY emits a single OR AssetsFilter over all rids."""
    query = create_search_events_query(asset_rids=["a", "b", "c"], asset_match=AssetMatch.ANY)
    assert len(query.and_) == 1
    assets_filter = query.and_[0].assets
    assert assets_filter.assets == ["a", "b", "c"]
    assert assets_filter.operator == api.SetOperator.OR


def test_create_search_assets_query_wraps_clauses_in_and() -> None:
    """The assets query builds its ANDed clause through a keyword-named proto field that typing cannot check."""
    query = create_search_assets_query(search_text="rover", labels=["fleet"], properties={"site": "a"})

    # `and` is a Python keyword, so it is not reachable as a named attribute.
    clauses = list(getattr(query, "and").queries)
    assert [clause.WhichOneof("query") for clause in clauses] == ["search_text", "label", "property"]
    assert clauses[0].search_text == "rover"
    assert clauses[1].label == "fleet"
    assert (clauses[2].property.name, clauses[2].property.value) == ("site", "a")
