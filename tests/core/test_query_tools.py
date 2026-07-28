from nominal_api import api

from nominal.core._utils.query_tools import AssetMatch, create_search_events_query


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
