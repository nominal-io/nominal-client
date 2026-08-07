from nominal.core._utils.query_tools import AssetMatch, create_search_events_query
from nominal.protos.event.v2 import event_pb2


def test_create_search_events_query_asset_match_all_ands_per_asset_clauses():
    """AssetMatch.ALL (default) emits one ANDed asset clause per rid."""
    query = create_search_events_query(asset_rids=["a", "b"])

    assert query == event_pb2.SearchQuery(
        **{
            "and": event_pb2.SearchQueryList(
                queries=[event_pb2.SearchQuery(asset="a"), event_pb2.SearchQuery(asset="b")]
            )
        }
    )


def test_create_search_events_query_asset_match_any_ors_assets():
    """AssetMatch.ANY emits a single OR AssetsFilter over all rids."""
    query = create_search_events_query(asset_rids=["a", "b", "c"], asset_match=AssetMatch.ANY)

    assert query == event_pb2.SearchQuery(
        **{
            "and": event_pb2.SearchQueryList(
                queries=[
                    event_pb2.SearchQuery(assets=event_pb2.AssetsFilter(assets=["a", "b", "c"], operator=event_pb2.OR))
                ]
            )
        }
    )
