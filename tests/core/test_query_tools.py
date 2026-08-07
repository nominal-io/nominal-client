from nominal.core._event_types import EventType, SearchEventOriginTypes
from nominal.core._utils.query_tools import AssetMatch, create_search_assets_query, create_search_events_query
from nominal.protos.asset.v2 import asset_pb2
from nominal.protos.event.v2 import event_pb2
from nominal.protos.types import types_pb2


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


def test_create_search_events_query_converts_domain_enums() -> None:
    """event_type and origin_types arrive as domain values and are converted here, not by the caller."""
    query = create_search_events_query(event_type=EventType.FLAG, origin_types=[SearchEventOriginTypes.WORKBOOK])

    assert query == event_pb2.SearchQuery(
        **{
            "and": event_pb2.SearchQueryList(
                queries=[
                    event_pb2.SearchQuery(event_type=event_pb2.FLAG),
                    event_pb2.SearchQuery(
                        origin_types=event_pb2.OriginTypesFilter(
                            operator=event_pb2.OR, origin_types=[event_pb2.WORKBOOK]
                        )
                    ),
                ]
            )
        }
    )


def test_create_search_events_query_drops_an_empty_origin_types() -> None:
    """An empty sequence must drop the clause; an OR over no origin types would match nothing."""
    assert create_search_events_query(origin_types=[]) == event_pb2.SearchQuery(
        **{"and": event_pb2.SearchQueryList(queries=[])}
    )


def test_create_search_assets_query_ands_one_clause_per_filter() -> None:
    """`and` is a Python keyword, so this query is built by keyword expansion rather than a named argument."""
    query = create_search_assets_query(
        search_text="rocket", labels=["flight", "prod"], properties={"vehicle": "v1"}, workspace_rid="ri.workspace.1"
    )

    assert query == asset_pb2.SearchAssetsQuery(
        **{
            "and": asset_pb2.SearchAssetsQueryList(
                queries=[
                    asset_pb2.SearchAssetsQuery(search_text="rocket"),
                    asset_pb2.SearchAssetsQuery(label="flight"),
                    asset_pb2.SearchAssetsQuery(label="prod"),
                    asset_pb2.SearchAssetsQuery(property=types_pb2.Property(name="vehicle", value="v1")),
                    asset_pb2.SearchAssetsQuery(workspace="ri.workspace.1"),
                ]
            )
        }
    )


def test_create_search_assets_query_ands_nothing_when_unfiltered() -> None:
    """An unfiltered search still wraps in `and`: the empty clause list matches every asset."""
    assert create_search_assets_query() == asset_pb2.SearchAssetsQuery(
        **{"and": asset_pb2.SearchAssetsQueryList(queries=[])}
    )
