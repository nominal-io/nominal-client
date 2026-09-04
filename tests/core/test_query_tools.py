from nominal.core._event_types import EventType, SearchEventOriginTypes
from nominal.core._utils.query_tools import (
    AssetMatch,
    create_search_assets_query,
    create_search_datasets_query,
    create_search_events_query,
    create_search_runs_query,
    create_search_users_query,
    create_search_workbook_templates_query,
    create_search_workbooks_query,
)
from nominal.protos.event.v2 import event_pb2


def test_create_search_users_query_uses_substring_match_for_exact_match_backend_field():
    query = create_search_users_query(substring_match="alex")

    assert query.and_ is not None
    assert query.and_[0].exact_match == "alex"


def test_create_search_assets_query_uses_substring_match_for_exact_substring_backend_field():
    query = create_search_assets_query(substring_match="vehicle")

    assert query.and_ is not None
    assert query.and_[0].exact_substring == "vehicle"


def test_create_search_datasets_query_uses_substring_match_for_exact_match_backend_field():
    query = create_search_datasets_query(substring_match="flight")

    assert query.and_ is not None
    assert query.and_[1].exact_match == "flight"


def test_create_search_runs_query_uses_substring_match_for_exact_match_backend_field():
    query = create_search_runs_query(substring_match="hotfire")

    assert query.and_ is not None
    assert query.and_[0].exact_match == "hotfire"


def test_create_search_workbooks_query_uses_substring_match_for_exact_match_backend_field():
    query = create_search_workbooks_query(substring_match="analysis")

    assert query.and_ is not None
    assert query.and_[2].exact_match == "analysis"


def test_create_search_workbook_templates_query_uses_substring_match_for_exact_match_backend_field():
    query = create_search_workbook_templates_query(substring_match="template")

    assert query.and_ is not None
    assert query.and_[1].exact_match == "template"


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
