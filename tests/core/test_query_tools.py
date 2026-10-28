from typing import Callable

import pytest

from nominal.core import Priority
from nominal.core._event_types import EventType, SearchEventOriginTypes
from nominal.core._utils.query_tools import (
    AssetMatch,
    create_search_assets_query,
    create_search_datasets_query,
    create_search_events_query,
    create_search_runs_query,
)
from nominal.protos.asset.v2 import asset_pb2
from nominal.protos.datasets.v1 import datasets_pb2
from nominal.protos.event.v2 import event_pb2
from nominal.protos.run.v1 import run_service_pb2
from nominal.protos.types import types_pb2

_STRING_PROPERTY = types_pb2.Property(name="serial", value="")
_NUMERIC_EQUALITY = types_pb2.NumericPropertyPredicate(name="count", operator=types_pb2.EQ, value=2.0)


@pytest.mark.parametrize(
    "priority, expected",
    [
        (Priority.P0, event_pb2.P0),
        (Priority.P1, event_pb2.P1),
        (Priority.P2, event_pb2.P2),
        (Priority.P3, event_pb2.P3),
        (Priority.P4, event_pb2.P4),
    ],
)
def test_event_priority_filter(priority: Priority, expected: event_pb2.Priority.ValueType) -> None:
    """Each priority, including Python value zero, becomes its corresponding wire enum."""
    query = create_search_events_query(priority=priority, asset_rids=["asset"])
    assert query == event_pb2.SearchQuery(
        **{
            "and": event_pb2.SearchQueryList(
                queries=[event_pb2.SearchQuery(asset="asset"), event_pb2.SearchQuery(priority=expected)]
            )
        }
    )


def test_event_priority_omitted_adds_no_clause() -> None:
    """Omitting priority leaves the search unrestricted by disposition priority."""
    query = create_search_events_query(asset_rids=["asset"])
    assert query == event_pb2.SearchQuery(
        **{"and": event_pb2.SearchQueryList(queries=[event_pb2.SearchQuery(asset="asset")])}
    )


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


@pytest.mark.parametrize(
    ("build_query", "expected"),
    [
        pytest.param(
            create_search_assets_query,
            asset_pb2.SearchAssetsQuery(
                **{
                    "and": asset_pb2.SearchAssetsQueryList(
                        queries=[
                            asset_pb2.SearchAssetsQuery(property=_STRING_PROPERTY),
                            asset_pb2.SearchAssetsQuery(numeric_property=_NUMERIC_EQUALITY),
                        ]
                    )
                }
            ),
            id="assets",
        ),
        pytest.param(
            create_search_runs_query,
            run_service_pb2.SearchQuery(
                all_of=run_service_pb2.SearchQueryList(
                    queries=[
                        run_service_pb2.SearchQuery(
                            properties=run_service_pb2.PropertiesFilter(name="serial", values=[""])
                        ),
                        run_service_pb2.SearchQuery(numeric_property=_NUMERIC_EQUALITY),
                    ]
                )
            ),
            id="runs",
        ),
        pytest.param(
            create_search_events_query,
            event_pb2.SearchQuery(
                **{
                    "and": event_pb2.SearchQueryList(
                        queries=[
                            event_pb2.SearchQuery(property=_STRING_PROPERTY),
                            event_pb2.SearchQuery(numeric_property=_NUMERIC_EQUALITY),
                        ]
                    )
                }
            ),
            id="events",
        ),
        pytest.param(
            create_search_datasets_query,
            datasets_pb2.DatasetQuery(
                **{
                    "and": datasets_pb2.DatasetQueryList(
                        queries=[
                            datasets_pb2.DatasetQuery(is_archived=False),
                            datasets_pb2.DatasetQuery(property=_STRING_PROPERTY),
                            datasets_pb2.DatasetQuery(numeric_property=_NUMERIC_EQUALITY),
                        ]
                    )
                }
            ),
            id="datasets",
        ),
    ],
)
def test_property_search_matches_strings_and_numbers_by_equality(
    build_query: Callable[..., object], expected: object
) -> None:
    """A string keeps the resource's string filter, and a number (an int here) becomes a numeric equality."""
    assert build_query(properties={"serial": "", "count": 2}) == expected
