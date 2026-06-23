import pytest
from nominal_api import api

from nominal.core import ArchiveStatusFilter
from nominal.core._utils.query_tools import AssetMatch, create_search_events_query, resolve_effective_archive_status


def test_resolve_effective_archive_status_prefers_archived_over_include_archived():
    """Legacy archived=True should win over include_archived=True."""
    assert (
        resolve_effective_archive_status(
            archived=True,
            include_archived=True,
        )
        == ArchiveStatusFilter.ARCHIVED
    )


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"archived": True}, ArchiveStatusFilter.ARCHIVED),
        ({"include_archived": True}, ArchiveStatusFilter.ANY),
        ({"archived": False, "include_archived": True}, ArchiveStatusFilter.ANY),
        ({}, ArchiveStatusFilter.NOT_ARCHIVED),
    ],
)
def test_resolve_effective_archive_status_legacy_matrix(kwargs, expected):
    """Legacy archive flags resolve to the intended effective status."""
    assert resolve_effective_archive_status(**kwargs) == expected


def test_resolve_effective_archive_status_rejects_mixing_modern_and_legacy_flags():
    """archive_status cannot be mixed with deprecated archive arguments."""
    with pytest.raises(ValueError, match="Cannot provide `archive_status` alongside deprecated"):
        resolve_effective_archive_status(
            ArchiveStatusFilter.ANY,
            archived=True,
        )


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
