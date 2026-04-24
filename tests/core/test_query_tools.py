import pytest

from nominal.core import ArchiveStatusFilter
from nominal.core._utils.query_tools import (
    create_search_assets_query,
    create_search_datasets_query,
    create_search_runs_query,
    create_search_users_query,
    create_search_workbook_templates_query,
    create_search_workbooks_query,
    resolve_effective_archive_status,
)


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

