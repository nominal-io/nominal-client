import pytest

from nominal.core import ArchiveStatusFilter
from nominal.core._utils.query_tools import resolve_effective_archive_status


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
