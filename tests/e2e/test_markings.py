from __future__ import annotations

import pytest

from nominal.core import NominalClient
from nominal.core.dataset import Dataset


def test_search_markings_returns_unarchived_markings(client: NominalClient) -> None:
    """Search succeeds and never returns archived markings, regardless of how many exist."""
    markings = client.search_markings()

    assert all(not marking.is_archived for marking in markings)


def test_search_markings_filters_by_id_substring(client: NominalClient) -> None:
    """The substring filter narrows an unfiltered search rather than widening it."""
    all_markings = client.search_markings()
    if not all_markings:
        pytest.skip("organization has no markings to narrow")

    target = all_markings[0]
    filtered = client.search_markings(id_substring=target.id)

    assert target.rid in {marking.rid for marking in filtered}
    assert len(filtered) <= len(all_markings)


def test_dataset_lists_its_markings(ingested_dataset: Dataset) -> None:
    """A dataset created without markings carries none."""
    assert ingested_dataset.list_markings() == ()
