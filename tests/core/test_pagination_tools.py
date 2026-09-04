from __future__ import annotations

from unittest.mock import MagicMock

from nominal_api import ingest_api

from nominal.core._utils.pagination_tools import DEFAULT_PAGE_SIZE, search_ingest_jobs_paginated


def test_search_ingest_jobs_paginated_walks_all_pages():
    """Walks every page, threading the token and building requests with the page size and descending CREATED_AT sort."""
    client = MagicMock()
    page_one = MagicMock(ingest_jobs=["job-a"], next_page_token="token-2")
    page_two = MagicMock(ingest_jobs=["job-b"], next_page_token=None)
    client.search_ingest_jobs.side_effect = [page_one, page_two]

    sentinel_filter = MagicMock()
    results = list(search_ingest_jobs_paginated(client, "Bearer token", filter=sentinel_filter))

    assert results == ["job-a", "job-b"]
    assert client.search_ingest_jobs.call_count == 2
    # First request carries the page size, filter, and descending CREATED_AT sort.
    first_request = client.search_ingest_jobs.call_args_list[0].args[1]
    assert first_request.page_size == DEFAULT_PAGE_SIZE == 100
    assert first_request.filter is sentinel_filter
    assert first_request.sort.is_descending is True
    assert first_request.sort.sort_key is ingest_api.IngestJobSortKey.CREATED_AT
    # Second request carries the token returned by the first page.
    second_request = client.search_ingest_jobs.call_args_list[1].args[1]
    assert second_request.next_page_token == "token-2"
