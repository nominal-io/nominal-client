from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from nominal.core._utils.query_tools import ArchiveStatusFilter, create_search_ingest_jobs_query
from nominal.core.client import NominalClient
from nominal.core.ingestion_job import IngestionJobStatus
from nominal.ts import _SecondsNanos


def test_search_data_reviews_passes_archive_status():
    """NominalClient.search_data_reviews forwards archive_status to the shared data-review iterator."""
    client = NominalClient(_clients=MagicMock())

    with patch("nominal.core.client._iter_search_data_reviews", return_value=iter(())) as mock_reviews:
        result = client.search_data_reviews(archive_status=ArchiveStatusFilter.ANY)

    assert result == []
    mock_reviews.assert_called_once()
    assert mock_reviews.call_args.kwargs["archive_status"] == ArchiveStatusFilter.ANY


def test_create_search_ingest_jobs_query_empty_returns_empty_and():
    """With no filters, the query is an empty AND (match-all)."""
    result = create_search_ingest_jobs_query()
    assert result.and_ == []


def test_create_search_ingest_jobs_query_single_leaf_wrapped_in_and():
    """A single filter is wrapped in a one-element AND."""
    result = create_search_ingest_jobs_query(datasets=["ri.catalog.test.dataset.a"])
    assert len(result.and_) == 1
    assert result.and_[0].dataset_rids == ["ri.catalog.test.dataset.a"]


def test_create_search_ingest_jobs_query_multiple_leaves_are_anded():
    """Multiple filters are AND-composed, with statuses converted to wire enums."""
    result = create_search_ingest_jobs_query(
        datasets=["ri.catalog.test.dataset.a"],
        statuses=[IngestionJobStatus.COMPLETED],
    )
    assert len(result.and_) == 2
    assert result.and_[0].dataset_rids == ["ri.catalog.test.dataset.a"]
    assert [s.name for s in result.and_[1].statuses] == ["COMPLETED"]


def test_create_search_ingest_jobs_query_start_time_after_sets_range():
    """A start-time bound becomes a single start_time_range leaf with an ISO-8601 timestamp."""
    result = create_search_ingest_jobs_query(start_time_after="2026-06-25T00:00:00Z")
    expected = _SecondsNanos.from_flexible("2026-06-25T00:00:00Z").to_iso8601()
    assert len(result.and_) == 1
    assert result.and_[0].start_time_range is not None
    assert result.and_[0].start_time_range.start_time_after == expected
    assert result.and_[0].start_time_range.start_time_before is None


def test_create_search_ingest_jobs_query_workspace_rid_sets_workspace():
    """A workspace RID becomes a single workspace filter leaf."""
    result = create_search_ingest_jobs_query(workspace_rid="ri.workspace.test.w")
    assert len(result.and_) == 1
    assert result.and_[0].workspace == "ri.workspace.test.w"


def test_search_ingestion_jobs_resolves_and_applies_workspace_filter():
    """search_ingestion_jobs resolves the workspace selector and forwards it as a workspace filter."""
    client = NominalClient(_clients=MagicMock())
    client._clients.resolve_workspace.return_value.rid = "ri.workspace.resolved"
    client._clients.ingest_jobs.search_ingest_jobs.return_value = SimpleNamespace(ingest_jobs=[], next_page_token=None)

    client.search_ingestion_jobs(workspace="ri.workspace.input")

    client._clients.resolve_workspace.assert_called_once_with("ri.workspace.input")
    request = client._clients.ingest_jobs.search_ingest_jobs.call_args.args[1]
    assert len(request.filter.and_) == 1
    assert request.filter.and_[0].workspace == "ri.workspace.resolved"


def test_search_ingestion_jobs_forwards_filter_to_the_search_endpoint():
    """search_ingestion_jobs builds an AND filter from its kwargs and forwards it to the search endpoint."""
    client = NominalClient(_clients=MagicMock())
    client._clients.ingest_jobs.search_ingest_jobs.return_value = SimpleNamespace(ingest_jobs=[], next_page_token=None)

    result = client.search_ingestion_jobs(statuses=[IngestionJobStatus.FAILED])

    assert result == []
    request = client._clients.ingest_jobs.search_ingest_jobs.call_args.args[1]
    assert [s.name for s in request.filter.and_[0].statuses] == ["FAILED"]
