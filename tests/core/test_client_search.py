from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from nominal.core._utils.query_tools import (
    ArchiveStatusFilter,
    create_search_ingest_jobs_query,
    create_search_users_query,
)
from nominal.core.client import NominalClient
from nominal.core.ingestion_job import IngestionJobStatus
from nominal.core.user import User
from nominal.protos.authentication.users.v1 import users_pb2
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


def test_search_users_follows_pagination_cursors_and_ands_filters() -> None:
    """search_users accumulates users across pages, ANDing exact_match (an email match) with search_text."""
    clients = MagicMock()
    client = NominalClient(_clients=clients)
    clients.users.SearchUsers.side_effect = [
        users_pb2.SearchUsersResponse(
            users=[users_pb2.User(rid="ri.authn.user.a", display_name="A", email="a@example.com")],
            next_page_token="tok",
        ),
        users_pb2.SearchUsersResponse(
            users=[users_pb2.User(rid="ri.authn.user.b", display_name="B", email="b@example.com")],
        ),
    ]

    results = client.search_users(exact_match="example.com", search_text="a")

    assert results == [
        User(rid="ri.authn.user.a", display_name="A", email="a@example.com"),
        User(rid="ri.authn.user.b", display_name="B", email="b@example.com"),
    ]
    first_request, second_request = (call.args[0] for call in clients.users.SearchUsers.call_args_list)
    assert first_request.page_token == ""
    assert first_request.sort == users_pb2.UserSort(field=users_pb2.USER_SORT_FIELD_EMAIL, is_descending=False)
    assert first_request.query == users_pb2.SearchUsersQuery(
        **{
            "and": users_pb2.SearchUsersQueries(
                queries=[
                    users_pb2.SearchUsersQuery(exact_email="example.com"),
                    users_pb2.SearchUsersQuery(search_text="a"),
                ]
            )
        }
    )
    assert second_request.page_token == "tok"


def test_create_search_users_query_without_filters_sets_an_empty_and() -> None:
    """Without filters the query still sets its match-all `and` arm, since the service rejects an unset query."""
    query = create_search_users_query()

    assert query.WhichOneof("query") == "and"
    assert list(getattr(query, "and").queries) == []
