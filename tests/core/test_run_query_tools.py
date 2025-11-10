"""Unit tests for run query building with new TimeframeFilter and created_at support."""

from datetime import datetime, timedelta, timezone

from nominal.core._utils.query_tools import create_search_runs_query
from nominal.ts import _SecondsNanos


def test_create_search_runs_query_basic():
    """Test basic query creation without filters."""
    query = create_search_runs_query()

    # Should return an empty AND query
    assert hasattr(query, "and_")
    assert query.and_ == []


def test_create_search_runs_query_with_start_time():
    """Test query creation with start time filter using TimeframeFilter."""
    start_time = datetime(2024, 1, 1, 12, 0, 0)
    query = create_search_runs_query(start=start_time)

    assert len(query.and_) == 1
    sub_query = query.and_[0]

    # Should use startTime with CustomTimeframeFilter(start_time=...)
    assert hasattr(sub_query, "start_time")
    assert sub_query.start_time is not None
    assert hasattr(sub_query.start_time, "custom")
    assert sub_query.start_time.custom is not None
    assert sub_query.start_time.custom.start_time == _SecondsNanos.from_datetime(start_time).to_scout_run_api()
    assert sub_query.start_time.custom.end_time is None


def test_create_search_runs_query_with_end_time():
    """Test query creation with end time filter using TimeframeFilter."""
    end_time = datetime(2024, 12, 31, 23, 59, 59)
    query = create_search_runs_query(end=end_time)

    assert len(query.and_) == 1
    sub_query = query.and_[0]

    # Should use endTime with CustomTimeframeFilter(end_time=...)
    assert hasattr(sub_query, "end_time")
    assert sub_query.end_time is not None
    assert hasattr(sub_query.end_time, "custom")
    assert sub_query.end_time.custom is not None
    assert sub_query.end_time.custom.start_time is None
    assert sub_query.end_time.custom.end_time == _SecondsNanos.from_datetime(end_time).to_scout_run_api()


def test_create_search_runs_query_with_created_after():
    """Test query creation with created_after filter."""
    created_after = datetime(2024, 6, 1, 0, 0, 0)
    query = create_search_runs_query(created_after=created_after)

    assert len(query.and_) == 1
    sub_query = query.and_[0]

    # Should use createdAt with CustomTimeframeFilter(start_time=...)
    assert hasattr(sub_query, "created_at")
    assert sub_query.created_at is not None
    assert hasattr(sub_query.created_at, "custom")
    assert sub_query.created_at.custom is not None
    assert sub_query.created_at.custom.start_time == _SecondsNanos.from_datetime(created_after).to_scout_run_api()
    assert sub_query.created_at.custom.end_time is None


def test_create_search_runs_query_with_created_before():
    """Test query creation with created_before filter."""
    created_before = datetime(2024, 6, 30, 23, 59, 59)
    query = create_search_runs_query(created_before=created_before)

    assert len(query.and_) == 1
    sub_query = query.and_[0]

    # Should use createdAt with CustomTimeframeFilter(end_time=...)
    assert hasattr(sub_query, "created_at")
    assert sub_query.created_at is not None
    assert hasattr(sub_query.created_at, "custom")
    assert sub_query.created_at.custom is not None
    assert sub_query.created_at.custom.start_time is None
    assert sub_query.created_at.custom.end_time == _SecondsNanos.from_datetime(created_before).to_scout_run_api()


def test_create_search_runs_query_with_both_created_filters():
    """Test query creation with both created_after and created_before filters."""
    created_after = datetime(2024, 6, 1, 0, 0, 0)
    created_before = datetime(2024, 6, 30, 23, 59, 59)

    query = create_search_runs_query(created_after=created_after, created_before=created_before)

    assert len(query.and_) == 1
    sub_query = query.and_[0]

    # Should have one createdAt filter with both after and before
    assert hasattr(sub_query, "created_at")
    assert sub_query.created_at is not None
    assert hasattr(sub_query.created_at, "custom")
    assert sub_query.created_at.custom is not None
    assert sub_query.created_at.custom.start_time == _SecondsNanos.from_datetime(created_after).to_scout_run_api()
    assert sub_query.created_at.custom.end_time == _SecondsNanos.from_datetime(created_before).to_scout_run_api()


def test_create_search_runs_query_with_string_timestamps():
    """Test query creation with ISO 8601 string timestamps."""
    start_str = "2024-01-01T00:00:00Z"
    end_str = "2024-12-31T23:59:59Z"
    created_after_str = "2024-06-01T00:00:00Z"
    created_before_str = "2024-06-30T23:59:59Z"

    query = create_search_runs_query(
        start=start_str, end=end_str, created_after=created_after_str, created_before=created_before_str
    )

    # Should have 3 filters: start_time, end_time, created_at
    assert len(query.and_) == 3


def test_create_search_runs_query_with_nanosecond_timestamps():
    """Test query creation with nanosecond integer timestamps."""
    now = datetime.now(timezone.utc)
    start_ns = _SecondsNanos.from_datetime(now).to_nanoseconds()
    end_ns = _SecondsNanos.from_datetime(now + timedelta(hours=1)).to_nanoseconds()
    created_after_ns = _SecondsNanos.from_datetime(now - timedelta(days=1)).to_nanoseconds()

    query = create_search_runs_query(start=start_ns, end=end_ns, created_after=created_after_ns)

    # Should have 3 filters
    assert len(query.and_) == 3


def test_create_search_runs_query_with_labels():
    """Test query creation with labels filter."""
    labels = ["test", "automated", "production"]
    query = create_search_runs_query(labels=labels)

    assert len(query.and_) == 1
    sub_query = query.and_[0]

    # Should use labels filter (not individual label filters)
    assert hasattr(sub_query, "labels")
    assert sub_query.labels is not None


def test_create_search_runs_query_with_properties():
    """Test query creation with properties filter."""
    properties = {"env": "production", "version": "1.0", "region": "us-east"}
    query = create_search_runs_query(properties=properties)

    assert len(query.and_) == 1
    sub_query = query.and_[0]

    # Should use properties filter (not individual property filters)
    assert hasattr(sub_query, "properties")
    assert sub_query.properties is not None
    assert len(sub_query.properties) == 3


def test_create_search_runs_query_with_name_substring():
    """Test query creation with name_substring filter."""
    name_substring = "test-run"
    query = create_search_runs_query(name_substring=name_substring)

    assert len(query.and_) == 1
    sub_query = query.and_[0]

    # Should use exact_match for name_substring
    assert hasattr(sub_query, "exact_match")
    assert sub_query.exact_match == name_substring


def test_create_search_runs_query_with_exact_match():
    """Test query creation with exact_match filter."""
    exact_match = "Run 12345"
    query = create_search_runs_query(exact_match=exact_match)

    assert len(query.and_) == 1
    sub_query = query.and_[0]

    assert hasattr(sub_query, "exact_match")
    assert sub_query.exact_match == exact_match


def test_create_search_runs_query_with_search_text():
    """Test query creation with search_text filter."""
    search_text = "important test data"
    query = create_search_runs_query(search_text=search_text)

    assert len(query.and_) == 1
    sub_query = query.and_[0]

    assert hasattr(sub_query, "search_text")
    assert sub_query.search_text == search_text


def test_create_search_runs_query_with_workspace_rid():
    """Test query creation with workspace_rid filter."""
    workspace_rid = "ri.workspace.main.workspace.12345"
    query = create_search_runs_query(workspace_rid=workspace_rid)

    assert len(query.and_) == 1
    sub_query = query.and_[0]

    assert hasattr(sub_query, "workspace")
    assert sub_query.workspace == workspace_rid


def test_create_search_runs_query_with_all_filters():
    """Test query creation with all filters combined."""
    start = datetime(2024, 1, 1)
    end = datetime(2024, 12, 31)
    created_after = datetime(2024, 6, 1)
    created_before = datetime(2024, 6, 30)
    name_substring = "test"
    labels = ["test", "automated"]
    properties = {"env": "prod"}
    exact_match = "Test Run 1"
    search_text = "important"
    workspace_rid = "ri.workspace.main.workspace.12345"

    query = create_search_runs_query(
        start=start,
        end=end,
        created_after=created_after,
        created_before=created_before,
        name_substring=name_substring,
        labels=labels,
        properties=properties,
        exact_match=exact_match,
        search_text=search_text,
        workspace_rid=workspace_rid,
    )

    # Should have all filters:
    # start_time, end_time, created_at, name_substring, labels, properties,
    # exact_match, search_text, workspace
    assert len(query.and_) == 9


def test_create_search_runs_query_empty_labels():
    """Test query creation with empty labels list."""
    query = create_search_runs_query(labels=[])

    # Empty labels should not add a filter
    assert len(query.and_) == 0


def test_create_search_runs_query_empty_properties():
    """Test query creation with empty properties dict."""
    query = create_search_runs_query(properties={})

    # Empty properties should not add a filter
    assert len(query.and_) == 0


def test_create_search_runs_query_none_values():
    """Test query creation with None values for all optional parameters."""
    query = create_search_runs_query(
        start=None,
        end=None,
        name_substring=None,
        labels=None,
        properties=None,
        exact_match=None,
        search_text=None,
        created_after=None,
        created_before=None,
        workspace_rid=None,
    )

    # Should return empty AND query
    assert len(query.and_) == 0


def test_create_search_runs_query_created_at_only_after():
    """Test that created_at filter is created when only created_after is provided."""
    created_after = datetime(2024, 6, 1)
    query = create_search_runs_query(created_after=created_after, created_before=None)

    assert len(query.and_) == 1
    sub_query = query.and_[0]
    assert hasattr(sub_query, "created_at")
    assert sub_query.created_at.custom is not None
    assert sub_query.created_at.custom.start_time == _SecondsNanos.from_datetime(created_after).to_scout_run_api()
    assert sub_query.created_at.custom.end_time is None
    # end_time should be None or not set
    assert not hasattr(sub_query.created_at, "custom") or sub_query.created_at.custom.end_time is None


def test_create_search_runs_query_created_at_only_before():
    """Test that created_at filter is created when only created_before is provided."""
    created_before = datetime(2024, 6, 30)
    query = create_search_runs_query(created_after=None, created_before=created_before)

    assert len(query.and_) == 1
    sub_query = query.and_[0]
    assert hasattr(sub_query, "created_at")
    assert sub_query.created_at.custom is not None
    assert sub_query.created_at.custom.end_time == _SecondsNanos.from_datetime(created_before).to_scout_run_api()
    assert sub_query.created_at.custom.start_time is None
    # start_time should be None or not set
    assert not hasattr(sub_query.created_at, "custom") or sub_query.created_at.custom.start_time is None
