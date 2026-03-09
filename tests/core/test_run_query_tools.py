"""Unit tests for run query building with new TimeframeFilter and created_at support."""

from datetime import datetime, timedelta, timezone

from nominal_api import api

from nominal.core._utils.query_tools import create_search_runs_query
from nominal.ts import _SecondsNanos


def test_create_search_runs_query_basic():
    """Test basic query creation without filters."""
    query = create_search_runs_query()
    assert query.and_ == []


def test_create_search_runs_query_with_start_time():
    """Test query creation with start time filter using TimeframeFilter.

    start and end are combined into a single end_time sub-query whose
    CustomTimeframeFilter carries the bounds.
    """
    start_time = datetime(2024, 1, 1, 12, 0, 0)
    query = create_search_runs_query(start=start_time)

    assert query.and_ is not None
    assert len(query.and_) == 1
    sub_query = query.and_[0]

    assert sub_query.end_time is not None
    assert sub_query.end_time.custom is not None
    assert sub_query.end_time.custom.start_time == _SecondsNanos.from_datetime(start_time).to_scout_run_api()
    assert sub_query.end_time.custom.end_time is None


def test_create_search_runs_query_with_end_time():
    """Test query creation with end time filter using TimeframeFilter."""
    end_time = datetime(2024, 12, 31, 23, 59, 59)
    query = create_search_runs_query(end=end_time)

    assert query.and_ is not None
    assert len(query.and_) == 1
    sub_query = query.and_[0]

    assert sub_query.end_time is not None
    assert sub_query.end_time.custom is not None
    assert sub_query.end_time.custom.start_time is None
    assert sub_query.end_time.custom.end_time == _SecondsNanos.from_datetime(end_time).to_scout_run_api()


def test_create_search_runs_query_with_created_after():
    """Test query creation with created_after filter."""
    created_after = datetime(2024, 6, 1, 0, 0, 0)
    query = create_search_runs_query(created_after=created_after)

    assert query.and_ is not None
    assert len(query.and_) == 1
    sub_query = query.and_[0]

    assert sub_query.created_at is not None
    assert sub_query.created_at.custom is not None
    assert sub_query.created_at.custom.start_time == _SecondsNanos.from_datetime(created_after).to_scout_run_api()
    assert sub_query.created_at.custom.end_time is None


def test_create_search_runs_query_with_created_before():
    """Test query creation with created_before filter."""
    created_before = datetime(2024, 6, 30, 23, 59, 59)
    query = create_search_runs_query(created_before=created_before)

    assert query.and_ is not None
    assert len(query.and_) == 1
    sub_query = query.and_[0]

    assert sub_query.created_at is not None
    assert sub_query.created_at.custom is not None
    assert sub_query.created_at.custom.start_time is None
    assert sub_query.created_at.custom.end_time == _SecondsNanos.from_datetime(created_before).to_scout_run_api()


def test_create_search_runs_query_with_both_created_filters():
    """Test query creation with both created_after and created_before filters."""
    created_after = datetime(2024, 6, 1, 0, 0, 0)
    created_before = datetime(2024, 6, 30, 23, 59, 59)

    query = create_search_runs_query(created_after=created_after, created_before=created_before)

    assert query.and_ is not None
    assert len(query.and_) == 1
    sub_query = query.and_[0]

    assert sub_query.created_at is not None
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

    # start+end are combined into one end_time filter; created_after+before into one created_at filter
    assert query.and_ is not None
    assert len(query.and_) == 2


def test_create_search_runs_query_with_nanosecond_timestamps():
    """Test query creation with nanosecond integer timestamps."""
    now = datetime.now(timezone.utc)
    start_ns = _SecondsNanos.from_datetime(now).to_nanoseconds()
    end_ns = _SecondsNanos.from_datetime(now + timedelta(hours=1)).to_nanoseconds()
    created_after_ns = _SecondsNanos.from_datetime(now - timedelta(days=1)).to_nanoseconds()

    query = create_search_runs_query(start=start_ns, end=end_ns, created_after=created_after_ns)

    # start+end combined into one end_time filter; created_after into one created_at filter
    assert query.and_ is not None
    assert len(query.and_) == 2


def test_create_search_runs_query_with_labels():
    """Test query creation with labels filter."""
    labels = ["test", "automated", "production"]
    query = create_search_runs_query(labels=labels)

    assert query.and_ is not None
    assert len(query.and_) == 1
    sub_query = query.and_[0]

    assert sub_query.labels is not None
    assert sub_query.labels.labels == labels
    assert sub_query.labels.operator == api.SetOperator.AND


def test_create_search_runs_query_with_properties():
    """Test query creation with properties filter."""
    properties = {"env": "production", "version": "1.0", "region": "us-east"}
    query = create_search_runs_query(properties=properties)

    # One PropertiesFilter per property (3 properties = 3 filters)
    assert query.and_ is not None
    assert len(query.and_) == 3

    prop_filters = {}
    for sub_query in query.and_:
        assert sub_query.properties is not None
        assert len(sub_query.properties.values) == 1
        prop_filters[sub_query.properties.name] = sub_query.properties.values[0]

    assert prop_filters == properties


def test_create_search_runs_query_with_name_substring():
    """Test query creation with name_substring filter."""
    name_substring = "test-run"
    query = create_search_runs_query(name_substring=name_substring)

    assert query.and_ is not None
    assert len(query.and_) == 1
    assert query.and_[0].exact_match == name_substring


def test_create_search_runs_query_with_exact_match():
    """Test query creation with exact_match filter."""
    exact_match = "Run 12345"
    query = create_search_runs_query(exact_match=exact_match)

    assert query.and_ is not None
    assert len(query.and_) == 1
    assert query.and_[0].exact_match == exact_match


def test_create_search_runs_query_with_search_text():
    """Test query creation with search_text filter."""
    search_text = "important test data"
    query = create_search_runs_query(search_text=search_text)

    assert query.and_ is not None
    assert len(query.and_) == 1
    assert query.and_[0].search_text == search_text


def test_create_search_runs_query_with_workspace_rid():
    """Test query creation with workspace_rid filter."""
    workspace_rid = "ri.workspace.main.workspace.12345"
    query = create_search_runs_query(workspace_rid=workspace_rid)

    assert query.and_ is not None
    assert len(query.and_) == 1
    assert query.and_[0].workspace == workspace_rid


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

    # end_time (start+end combined), created_at (created_after+before combined),
    # name_substring, labels, properties (1 per pair = 1), exact_match, search_text, workspace
    assert query.and_ is not None
    assert len(query.and_) == 8


def test_create_search_runs_query_empty_labels():
    """Test query creation with empty labels list."""
    query = create_search_runs_query(labels=[])
    assert query.and_ == []


def test_create_search_runs_query_empty_properties():
    """Test query creation with empty properties dict."""
    query = create_search_runs_query(properties={})
    assert query.and_ == []


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
    assert query.and_ == []


def test_create_search_runs_query_created_at_only_after():
    """Test that created_at filter is created when only created_after is provided."""
    created_after = datetime(2024, 6, 1)
    query = create_search_runs_query(created_after=created_after, created_before=None)

    assert query.and_ is not None
    assert len(query.and_) == 1
    sub_query = query.and_[0]
    assert sub_query.created_at is not None
    assert sub_query.created_at.custom is not None
    assert sub_query.created_at.custom.start_time == _SecondsNanos.from_datetime(created_after).to_scout_run_api()
    assert sub_query.created_at.custom.end_time is None


def test_create_search_runs_query_created_at_only_before():
    """Test that created_at filter is created when only created_before is provided."""
    created_before = datetime(2024, 6, 30)
    query = create_search_runs_query(created_after=None, created_before=created_before)

    assert query.and_ is not None
    assert len(query.and_) == 1
    sub_query = query.and_[0]
    assert sub_query.created_at is not None
    assert sub_query.created_at.custom is not None
    assert sub_query.created_at.custom.end_time == _SecondsNanos.from_datetime(created_before).to_scout_run_api()
    assert sub_query.created_at.custom.start_time is None


def test_create_search_runs_query_with_asset_rids_any_of():
    """Test query creation with asset_rids_any_of filter."""
    asset_rids = ["ri.asset.main.asset.1", "ri.asset.main.asset.2"]
    query = create_search_runs_query(asset_rids_any_of=asset_rids)

    assert query.and_ is not None
    assert len(query.and_) == 1
    sub_query = query.and_[0]
    assert sub_query.assets is not None
    assert sub_query.assets.assets == asset_rids


def test_create_search_runs_query_with_asset_rids_all_of():
    """Test query creation with asset_rids_all_of filter (AND semantics)."""
    asset_rids = ["ri.asset.main.asset.1", "ri.asset.main.asset.2"]
    query = create_search_runs_query(asset_rids_all_of=asset_rids)

    assert query.and_ is not None
    assert len(query.and_) == 2
    for i, sub_query in enumerate(query.and_):
        assert sub_query.assets is not None
        assert sub_query.assets.assets == [asset_rids[i]]


def test_create_search_runs_query_with_empty_asset_rids_any_of():
    """Empty asset_rids_any_of should not add a filter."""
    query = create_search_runs_query(asset_rids_any_of=[])
    assert query.and_ == []


def test_create_search_runs_query_with_has_single_asset():
    """Test query creation with has_single_asset filter."""
    query = create_search_runs_query(has_single_asset=True)
    assert query.and_ is not None
    assert len(query.and_) == 1
    assert query.and_[0].is_single_asset is True

    query_false = create_search_runs_query(has_single_asset=False)
    assert query_false.and_ is not None
    assert query_false.and_[0].is_single_asset is False
