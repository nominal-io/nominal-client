from __future__ import annotations

from datetime import datetime, timezone

from nominal.core.channel import filter_channels_with_data

# Common time bounds used across the test suite; concrete values don't matter, they just
# need to be valid and shared so tests don't re-state them.
START_TIME = datetime(2024, 1, 1, tzinfo=timezone.utc)
END_TIME = datetime(2024, 12, 31, tzinfo=timezone.utc)


def test_returns_channels_with_data(mock_clients, make_channel, make_series_count_response):
    """Only channels with data (series_count > 0) are yielded."""
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1, 0, 1])
    channels = [make_channel("has_data_1"), make_channel("no_data"), make_channel("has_data_2")]

    result = list(filter_channels_with_data(channels, start_time=START_TIME, end_time=END_TIME))

    assert {ch.name for ch in result} == {"has_data_1", "has_data_2"}


def test_empty_input_yields_nothing():
    """An empty channel list produces an empty iterator with no API calls."""
    result = list(filter_channels_with_data([], start_time=START_TIME, end_time=END_TIME))

    assert result == []


def test_tags_are_forwarded_to_api(mock_clients, make_channel, make_series_count_response):
    """Tags are included in the API request so the server filters by them."""
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([0, 1])
    channels = [make_channel("no_match"), make_channel("match")]

    result = list(filter_channels_with_data(channels, tags={"env": "prod"}, start_time=START_TIME, end_time=END_TIME))

    assert [ch.name for ch in result] == ["match"]
    request = mock_clients.datasource.batch_get_series_count.call_args[0][1]
    assert request.requests[0].tag_filters is not None


def test_accepts_nanosecond_timestamps(mock_clients, make_channel, make_series_count_response):
    """Integer nanosecond timestamps are accepted alongside datetime objects."""
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1])
    start_ns = int(START_TIME.timestamp() * 1_000_000_000)
    end_ns = int(END_TIME.timestamp() * 1_000_000_000)

    result = list(filter_channels_with_data([make_channel("ch1")], start_time=start_ns, end_time=end_ns))

    assert [ch.name for ch in result] == ["ch1"]


def test_underconstrained_channels_still_yielded(mock_clients, make_channel, make_series_count_response):
    """Channels whose tag set matches multiple series (series_count > 1) are still yielded.

    Underconstrained tags signal an incomplete caller filter, but the data is present — the
    channels should still flow through the pipeline so the caller can see them.
    """
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([3, 5])
    channels = [make_channel("ch1"), make_channel("ch2")]

    result = list(filter_channels_with_data(channels, tags={"env": "prod"}, start_time=START_TIME, end_time=END_TIME))

    assert [ch.name for ch in result] == ["ch1", "ch2"]


def test_external_datasources_excluded(mock_clients, make_channel, make_series_count_response):
    """Channels returning series_count=None (external datasources) are excluded."""
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([None, 1])
    channels = [make_channel("external"), make_channel("nominal")]

    result = list(filter_channels_with_data(channels, start_time=START_TIME, end_time=END_TIME))

    assert [ch.name for ch in result] == ["nominal"]


def test_batching_respects_batch_size(mock_clients, make_channel, make_series_count_response):
    """Channels are split into batches of the configured size."""
    # A callable side_effect keeps the test deterministic regardless of which order the
    # ThreadPoolExecutor happens to complete batches in — each request gets a response
    # whose shape matches its contents.
    def side_effect(_auth_header, request):
        return make_series_count_response([1] * len(request.requests))

    mock_clients.datasource.batch_get_series_count.side_effect = side_effect
    channels = [make_channel(f"ch{i}") for i in range(5)]

    result = list(filter_channels_with_data(channels, start_time=START_TIME, end_time=END_TIME, batch_size=2))

    assert len(result) == 5
    # 5 channels at batch_size=2 → batches of 2, 2, 1 = 3 API calls.
    assert mock_clients.datasource.batch_get_series_count.call_count == 3


def test_all_results_collected_across_concurrent_batches(mock_clients, make_channel, make_series_count_response):
    """With multiple batches and workers, all results are collected without drops."""
    # 7 of 10 channels have data; the rest don't. Encoding the outcome per channel (not
    # per batch) makes the test agnostic to batch-completion order.
    has_data = {f"ch{i}" for i in (0, 2, 3, 4, 7, 8, 9)}

    def side_effect(_auth_header, request):
        counts = [1 if req.channel in has_data else 0 for req in request.requests]
        return make_series_count_response(counts)

    mock_clients.datasource.batch_get_series_count.side_effect = side_effect
    channels = [make_channel(f"ch{i}") for i in range(10)]

    result = list(
        filter_channels_with_data(channels, start_time=START_TIME, end_time=END_TIME, batch_size=3, num_workers=4)
    )

    assert {ch.name for ch in result} == has_data


def test_api_error_triggers_individual_retry_then_excludes_still_failing(
    mock_clients, make_channel, make_series_count_response
):
    """Failed batch triggers per-channel retry; channels that still fail are excluded, not admitted.

    Behavior verified:
    * The batch call is retried — once per channel — after the initial batch failure.
    * A channel whose individual retry succeeds is yielded.
    * A channel whose individual retry also fails is excluded from the result.
    """

    # Multi-channel requests are the initial batch — fail. Single-channel requests are retries —
    # pass or fail based on the channel name. Checking request size keeps the test deterministic
    # regardless of ThreadPoolExecutor scheduling.
    def side_effect(_auth_header, request):
        if len(request.requests) > 1:
            raise RuntimeError("batch failed")
        if request.requests[0].channel == "ch0":
            return make_series_count_response([1])
        raise RuntimeError("still failing")

    mock_clients.datasource.batch_get_series_count.side_effect = side_effect
    channels = [make_channel("ch0"), make_channel("ch1")]

    result = list(filter_channels_with_data(channels, start_time=START_TIME, end_time=END_TIME, batch_size=2))

    # ch0 confirmed by individual retry; ch1 excluded because both attempts failed.
    assert [ch.name for ch in result] == ["ch0"]
    # 1 batch call + 2 individual retries = 3 API calls total. Confirms the retry actually
    # happened rather than both channels being silently excluded or admitted.
    assert mock_clients.datasource.batch_get_series_count.call_count == 3


def test_preserves_input_order(mock_clients, make_channel, make_series_count_response):
    """Channels are yielded in input order, not batch-completion order."""
    channels = [make_channel("c"), make_channel("a"), make_channel("b")]
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1, 1, 1])

    result = list(filter_channels_with_data(channels, start_time=START_TIME, end_time=END_TIME))

    assert [ch.name for ch in result] == ["c", "a", "b"]
