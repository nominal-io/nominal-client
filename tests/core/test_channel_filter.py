from __future__ import annotations

import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from nominal.core.channel import filter_channels_with_data


@pytest.fixture
def make_series_count_response():
    """Factory fixture that builds a mock BatchGetSeriesCountResponse."""
    def _make(counts: list[int | None]):
        response = MagicMock()
        response.responses = [MagicMock(series_count=count) for count in counts]
        return response
    return _make


def test_returns_channels_with_data(mock_clients, make_channel, make_series_count_response):
    """Only channels with data (series_count > 0) are yielded."""
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1, 0, 1])
    channels = [make_channel("has_data_1"), make_channel("no_data"), make_channel("has_data_2")]

    result = list(filter_channels_with_data(
        channels,
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2024, 12, 31, tzinfo=timezone.utc),
    ))

    assert {ch.name for ch in result} == {"has_data_1", "has_data_2"}


def test_empty_input_yields_nothing():
    """An empty channel list produces an empty iterator with no API calls."""
    result = list(filter_channels_with_data(
        [],
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2024, 12, 31, tzinfo=timezone.utc),
    ))
    assert result == []


def test_tags_are_forwarded_to_api(mock_clients, make_channel, make_series_count_response):
    """Tags are included in the API request so the server filters by them."""
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([0, 1])
    channels = [make_channel("no_match"), make_channel("match")]

    result = list(filter_channels_with_data(
        channels,
        tags={"env": "prod"},
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2024, 12, 31, tzinfo=timezone.utc),
    ))

    assert [ch.name for ch in result] == ["match"]
    request = mock_clients.datasource.batch_get_series_count.call_args[0][1]
    assert request.requests[0].tag_filters is not None


def test_accepts_nanosecond_timestamps(mock_clients, make_channel, make_series_count_response):
    """Integer nanosecond timestamps are accepted alongside datetime objects."""
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([1])

    result = list(filter_channels_with_data(
        [make_channel("ch1")],
        start_time=1704067200000000000,
        end_time=1735689600000000000,
    ))

    assert len(result) == 1


def test_underconstrained_tags_warning(mock_clients, make_channel, make_series_count_response, caplog):
    """Channels with multiple series are yielded but a summary warning is logged."""
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([3, 5])
    channels = [make_channel("ch1"), make_channel("ch2")]

    with caplog.at_level(logging.WARNING):
        result = list(filter_channels_with_data(
            channels,
            tags={"env": "prod"},
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 12, 31, tzinfo=timezone.utc),
        ))

    assert len(result) == 2
    assert "2 channels have underconstrained tags" in caplog.text


def test_external_datasources_excluded(mock_clients, make_channel, make_series_count_response):
    """Channels returning series_count=None (external datasources) are excluded."""
    mock_clients.datasource.batch_get_series_count.return_value = make_series_count_response([None, 1])
    channels = [make_channel("external"), make_channel("nominal")]

    result = list(filter_channels_with_data(
        channels,
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2024, 12, 31, tzinfo=timezone.utc),
    ))

    assert [ch.name for ch in result] == ["nominal"]


def test_batching_respects_batch_size(mock_clients, make_channel, make_series_count_response):
    """Channels are split into batches of the configured size."""
    mock_clients.datasource.batch_get_series_count.side_effect = [
        make_series_count_response([1, 1]),
        make_series_count_response([1, 1]),
        make_series_count_response([1]),
    ]
    channels = [make_channel(f"ch{i}") for i in range(5)]

    result = list(filter_channels_with_data(
        channels,
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2024, 12, 31, tzinfo=timezone.utc),
        batch_size=2,
    ))

    assert len(result) == 5
    assert mock_clients.datasource.batch_get_series_count.call_count == 3


def test_all_results_collected_across_concurrent_batches(mock_clients, make_channel, make_series_count_response):
    """With multiple batches and workers, all results are collected without drops."""
    mock_clients.datasource.batch_get_series_count.side_effect = [
        make_series_count_response([1, 0, 1]),
        make_series_count_response([1, 1, 0]),
        make_series_count_response([0, 1, 1]),
        make_series_count_response([1]),
    ]
    channels = [make_channel(f"ch{i}") for i in range(10)]

    result = list(filter_channels_with_data(
        channels,
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2024, 12, 31, tzinfo=timezone.utc),
        batch_size=3,
        num_workers=4,
    ))

    assert len(result) == 7


def test_api_error_in_one_batch_does_not_block_others(mock_clients, make_channel, make_series_count_response, caplog):
    """If one batch fails, channels from that batch are included as a safe default."""
    mock_clients.datasource.batch_get_series_count.side_effect = [
        make_series_count_response([1]),
        RuntimeError("API error"),
        make_series_count_response([1]),
    ]
    channels = [make_channel(f"ch{i}") for i in range(3)]

    with caplog.at_level(logging.ERROR):
        result = list(filter_channels_with_data(
            channels,
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 12, 31, tzinfo=timezone.utc),
            batch_size=1,
        ))

    # All 3 channels returned: 2 confirmed by API + 1 from failed batch (safe default)
    assert len(result) == 3
    assert "Failed to check data presence" in caplog.text
