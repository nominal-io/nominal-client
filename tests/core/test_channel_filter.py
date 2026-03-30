from __future__ import annotations

import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from nominal.core.channel import (
    Channel,
    ChannelDataType,
    _batch_check_channels_have_data,
    _build_tag_filters,
    filter_channels_with_data,
)


@pytest.fixture
def mock_clients():
    clients = MagicMock()
    clients.auth_header = "Bearer test-token"
    return clients


def _make_channel(name: str, data_source: str = "ds-1", data_type: ChannelDataType | None = ChannelDataType.DOUBLE, **kwargs) -> Channel:
    """Create a Channel with mock clients for testing."""
    clients = kwargs.pop("clients", MagicMock())
    clients.auth_header = "Bearer test-token"
    return Channel(
        name=name,
        data_source=data_source,
        data_type=data_type,
        unit=None,
        description=None,
        _clients=clients,
    )


def _make_series_count_response(counts: list[int | None]) -> MagicMock:
    """Build a mock BatchGetSeriesCountResponse with the given series counts."""
    response = MagicMock()
    response.responses = [MagicMock(series_count=count) for count in counts]
    return response


class TestBuildTagFilters:
    def test_none_tags_returns_none(self):
        """None tags produce no tag filters."""
        assert _build_tag_filters(None) is None

    def test_empty_tags_returns_none(self):
        """Empty tags dict produces no tag filters."""
        assert _build_tag_filters({}) is None

    def test_single_tag_produces_single_filter(self):
        """A single tag produces a TagFilters with the 'single' variant."""
        result = _build_tag_filters({"env": "prod"})
        assert result is not None
        assert result.single is not None
        assert result.single.key.literal == "env"
        assert result.single.values[0].literal == "prod"
        assert result.single.operator.value == "IN"

    def test_multiple_tags_produces_and_composition(self):
        """Multiple tags are composed with AND semantics."""
        result = _build_tag_filters({"env": "prod", "region": "us-east"})
        assert result is not None
        assert result.and_ is not None
        assert len(result.and_) == 2
        keys = {f.single.key.literal for f in result.and_}
        assert keys == {"env", "region"}


class TestBatchCheckChannelsHaveData:
    def test_filters_channels_by_series_count(self, mock_clients):
        """Channels with series_count > 0 are returned; those with 0 or None are excluded."""
        channels = [
            _make_channel("ch1", clients=mock_clients),
            _make_channel("ch2", clients=mock_clients),
            _make_channel("ch3", clients=mock_clients),
        ]
        mock_clients.datasource.batch_get_series_count.return_value = _make_series_count_response([1, 0, None])

        start = MagicMock()
        end = MagicMock()
        matched, underconstrained = _batch_check_channels_have_data(mock_clients, channels, None, start, end)

        assert [ch.name for ch in matched] == ["ch1"]
        assert underconstrained == []

    def test_detects_underconstrained_tags(self, mock_clients):
        """Channels with series_count > 1 are flagged as underconstrained but still returned."""
        channels = [
            _make_channel("ch1", clients=mock_clients),
            _make_channel("ch2", clients=mock_clients),
        ]
        mock_clients.datasource.batch_get_series_count.return_value = _make_series_count_response([3, 1])

        matched, underconstrained = _batch_check_channels_have_data(
            mock_clients, channels, None, MagicMock(), MagicMock()
        )

        assert [ch.name for ch in matched] == ["ch1", "ch2"]
        assert underconstrained == ["ch1"]

    def test_builds_request_with_tag_filters(self, mock_clients):
        """Tag filters are passed through to the API request."""
        channels = [_make_channel("ch1", clients=mock_clients)]
        mock_clients.datasource.batch_get_series_count.return_value = _make_series_count_response([1])
        tag_filters = _build_tag_filters({"env": "prod"})

        _batch_check_channels_have_data(mock_clients, channels, tag_filters, MagicMock(), MagicMock())

        call_args = mock_clients.datasource.batch_get_series_count.call_args
        request = call_args[0][1]  # second positional arg
        assert request.requests[0].tag_filters is not None
        assert request.requests[0].tag_filters.single.key.literal == "env"


class TestFilterChannelsWithData:
    def test_returns_channels_with_data(self):
        """Only channels with data (series_count > 0) are yielded."""
        clients = MagicMock()
        clients.auth_header = "Bearer test-token"
        clients.datasource.batch_get_series_count.return_value = _make_series_count_response([1, 0, 1])

        channels = [
            _make_channel("has_data_1", clients=clients),
            _make_channel("no_data", clients=clients),
            _make_channel("has_data_2", clients=clients),
        ]
        result = list(filter_channels_with_data(
            channels,
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 12, 31, tzinfo=timezone.utc),
        ))

        assert {ch.name for ch in result} == {"has_data_1", "has_data_2"}

    def test_empty_input_yields_nothing(self):
        """An empty channel list produces an empty iterator with no API calls."""
        result = list(filter_channels_with_data(
            [],
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 12, 31, tzinfo=timezone.utc),
        ))
        assert result == []

    def test_filters_by_tags(self):
        """Tags are forwarded to the API; channels without matching tags are excluded."""
        clients = MagicMock()
        clients.auth_header = "Bearer test-token"
        clients.datasource.batch_get_series_count.return_value = _make_series_count_response([0, 1])

        channels = [
            _make_channel("no_match", clients=clients),
            _make_channel("match", clients=clients),
        ]
        result = list(filter_channels_with_data(
            channels,
            tags={"env": "prod"},
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 12, 31, tzinfo=timezone.utc),
        ))

        assert [ch.name for ch in result] == ["match"]
        # Verify tags were included in the request
        call_args = clients.datasource.batch_get_series_count.call_args
        request = call_args[0][1]
        assert request.requests[0].tag_filters is not None

    def test_accepts_nanosecond_timestamps(self):
        """Integer nanosecond timestamps are accepted alongside datetime objects."""
        clients = MagicMock()
        clients.auth_header = "Bearer test-token"
        clients.datasource.batch_get_series_count.return_value = _make_series_count_response([1])

        channels = [_make_channel("ch1", clients=clients)]
        result = list(filter_channels_with_data(
            channels,
            start_time=1704067200000000000,  # 2024-01-01 as nanos
            end_time=1735689600000000000,    # 2025-01-01 as nanos
        ))

        assert len(result) == 1

    def test_underconstrained_tags_warning(self, caplog):
        """Channels with multiple series are yielded but a summary warning is logged."""
        clients = MagicMock()
        clients.auth_header = "Bearer test-token"
        clients.datasource.batch_get_series_count.return_value = _make_series_count_response([3, 5])

        channels = [
            _make_channel("ch1", clients=clients),
            _make_channel("ch2", clients=clients),
        ]
        with caplog.at_level(logging.WARNING):
            result = list(filter_channels_with_data(
                channels,
                tags={"env": "prod"},
                start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
                end_time=datetime(2024, 12, 31, tzinfo=timezone.utc),
            ))

        assert len(result) == 2
        assert "2 channels have underconstrained tags" in caplog.text

    def test_external_datasources_excluded(self):
        """Channels returning series_count=None (external datasources) are excluded."""
        clients = MagicMock()
        clients.auth_header = "Bearer test-token"
        clients.datasource.batch_get_series_count.return_value = _make_series_count_response([None, 1])

        channels = [
            _make_channel("external", clients=clients),
            _make_channel("nominal", clients=clients),
        ]
        result = list(filter_channels_with_data(
            channels,
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 12, 31, tzinfo=timezone.utc),
        ))

        assert [ch.name for ch in result] == ["nominal"]

    def test_batching_respects_batch_size(self):
        """Channels are split into batches of the configured size."""
        clients = MagicMock()
        clients.auth_header = "Bearer test-token"
        # Each batch call returns all channels as having data
        clients.datasource.batch_get_series_count.side_effect = [
            _make_series_count_response([1, 1]),
            _make_series_count_response([1, 1]),
            _make_series_count_response([1]),
        ]

        channels = [_make_channel(f"ch{i}", clients=clients) for i in range(5)]
        result = list(filter_channels_with_data(
            channels,
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 12, 31, tzinfo=timezone.utc),
            batch_size=2,
        ))

        assert len(result) == 5
        assert clients.datasource.batch_get_series_count.call_count == 3

    def test_concurrent_execution(self):
        """With multiple batches and workers, all results are collected without drops."""
        clients = MagicMock()
        clients.auth_header = "Bearer test-token"
        # 10 channels, batch_size=3 → 4 batches, some workers run in parallel
        clients.datasource.batch_get_series_count.side_effect = [
            _make_series_count_response([1, 0, 1]),
            _make_series_count_response([1, 1, 0]),
            _make_series_count_response([0, 1, 1]),
            _make_series_count_response([1]),
        ]

        channels = [_make_channel(f"ch{i}", clients=clients) for i in range(10)]
        result = list(filter_channels_with_data(
            channels,
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 12, 31, tzinfo=timezone.utc),
            batch_size=3,
            num_workers=4,
        ))

        # 1+0+1 + 1+1+0 + 0+1+1 + 1 = 7 channels with data
        assert len(result) == 7

    def test_api_error_in_one_batch_does_not_block_others(self, caplog):
        """If one batch fails, other batches still return their results."""
        clients = MagicMock()
        clients.auth_header = "Bearer test-token"
        clients.datasource.batch_get_series_count.side_effect = [
            _make_series_count_response([1]),
            RuntimeError("API error"),
            _make_series_count_response([1]),
        ]

        channels = [_make_channel(f"ch{i}", clients=clients) for i in range(3)]
        with caplog.at_level(logging.ERROR):
            result = list(filter_channels_with_data(
                channels,
                start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
                end_time=datetime(2024, 12, 31, tzinfo=timezone.utc),
                batch_size=1,
            ))

        # 2 of 3 batches succeed
        assert len(result) == 2
        assert "Failed to check data presence" in caplog.text
