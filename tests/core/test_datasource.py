from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core.channel import ChannelDataType
from nominal.core.datasource import CreateChannelRequest, DataSource


def _make_channels(n: int) -> list[CreateChannelRequest]:
    return [CreateChannelRequest(name=f"ch{i}", data_type=ChannelDataType.DOUBLE) for i in range(n)]


@pytest.fixture
def mock_clients():
    clients = MagicMock()
    clients.series_metadata = MagicMock()
    return clients


@pytest.fixture
def mock_datasource(mock_clients):
    return DataSource(rid="test-datasource-rid", _clients=mock_clients)


def test_batch_add_channels_single_batch(mock_datasource: DataSource, mock_clients: MagicMock):
    channels = [
        CreateChannelRequest(name="ch1", data_type=ChannelDataType.DOUBLE),
        CreateChannelRequest(name="ch2", data_type=ChannelDataType.STRING, description="a string"),
        CreateChannelRequest(name="ch3", data_type=ChannelDataType.INT, unit="m/s"),
    ]
    mock_datasource.batch_add_channels(channels, batch_size=100)

    assert mock_clients.series_metadata.batch_create.call_count == 1
    _, batch_req = mock_clients.series_metadata.batch_create.call_args[0]
    assert [r.channel for r in batch_req.requests] == ["ch1", "ch2", "ch3"]


def test_batch_add_channels_multiple_batches(mock_datasource: DataSource, mock_clients: MagicMock):
    channels = [
        CreateChannelRequest(name="ch1", data_type=ChannelDataType.DOUBLE),
        CreateChannelRequest(name="ch2", data_type=ChannelDataType.STRING),
        CreateChannelRequest(name="ch3", data_type=ChannelDataType.INT),
    ]
    mock_datasource.batch_add_channels(channels, batch_size=2)

    assert mock_clients.series_metadata.batch_create.call_count == 2
    _, first_req = mock_clients.series_metadata.batch_create.call_args_list[0][0]
    assert [r.channel for r in first_req.requests] == ["ch1", "ch2"]
    _, second_req = mock_clients.series_metadata.batch_create.call_args_list[1][0]
    assert [r.channel for r in second_req.requests] == ["ch3"]


def test_batch_add_channels_empty(mock_datasource: DataSource, mock_clients: MagicMock):
    mock_datasource.batch_add_channels([])
    mock_clients.series_metadata.batch_create.assert_not_called()


def test_batch_add_channels_request_fields(mock_datasource: DataSource, mock_clients: MagicMock):
    channels = [
        CreateChannelRequest(name="velocity", data_type=ChannelDataType.DOUBLE, description="speed", unit="m/s"),
    ]
    mock_datasource.batch_add_channels(channels)

    _, batch_req = mock_clients.series_metadata.batch_create.call_args[0]
    req = batch_req.requests[0]
    assert req.channel == "velocity"
    assert req.data_source_rid == "test-datasource-rid"
    assert req.description == "speed"
    assert req.unit == "m/s"


@pytest.mark.parametrize("batch_size", [0, -1])
def test_batch_add_channels_invalid_batch_size(mock_datasource: DataSource, batch_size: int):
    with pytest.raises(ValueError):
        mock_datasource.batch_add_channels(_make_channels(3), batch_size=batch_size)


def test_batch_add_channels_api_failure_propagates(mock_datasource: DataSource, mock_clients: MagicMock):
    mock_clients.series_metadata.batch_create.side_effect = RuntimeError("API error")
    with pytest.raises(RuntimeError, match="API error"):
        mock_datasource.batch_add_channels(_make_channels(1))


def test_batch_add_channels_large_dataset(mock_datasource: DataSource, mock_clients: MagicMock):
    mock_datasource.batch_add_channels(_make_channels(250), batch_size=100)

    calls = mock_clients.series_metadata.batch_create.call_args_list
    assert len(calls) == 3
    assert [r.channel for r in calls[0][0][1].requests] == [f"ch{i}" for i in range(100)]
    assert [r.channel for r in calls[1][0][1].requests] == [f"ch{i}" for i in range(100, 200)]
    assert [r.channel for r in calls[2][0][1].requests] == [f"ch{i}" for i in range(200, 250)]
