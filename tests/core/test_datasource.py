from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core.channel import ChannelDataType
from nominal.core.datasource import CreateChannelRequest, DataSource


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
    assert len(batch_req.requests) == 3


def test_batch_add_channels_multiple_batches(mock_datasource: DataSource, mock_clients: MagicMock):
    channels = [
        CreateChannelRequest(name="ch1", data_type=ChannelDataType.DOUBLE),
        CreateChannelRequest(name="ch2", data_type=ChannelDataType.STRING),
        CreateChannelRequest(name="ch3", data_type=ChannelDataType.INT),
    ]
    mock_datasource.batch_add_channels(channels, batch_size=2)

    assert mock_clients.series_metadata.batch_create.call_count == 2
    _, first_req = mock_clients.series_metadata.batch_create.call_args_list[0][0]
    assert len(first_req.requests) == 2
    _, second_req = mock_clients.series_metadata.batch_create.call_args_list[1][0]
    assert len(second_req.requests) == 1


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
