from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from nominal.core.channel import ChannelDataType
from nominal.core.datasource import BatchAddChannelsResult, CreateChannelRequest, DataSource


def _make_channels(n: int) -> list[CreateChannelRequest]:
    return [CreateChannelRequest(name=f"ch{i}", data_type=ChannelDataType.DOUBLE) for i in range(n)]


@pytest.fixture
def mock_clients():
    clients = MagicMock()
    clients.series_metadata = MagicMock()
    # get_channels iterates over response.responses — default to empty so existing tests don't crash
    clients.channel_metadata.batch_get_channel_metadata.return_value.responses = []
    return clients


@pytest.fixture
def mock_datasource(mock_clients):
    return DataSource(rid="test-datasource-rid", _clients=mock_clients)


def test_batch_add_channels_single_batch(mock_datasource: DataSource, mock_clients: MagicMock):
    """All channels fit in one batch, so batch_create is called exactly once."""
    channels = [
        CreateChannelRequest(name="ch1", data_type=ChannelDataType.DOUBLE),
        CreateChannelRequest(name="ch2", data_type=ChannelDataType.STRING, description="a string"),
        CreateChannelRequest(name="ch3", data_type=ChannelDataType.INT, unit="m/s"),
    ]
    mock_datasource.batch_add_channels(channels)

    assert mock_clients.series_metadata.batch_create.call_count == 1
    _, batch_req = mock_clients.series_metadata.batch_create.call_args[0]
    assert [r.channel for r in batch_req.requests] == ["ch1", "ch2", "ch3"]


def test_batch_add_channels_empty(mock_datasource: DataSource, mock_clients: MagicMock):
    """An empty channel list results in no API calls."""
    mock_datasource.batch_add_channels([])
    mock_clients.series_metadata.batch_create.assert_not_called()


def test_batch_add_channels_request_fields(mock_datasource: DataSource, mock_clients: MagicMock):
    """Channel name, datasource RID, description, and unit are correctly propagated to the API request."""
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


def test_batch_add_channels_api_failure_propagates(mock_datasource: DataSource, mock_clients: MagicMock):
    """An exception raised by the API propagates to the caller unchanged."""
    mock_clients.series_metadata.batch_create.side_effect = RuntimeError("API error")
    with pytest.raises(RuntimeError, match="API error"):
        mock_datasource.batch_add_channels(_make_channels(1))


def test_batch_add_channels_returns_channels_and_no_missing(mock_datasource: DataSource):
    """When all requested channels are created, result.channels is populated and result.missing is empty."""
    req1 = CreateChannelRequest(name="ch1", data_type=ChannelDataType.DOUBLE)
    req2 = CreateChannelRequest(name="ch2", data_type=ChannelDataType.STRING)
    mock_ch1, mock_ch2 = MagicMock(), MagicMock()
    mock_ch1.name = "ch1"
    mock_ch2.name = "ch2"

    with patch.object(DataSource, "get_channels", return_value=[mock_ch1, mock_ch2]):
        result = mock_datasource.batch_add_channels([req1, req2])

    assert isinstance(result, BatchAddChannelsResult)
    assert result.channels == [mock_ch1, mock_ch2]
    assert result.missing == []


def test_batch_add_channels_returns_missing_when_server_drops_channel(mock_datasource: DataSource):
    """Channels not returned by get_channels after creation appear in result.missing."""
    req1 = CreateChannelRequest(name="ch1", data_type=ChannelDataType.DOUBLE)
    req2 = CreateChannelRequest(name="ch2", data_type=ChannelDataType.DOUBLE)
    mock_ch1 = MagicMock()
    mock_ch1.name = "ch1"

    with patch.object(DataSource, "get_channels", return_value=[mock_ch1]):  # ch2 not returned
        result = mock_datasource.batch_add_channels([req1, req2])

    assert result.channels == [mock_ch1]
    assert result.missing == [req2]
