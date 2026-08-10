from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from nominal.core.channel import ChannelDataType
from nominal.core.datasource import BatchAddChannelsResult, CreateChannelRequest, DataSource

# (public method, series_metadata endpoint it must hit) — both delegate to _batch_write_channels,
# so every test runs against both to guarantee they can't drift apart.
BATCH_WRITE_METHODS = [
    ("batch_add_channels", "batch_create"),
    ("batch_upsert_channels", "batch_create_or_update"),
]
ENDPOINTS = [endpoint for _, endpoint in BATCH_WRITE_METHODS]


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


@pytest.mark.parametrize(("method", "endpoint"), BATCH_WRITE_METHODS)
def test_batch_write_channels_single_batch(
    mock_datasource: DataSource, mock_clients: MagicMock, method: str, endpoint: str
):
    """All channels fit in one batch, so the endpoint is called exactly once (and no other endpoint at all)."""
    channels = [
        CreateChannelRequest(name="ch1", data_type=ChannelDataType.DOUBLE),
        CreateChannelRequest(name="ch2", data_type=ChannelDataType.STRING, description="a string"),
        CreateChannelRequest(name="ch3", data_type=ChannelDataType.INT, unit="m/s"),
    ]
    getattr(mock_datasource, method)(channels)

    endpoint_mock = getattr(mock_clients.series_metadata, endpoint)
    assert endpoint_mock.call_count == 1
    for other in ENDPOINTS:
        if other != endpoint:
            getattr(mock_clients.series_metadata, other).assert_not_called()
    _, batch_req = endpoint_mock.call_args[0]
    assert [r.channel for r in batch_req.requests] == ["ch1", "ch2", "ch3"]


@pytest.mark.parametrize("method", [method for method, _ in BATCH_WRITE_METHODS])
def test_batch_write_channels_empty(mock_datasource: DataSource, mock_clients: MagicMock, method: str):
    """An empty channel list results in no API calls."""
    getattr(mock_datasource, method)([])
    for endpoint in ENDPOINTS:
        getattr(mock_clients.series_metadata, endpoint).assert_not_called()


@pytest.mark.parametrize(("method", "endpoint"), BATCH_WRITE_METHODS)
def test_batch_write_channels_request_fields(
    mock_datasource: DataSource, mock_clients: MagicMock, method: str, endpoint: str
):
    """Channel name, datasource RID, description, and unit are correctly propagated to the API request."""
    channels = [
        CreateChannelRequest(name="velocity", data_type=ChannelDataType.DOUBLE, description="speed", unit="m/s"),
    ]
    getattr(mock_datasource, method)(channels)

    _, batch_req = getattr(mock_clients.series_metadata, endpoint).call_args[0]
    req = batch_req.requests[0]
    assert req.channel == "velocity"
    assert req.data_source_rid == "test-datasource-rid"
    assert req.description == "speed"
    assert req.unit == "m/s"


@pytest.mark.parametrize(("method", "endpoint"), BATCH_WRITE_METHODS)
def test_batch_write_channels_api_failure_propagates(
    mock_datasource: DataSource, mock_clients: MagicMock, method: str, endpoint: str
):
    """An exception raised by the API propagates to the caller unchanged."""
    getattr(mock_clients.series_metadata, endpoint).side_effect = RuntimeError("API error")
    with pytest.raises(RuntimeError, match="API error"):
        getattr(mock_datasource, method)(_make_channels(1))


@pytest.mark.parametrize("method", [method for method, _ in BATCH_WRITE_METHODS])
def test_batch_write_channels_returns_channels_and_no_missing(mock_datasource: DataSource, method: str):
    """When all requested channels are written, result.channels is populated and result.missing is empty."""
    req1 = CreateChannelRequest(name="ch1", data_type=ChannelDataType.DOUBLE)
    req2 = CreateChannelRequest(name="ch2", data_type=ChannelDataType.STRING)
    mock_ch1, mock_ch2 = MagicMock(), MagicMock()
    mock_ch1.name = "ch1"
    mock_ch2.name = "ch2"

    with patch.object(DataSource, "get_channels", return_value=[mock_ch1, mock_ch2]):
        result = getattr(mock_datasource, method)([req1, req2])

    assert isinstance(result, BatchAddChannelsResult)
    assert result.channels == [mock_ch1, mock_ch2]
    assert result.missing == []


@pytest.mark.parametrize("method", [method for method, _ in BATCH_WRITE_METHODS])
def test_batch_write_channels_returns_missing_when_server_drops_channel(mock_datasource: DataSource, method: str):
    """Channels not returned by get_channels after the write appear in result.missing."""
    req1 = CreateChannelRequest(name="ch1", data_type=ChannelDataType.DOUBLE)
    req2 = CreateChannelRequest(name="ch2", data_type=ChannelDataType.DOUBLE)
    mock_ch1 = MagicMock()
    mock_ch1.name = "ch1"

    with patch.object(DataSource, "get_channels", return_value=[mock_ch1]):  # ch2 not returned
        result = getattr(mock_datasource, method)([req1, req2])

    assert result.channels == [mock_ch1]
    assert result.missing == [req2]
