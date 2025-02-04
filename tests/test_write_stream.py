import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from nominal.core.stream import BatchItem, WriteStream
from nominal.core.connection import Connection
from nominal.ts import _SecondsNanos
from nominal_api_protos.nominal_write_pb2 import (
    WriteRequestNominal,
    Series,
    Points,
    DoublePoint,
    StringPoint,
    Channel,
)

@pytest.fixture(autouse=True)
def mock_channel():
    with patch('nominal_api_protos.nominal_write_pb2.Channel', autospec=True) as mock:
        mock.return_value = MagicMock()
        mock.return_value.name = ""
        yield mock

@pytest.fixture
def mock_clients():
    clients = MagicMock()
    clients.storage_writer = MagicMock()
    clients.auth_header = "test-auth-header"
    return clients

@pytest.fixture
def mock_connection(mock_clients):
    return Connection(
        rid="test-connection-rid",
        name="Test Connection",
        description="A connection for testing",
        _tags={},
        _clients=mock_clients,
        _nominal_data_source_rid="test-datasource-rid"
    )

def test_process_batch_double_points(mock_connection):
    # Create test data
    timestamp = datetime.now()
    batch = [
        BatchItem("test_channel", timestamp, 42.0),
        BatchItem("test_channel", timestamp + timedelta(seconds=1), 43.0),
    ]

    # Process the batch
    mock_connection._process_batch(batch)

    # Get the actual request that was sent
    actual_request = mock_connection._clients.storage_writer.write_nominal_batches.call_args[0][1]

    # Verify it's the correct type
    assert isinstance(actual_request, WriteRequestNominal)
    
    # Verify series structure
    assert len(actual_request.series) == 1
    series = actual_request.series[0]
    assert isinstance(series, Series)
    assert series.channel.name == "test_channel"
    
    # Verify points
    points = series.points
    assert points.HasField("double_points")
    assert not points.HasField("string_points")
    
    double_points = points.double_points.points
    assert len(double_points) == 2
    
    # Verify individual point values
    assert double_points[0].value == 42.0
    assert double_points[1].value == 43.0
    
    # Verify timestamps
    expected_ts1 = _SecondsNanos.from_datetime(timestamp)
    expected_ts2 = _SecondsNanos.from_datetime(timestamp + timedelta(seconds=1))
    
    assert double_points[0].timestamp.seconds == expected_ts1.seconds
    assert double_points[0].timestamp.nanos == expected_ts1.nanos
    assert double_points[1].timestamp.seconds == expected_ts2.seconds
    assert double_points[1].timestamp.nanos == expected_ts2.nanos

def test_process_batch_string_points(mock_connection):
    # Create test data
    timestamp = datetime.now()
    batch = [
        BatchItem("test_channel", timestamp, "value1"),
        BatchItem("test_channel", timestamp + timedelta(seconds=1), "value2"),
    ]

    # Process the batch
    mock_connection._process_batch(batch)

    # Get the actual request that was sent
    actual_request = mock_connection._clients.storage_writer.write_nominal_batches.call_args[0][1]

    # Verify series structure
    assert len(actual_request.series) == 1
    series = actual_request.series[0]
    
    # Verify points
    points = series.points
    assert points.HasField("string_points")
    assert not points.HasField("double_points")
    
    string_points = points.string_points.points
    assert len(string_points) == 2
    
    # Verify values
    assert string_points[0].value == "value1"
    assert string_points[1].value == "value2"

def test_process_batch_with_tags(mock_connection):
    # Create test data with tags
    timestamp = datetime.now()
    batch = [
        BatchItem("test_channel", timestamp, 42.0, {"tag1": "value1"}),
        BatchItem("test_channel", timestamp + timedelta(seconds=1), 43.0, {"tag1": "value1"}),
    ]

    # Process the batch
    mock_connection._process_batch(batch)

    # Get the actual request that was sent
    actual_request = mock_connection._clients.storage_writer.write_nominal_batches.call_args[0][1]

    # Verify tags were included
    assert len(actual_request.series) == 1
    series = actual_request.series[0]
    assert series.tags == {"tag1": "value1"}

def test_process_batch_invalid_type(mock_connection):
    # Create test data with invalid type
    timestamp = datetime.now()
    batch = [
        BatchItem("test_channel", timestamp, [1, 2, 3]),  # Lists are not supported
    ]

    # Verify it raises the correct error
    with pytest.raises(ValueError, match="only float and string are supported types for value"):
        mock_connection._process_batch(batch)
