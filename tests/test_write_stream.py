from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from nominal_api_protos.nominal_write_pb2 import (
    Series,
    WriteRequestNominal,
)

from nominal.core.batch_processor_proto import process_batch
from nominal.core.connection import Connection
from nominal.core.stream import BatchItem
from nominal.ts import _SecondsNanos


@pytest.fixture(autouse=True)
def mock_channel():
    with patch("nominal_api_protos.nominal_write_pb2.Channel", autospec=True) as mock:
        mock.return_value = MagicMock()
        mock.return_value.name = ""
        yield mock


@pytest.fixture
def mock_clients():
    clients = MagicMock()
    clients.storage_writer = MagicMock()
    clients.auth_header = "test-auth-header"
    clients.proto_write = MagicMock()
    return clients


@pytest.fixture
def mock_connection(mock_clients):
    return Connection(
        rid="test-connection-rid",
        name="Test Connection",
        description="A connection for testing",
        _tags={},
        _clients=mock_clients,
        _nominal_data_source_rid="test-datasource-rid",
    )


def test_process_batch_double_points(mock_connection):
    # Create test data with fixed timestamp
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("test_channel", timestamp, 42.0),
        BatchItem("test_channel", timestamp + timedelta(seconds=1), 43.0),
    ]

    # Process the batch using the imported process_batch function
    process_batch(
        batch=batch,
        nominal_data_source_rid=mock_connection._nominal_data_source_rid,
        auth_header=mock_connection._clients.auth_header,
        proto_write=mock_connection._clients.proto_write,
    )

    # Get the actual request that was sent
    mock_write = mock_connection._clients.proto_write.write_nominal_batches
    mock_write.assert_called_once()

    # Check the arguments using kwargs instead of args
    kwargs = mock_write.call_args.kwargs
    assert kwargs["auth_header"] == "test-auth-header"
    assert kwargs["data_source_rid"] == "test-datasource-rid"
    actual_request = kwargs["request"]

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
    # Create test data with fixed timestamp
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("test_channel", timestamp, "value1"),
        BatchItem("test_channel", timestamp + timedelta(seconds=1), "value2"),
    ]

    # Process the batch using the imported process_batch function
    process_batch(
        batch=batch,
        nominal_data_source_rid=mock_connection._nominal_data_source_rid,
        auth_header=mock_connection._clients.auth_header,
        proto_write=mock_connection._clients.proto_write,
    )

    # Get the actual request that was sent
    mock_write = mock_connection._clients.proto_write.write_nominal_batches
    mock_write.assert_called_once()

    # Check the arguments using kwargs instead of args
    kwargs = mock_write.call_args.kwargs
    assert kwargs["auth_header"] == "test-auth-header"
    assert kwargs["data_source_rid"] == "test-datasource-rid"
    actual_request = kwargs["request"]

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
    # Create test data with fixed timestamp
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("test_channel", timestamp, 42.0, {"tag1": "value1"}),
        BatchItem("test_channel", timestamp + timedelta(seconds=1), 43.0, {"tag1": "value1"}),
    ]

    # Process the batch using the imported process_batch function
    process_batch(
        batch=batch,
        nominal_data_source_rid=mock_connection._nominal_data_source_rid,
        auth_header=mock_connection._clients.auth_header,
        proto_write=mock_connection._clients.proto_write,
    )

    # Get the actual request that was sent
    mock_write = mock_connection._clients.proto_write.write_nominal_batches
    mock_write.assert_called_once()

    # Check the arguments using kwargs instead of args
    kwargs = mock_write.call_args.kwargs
    assert kwargs["auth_header"] == "test-auth-header"
    assert kwargs["data_source_rid"] == "test-datasource-rid"
    actual_request = kwargs["request"]

    # Verify tags were included
    assert len(actual_request.series) == 1
    series = actual_request.series[0]
    assert series.tags == {"tag1": "value1"}


def test_process_batch_invalid_type(mock_connection):
    # Create test data with fixed timestamp
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("test_channel", timestamp, [1, 2, 3]),  # Lists are not supported
    ]

    # Verify it raises the correct error
    with pytest.raises(ValueError, match="only float and string are supported types for value"):
        process_batch(
            batch=batch,
            nominal_data_source_rid=mock_connection._nominal_data_source_rid,
            auth_header=mock_connection._clients.auth_header,
            proto_write=mock_connection._clients.proto_write,
        )


def test_process_batch_multiple_channels(mock_connection):
    # Create test data with fixed timestamp
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("channel1", timestamp, 42.0),
        BatchItem("channel1", timestamp + timedelta(seconds=1), 43.0),
        BatchItem("channel2", timestamp, "value1"),
        BatchItem("channel2", timestamp + timedelta(seconds=1), "value2"),
        BatchItem("channel3", timestamp, 100.0, {"tag1": "value1"}),
    ]

    # Process the batch
    process_batch(
        batch=batch,
        nominal_data_source_rid=mock_connection._nominal_data_source_rid,
        auth_header=mock_connection._clients.auth_header,
        proto_write=mock_connection._clients.proto_write,
    )

    # Get the actual request that was sent
    mock_write = mock_connection._clients.proto_write.write_nominal_batches
    mock_write.assert_called_once()

    # Check the basic arguments
    kwargs = mock_write.call_args.kwargs
    assert kwargs["auth_header"] == "test-auth-header"
    assert kwargs["data_source_rid"] == "test-datasource-rid"
    actual_request = kwargs["request"]

    # Verify we have three series
    assert len(actual_request.series) == 3

    # Check channel1 (double points)
    series1 = [s for s in actual_request.series if s.channel.name == "channel1"][0]
    assert series1.points.HasField("double_points")
    double_points = series1.points.double_points.points
    assert len(double_points) == 2
    assert double_points[0].value == 42.0
    assert double_points[1].value == 43.0

    # Check channel2 (string points)
    series2 = [s for s in actual_request.series if s.channel.name == "channel2"][0]
    assert series2.points.HasField("string_points")
    string_points = series2.points.string_points.points
    assert len(string_points) == 2
    assert string_points[0].value == "value1"
    assert string_points[1].value == "value2"

    # Check channel3 (double points with tags)
    series3 = [s for s in actual_request.series if s.channel.name == "channel3"][0]
    assert series3.points.HasField("double_points")
    assert series3.tags == {"tag1": "value1"}
    double_points = series3.points.double_points.points
    assert len(double_points) == 1
    assert double_points[0].value == 100.0


def test_multiple_write_streams(mock_connection):
    # Create test data with fixed timestamp
    timestamp = datetime(2024, 1, 1, 12, 0, 0)

    # First stream
    with mock_connection.get_write_stream(batch_size=2, max_wait=timedelta(seconds=1), use_protos=True) as stream1:
        stream1.enqueue("channel1", timestamp, 42.0)
        stream1.enqueue("channel1", timestamp + timedelta(seconds=1), 43.0)
        # Force a small sleep to allow the batch to be processed

    # Second stream
    with mock_connection.get_write_stream(batch_size=2, max_wait=timedelta(seconds=1), use_protos=True) as stream2:
        stream2.enqueue("channel2", timestamp, "value1")
        stream2.enqueue("channel2", timestamp + timedelta(seconds=1), "value2")

    # Verify both streams wrote their data
    mock_write = mock_connection._clients.proto_write.write_nominal_batches
    assert mock_write.call_count == 2
    # return
    # Check first call (stream1)
    first_call = mock_write.call_args_list[0].kwargs
    first_request = first_call["request"]
    assert len(first_request.series) == 1
    assert first_request.series[0].channel.name == "channel1"
    assert first_request.series[0].points.HasField("double_points")
    double_points = first_request.series[0].points.double_points.points
    assert len(double_points) == 2
    assert double_points[0].value == 42.0
    assert double_points[1].value == 43.0

    # Check second call (stream2)
    second_call = mock_write.call_args_list[1].kwargs
    second_request = second_call["request"]
    assert len(second_request.series) == 1
    assert second_request.series[0].channel.name == "channel2"
    assert second_request.series[0].points.HasField("string_points")
    string_points = second_request.series[0].points.string_points.points
    assert len(string_points) == 2
    assert string_points[0].value == "value1"
    assert string_points[1].value == "value2"
