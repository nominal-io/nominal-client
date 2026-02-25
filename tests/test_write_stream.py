from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from nominal.core._columnar_write_pb2 import WriteBatchesRequest
from nominal.core._batch_processor_proto import process_batch
from nominal.core.connection import StreamingConnection
from nominal.core.stream import BatchItem
from nominal.ts import _SecondsNanos


@pytest.fixture
def mock_clients():
    clients = MagicMock()
    clients.storage_writer = MagicMock()
    clients.auth_header = "test-auth-header"
    clients.proto_write = MagicMock()
    return clients


@pytest.fixture
def mock_connection(mock_clients):
    return StreamingConnection(
        rid="test-connection-rid",
        name="Test Connection",
        description="A connection for testing",
        _tags={},
        _clients=mock_clients,
        nominal_data_source_rid="test-datasource-rid",
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
        nominal_data_source_rid=mock_connection.nominal_data_source_rid,
        auth_header=mock_connection._clients.auth_header,
        proto_write=mock_connection._clients.proto_write,
    )

    # Get the actual request that was sent
    mock_write = mock_connection._clients.proto_write.write_nominal_columnar_batches
    mock_write.assert_called_once()

    # Check the arguments using kwargs
    kwargs = mock_write.call_args.kwargs
    assert kwargs["auth_header"] == "test-auth-header"
    actual_request = kwargs["request"]

    # Convert bytes back to WriteBatchesRequest
    actual_request = WriteBatchesRequest.FromString(actual_request)

    # Verify it's the correct type
    assert isinstance(actual_request, WriteBatchesRequest)

    # Verify data_source_rid is in the proto body
    assert actual_request.data_source_rid == "test-datasource-rid"

    # Verify batches structure (columnar format)
    assert len(actual_request.batches) == 1
    batch_proto = actual_request.batches[0]
    assert batch_proto.channel == "test_channel"

    # Verify points - columnar format has separate timestamps and values
    points = batch_proto.points
    assert points.HasField("double_points")
    assert not points.HasField("string_points")

    # Verify timestamps
    assert len(points.timestamps) == 2
    expected_ts1 = _SecondsNanos.from_datetime(timestamp)
    expected_ts2 = _SecondsNanos.from_datetime(timestamp + timedelta(seconds=1))
    assert points.timestamps[0].seconds == expected_ts1.seconds
    assert points.timestamps[0].nanos == expected_ts1.nanos
    assert points.timestamps[1].seconds == expected_ts2.seconds
    assert points.timestamps[1].nanos == expected_ts2.nanos

    # Verify values (packed array)
    assert list(points.double_points.points) == [42.0, 43.0]


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
        nominal_data_source_rid=mock_connection.nominal_data_source_rid,
        auth_header=mock_connection._clients.auth_header,
        proto_write=mock_connection._clients.proto_write,
    )

    # Get the actual request that was sent
    mock_write = mock_connection._clients.proto_write.write_nominal_columnar_batches
    mock_write.assert_called_once()

    kwargs = mock_write.call_args.kwargs
    assert kwargs["auth_header"] == "test-auth-header"
    actual_request = kwargs["request"]

    actual_request = WriteBatchesRequest.FromString(actual_request)

    # Verify batches structure
    assert len(actual_request.batches) == 1
    batch_proto = actual_request.batches[0]

    # Verify points - columnar format
    points = batch_proto.points
    assert points.HasField("string_points")
    assert not points.HasField("double_points")

    # Verify timestamps
    assert len(points.timestamps) == 2

    # Verify values (packed array)
    assert list(points.string_points.points) == ["value1", "value2"]


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
        nominal_data_source_rid=mock_connection.nominal_data_source_rid,
        auth_header=mock_connection._clients.auth_header,
        proto_write=mock_connection._clients.proto_write,
    )

    # Get the actual request that was sent
    mock_write = mock_connection._clients.proto_write.write_nominal_columnar_batches
    mock_write.assert_called_once()

    kwargs = mock_write.call_args.kwargs
    actual_request = WriteBatchesRequest.FromString(kwargs["request"])

    # Verify tags were included
    assert len(actual_request.batches) == 1
    batch_proto = actual_request.batches[0]
    assert dict(batch_proto.tags) == {"tag1": "value1"}


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
            nominal_data_source_rid=mock_connection.nominal_data_source_rid,
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
        nominal_data_source_rid=mock_connection.nominal_data_source_rid,
        auth_header=mock_connection._clients.auth_header,
        proto_write=mock_connection._clients.proto_write,
    )

    # Get the actual request that was sent
    mock_write = mock_connection._clients.proto_write.write_nominal_columnar_batches
    mock_write.assert_called_once()

    kwargs = mock_write.call_args.kwargs
    assert kwargs["auth_header"] == "test-auth-header"
    actual_request = WriteBatchesRequest.FromString(kwargs["request"])

    # Verify data_source_rid in proto body
    assert actual_request.data_source_rid == "test-datasource-rid"

    # Verify we have three batches
    assert len(actual_request.batches) == 3

    # Check channel1 (double points, columnar)
    batch1 = [b for b in actual_request.batches if b.channel == "channel1"][0]
    assert batch1.points.HasField("double_points")
    assert list(batch1.points.double_points.points) == [42.0, 43.0]
    assert len(batch1.points.timestamps) == 2

    # Check channel2 (string points, columnar)
    batch2 = [b for b in actual_request.batches if b.channel == "channel2"][0]
    assert batch2.points.HasField("string_points")
    assert list(batch2.points.string_points.points) == ["value1", "value2"]
    assert len(batch2.points.timestamps) == 2

    # Check channel3 (double points with tags, columnar)
    batch3 = [b for b in actual_request.batches if b.channel == "channel3"][0]
    assert batch3.points.HasField("double_points")
    assert dict(batch3.tags) == {"tag1": "value1"}
    assert list(batch3.points.double_points.points) == [100.0]
    assert len(batch3.points.timestamps) == 1


def test_multiple_write_streams(mock_connection):
    # Create test data with fixed timestamp
    timestamp = datetime(2024, 1, 1, 12, 0, 0)

    # First stream
    with mock_connection.get_write_stream(
        batch_size=2, max_wait=timedelta(seconds=1), data_format="protobuf"
    ) as stream1:
        stream1.enqueue("channel1", timestamp, 42.0)
        stream1.enqueue("channel1", timestamp + timedelta(seconds=1), 43.0)

    # Second stream
    with mock_connection.get_write_stream(
        batch_size=2, max_wait=timedelta(seconds=1), data_format="protobuf"
    ) as stream2:
        stream2.enqueue("channel2", timestamp, "value1")
        stream2.enqueue("channel2", timestamp + timedelta(seconds=1), "value2")

    # Verify both streams wrote their data
    mock_write = mock_connection._clients.proto_write.write_nominal_columnar_batches
    assert mock_write.call_count == 2

    # Check first call (stream1) - columnar format
    first_call = mock_write.call_args_list[0].kwargs
    first_request = WriteBatchesRequest.FromString(first_call["request"])

    assert first_request.data_source_rid == "test-datasource-rid"
    assert len(first_request.batches) == 1
    assert first_request.batches[0].channel == "channel1"
    assert first_request.batches[0].points.HasField("double_points")
    assert list(first_request.batches[0].points.double_points.points) == [42.0, 43.0]
    assert len(first_request.batches[0].points.timestamps) == 2

    # Check second call (stream2) - columnar format
    second_call = mock_write.call_args_list[1].kwargs
    second_request = WriteBatchesRequest.FromString(second_call["request"])

    assert second_request.data_source_rid == "test-datasource-rid"
    assert len(second_request.batches) == 1
    assert second_request.batches[0].channel == "channel2"
    assert second_request.batches[0].points.HasField("string_points")
    assert list(second_request.batches[0].points.string_points.points) == ["value1", "value2"]
    assert len(second_request.batches[0].points.timestamps) == 2
