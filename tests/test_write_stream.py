from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from nominal.core._columnar_write_pb2 import WriteBatchesRequest
from nominal.core._stream.batch_processor_proto import process_batch
from nominal.core._stream.write_stream import BatchItem
from nominal.core.connection import StreamingConnection
from nominal.core.dataset import Dataset
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


def dt_to_nano(dt: datetime) -> IntegralNanosecondsUTC:
    return _SecondsNanos.from_datetime(dt).to_nanoseconds()


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
        _clients=mock_clients,
        nominal_data_source_rid="test-datasource-rid",
    )


@pytest.fixture
def mock_dataset(mock_clients):
    return Dataset(
        rid="test-dataset-rid",
        _clients=mock_clients,
        name="test dataset",
        description="test description",
        properties={},
        labels=[],
        bounds=None,
    )


def test_process_batch_double_points(mock_connection):
    # Create test data with fixed timestamp
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("test_channel", dt_to_nano(timestamp), 42.0),
        BatchItem("test_channel", dt_to_nano(timestamp + timedelta(seconds=1)), 43.0),
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
        BatchItem("test_channel", dt_to_nano(timestamp), "value1"),
        BatchItem("test_channel", dt_to_nano(timestamp + timedelta(seconds=1)), "value2"),
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
        BatchItem("test_channel", dt_to_nano(timestamp), 42.0, {"tag1": "value1"}),
        BatchItem("test_channel", dt_to_nano(timestamp + timedelta(seconds=1)), 43.0, {"tag1": "value1"}),
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

    # Dictionaries are not supported
    batch = [
        BatchItem("test_channel", dt_to_nano(timestamp), {"key": "value"}),  # type: ignore[arg-type]
    ]

    # Verify it raises the correct error
    with pytest.raises(ValueError, match="Unsupported value type"):
        process_batch(
            batch=batch,
            nominal_data_source_rid=mock_connection.nominal_data_source_rid,
            auth_header=mock_connection._clients.auth_header,
            proto_write=mock_connection._clients.proto_write,
        )


def test_process_batch_int_points(mock_connection):
    # Create test data with fixed timestamp
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("test_channel", dt_to_nano(timestamp), 42),
        BatchItem("test_channel", dt_to_nano(timestamp + timedelta(seconds=1)), 43),
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

    # Verify batches structure (columnar format)
    assert len(actual_request.batches) == 1
    batch_proto = actual_request.batches[0]
    assert batch_proto.channel == "test_channel"

    # Verify int points (columnar)
    points = batch_proto.points
    assert points.HasField("int_points")
    assert not points.HasField("double_points")
    assert not points.HasField("string_points")

    # Verify timestamps
    assert len(points.timestamps) == 2

    # Verify values (packed array)
    assert list(points.int_points.points) == [42, 43]


def test_process_batch_multiple_channels(mock_connection):
    # Create test data with fixed timestamp
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("channel1", dt_to_nano(timestamp), 42.0),
        BatchItem("channel1", dt_to_nano(timestamp + timedelta(seconds=1)), 43.0),
        BatchItem("channel2", dt_to_nano(timestamp), "value1"),
        BatchItem("channel2", dt_to_nano(timestamp + timedelta(seconds=1)), "value2"),
        BatchItem("channel3", dt_to_nano(timestamp), 100.0, {"tag1": "value1"}),
        BatchItem("channel4", dt_to_nano(timestamp), 10),
        BatchItem("channel4", dt_to_nano(timestamp + timedelta(seconds=1)), 20),
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

    # Verify we have four batches (one per channel/type combo)
    assert len(actual_request.batches) == 4

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

    # Check channel4 (int points, columnar)
    batch4 = [b for b in actual_request.batches if b.channel == "channel4"][0]
    assert batch4.points.HasField("int_points")
    assert list(batch4.points.int_points.points) == [10, 20]
    assert len(batch4.points.timestamps) == 2


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


# ============== Dataset Tests (columnar format) ==============


def test_process_batch_double_points_dataset(mock_dataset):
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("test_channel", dt_to_nano(timestamp), 42.0),
        BatchItem("test_channel", dt_to_nano(timestamp + timedelta(seconds=1)), 43.0),
    ]

    process_batch(
        batch=batch,
        nominal_data_source_rid=mock_dataset.rid,
        auth_header=mock_dataset._clients.auth_header,
        proto_write=mock_dataset._clients.proto_write,
    )

    mock_write = mock_dataset._clients.proto_write.write_nominal_columnar_batches
    mock_write.assert_called_once()

    kwargs = mock_write.call_args.kwargs
    assert kwargs["auth_header"] == "test-auth-header"
    actual_request = WriteBatchesRequest.FromString(kwargs["request"])

    assert isinstance(actual_request, WriteBatchesRequest)
    assert actual_request.data_source_rid == "test-dataset-rid"

    assert len(actual_request.batches) == 1
    batch_proto = actual_request.batches[0]
    assert batch_proto.channel == "test_channel"

    points = batch_proto.points
    assert points.HasField("double_points")
    assert not points.HasField("string_points")

    assert len(points.timestamps) == 2
    expected_ts1 = _SecondsNanos.from_datetime(timestamp)
    expected_ts2 = _SecondsNanos.from_datetime(timestamp + timedelta(seconds=1))
    assert points.timestamps[0].seconds == expected_ts1.seconds
    assert points.timestamps[0].nanos == expected_ts1.nanos
    assert points.timestamps[1].seconds == expected_ts2.seconds
    assert points.timestamps[1].nanos == expected_ts2.nanos

    assert list(points.double_points.points) == [42.0, 43.0]


def test_process_batch_string_points_dataset(mock_dataset):
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("test_channel", dt_to_nano(timestamp), "value1"),
        BatchItem("test_channel", dt_to_nano(timestamp + timedelta(seconds=1)), "value2"),
    ]

    process_batch(
        batch=batch,
        nominal_data_source_rid=mock_dataset.rid,
        auth_header=mock_dataset._clients.auth_header,
        proto_write=mock_dataset._clients.proto_write,
    )

    mock_write = mock_dataset._clients.proto_write.write_nominal_columnar_batches
    mock_write.assert_called_once()

    kwargs = mock_write.call_args.kwargs
    actual_request = WriteBatchesRequest.FromString(kwargs["request"])

    assert len(actual_request.batches) == 1
    batch_proto = actual_request.batches[0]

    points = batch_proto.points
    assert points.HasField("string_points")
    assert not points.HasField("double_points")
    assert len(points.timestamps) == 2
    assert list(points.string_points.points) == ["value1", "value2"]


def test_process_batch_with_tags_dataset(mock_dataset):
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("test_channel", dt_to_nano(timestamp), 42.0, {"tag1": "value1"}),
        BatchItem("test_channel", dt_to_nano(timestamp + timedelta(seconds=1)), 43.0, {"tag1": "value1"}),
    ]

    process_batch(
        batch=batch,
        nominal_data_source_rid=mock_dataset.rid,
        auth_header=mock_dataset._clients.auth_header,
        proto_write=mock_dataset._clients.proto_write,
    )

    mock_write = mock_dataset._clients.proto_write.write_nominal_columnar_batches
    mock_write.assert_called_once()

    kwargs = mock_write.call_args.kwargs
    actual_request = WriteBatchesRequest.FromString(kwargs["request"])

    assert len(actual_request.batches) == 1
    batch_proto = actual_request.batches[0]
    assert dict(batch_proto.tags) == {"tag1": "value1"}


def test_process_batch_invalid_type_dataset(mock_dataset):
    timestamp = datetime(2024, 1, 1, 12, 0, 0)

    batch = [
        BatchItem("test_channel", dt_to_nano(timestamp), {"key": "value"}),  # type: ignore[arg-type]
    ]

    with pytest.raises(ValueError, match="Unsupported value type"):
        process_batch(
            batch=batch,
            nominal_data_source_rid=mock_dataset.rid,
            auth_header=mock_dataset._clients.auth_header,
            proto_write=mock_dataset._clients.proto_write,
        )


def test_process_batch_int_points_dataset(mock_dataset):
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("test_channel", dt_to_nano(timestamp), 42),
        BatchItem("test_channel", dt_to_nano(timestamp + timedelta(seconds=1)), 43),
    ]

    process_batch(
        batch=batch,
        nominal_data_source_rid=mock_dataset.rid,
        auth_header=mock_dataset._clients.auth_header,
        proto_write=mock_dataset._clients.proto_write,
    )

    mock_write = mock_dataset._clients.proto_write.write_nominal_columnar_batches
    mock_write.assert_called_once()

    kwargs = mock_write.call_args.kwargs
    actual_request = WriteBatchesRequest.FromString(kwargs["request"])

    assert actual_request.data_source_rid == "test-dataset-rid"
    assert len(actual_request.batches) == 1
    batch_proto = actual_request.batches[0]
    assert batch_proto.channel == "test_channel"

    points = batch_proto.points
    assert points.HasField("int_points")
    assert not points.HasField("double_points")
    assert len(points.timestamps) == 2
    assert list(points.int_points.points) == [42, 43]


def test_process_batch_multiple_channels_dataset(mock_dataset):
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("channel1", dt_to_nano(timestamp), 42.0),
        BatchItem("channel1", dt_to_nano(timestamp + timedelta(seconds=1)), 43.0),
        BatchItem("channel2", dt_to_nano(timestamp), "value1"),
        BatchItem("channel2", dt_to_nano(timestamp + timedelta(seconds=1)), "value2"),
        BatchItem("channel3", dt_to_nano(timestamp), 100.0, {"tag1": "value1"}),
        BatchItem("channel4", dt_to_nano(timestamp), 10),
        BatchItem("channel4", dt_to_nano(timestamp + timedelta(seconds=1)), 20),
    ]

    process_batch(
        batch=batch,
        nominal_data_source_rid=mock_dataset.rid,
        auth_header=mock_dataset._clients.auth_header,
        proto_write=mock_dataset._clients.proto_write,
    )

    mock_write = mock_dataset._clients.proto_write.write_nominal_columnar_batches
    mock_write.assert_called_once()

    kwargs = mock_write.call_args.kwargs
    actual_request = WriteBatchesRequest.FromString(kwargs["request"])

    assert len(actual_request.batches) == 4

    batch1 = [b for b in actual_request.batches if b.channel == "channel1"][0]
    assert batch1.points.HasField("double_points")
    assert list(batch1.points.double_points.points) == [42.0, 43.0]
    assert len(batch1.points.timestamps) == 2

    batch2 = [b for b in actual_request.batches if b.channel == "channel2"][0]
    assert batch2.points.HasField("string_points")
    assert list(batch2.points.string_points.points) == ["value1", "value2"]
    assert len(batch2.points.timestamps) == 2

    batch3 = [b for b in actual_request.batches if b.channel == "channel3"][0]
    assert batch3.points.HasField("double_points")
    assert dict(batch3.tags) == {"tag1": "value1"}
    assert list(batch3.points.double_points.points) == [100.0]
    assert len(batch3.points.timestamps) == 1

    batch4 = [b for b in actual_request.batches if b.channel == "channel4"][0]
    assert batch4.points.HasField("int_points")
    assert list(batch4.points.int_points.points) == [10, 20]
    assert len(batch4.points.timestamps) == 2


def test_multiple_write_streams_dataset(mock_dataset):
    timestamp = datetime(2024, 1, 1, 12, 0, 0)

    with mock_dataset.get_write_stream(batch_size=2, max_wait=timedelta(seconds=1), data_format="protobuf") as stream1:
        stream1.enqueue("channel1", timestamp, 42.0)
        stream1.enqueue("channel1", timestamp + timedelta(seconds=1), 43.0)

    with mock_dataset.get_write_stream(batch_size=2, max_wait=timedelta(seconds=1), data_format="protobuf") as stream2:
        stream2.enqueue("channel2", timestamp, "value1")
        stream2.enqueue("channel2", timestamp + timedelta(seconds=1), "value2")

    mock_write = mock_dataset._clients.proto_write.write_nominal_columnar_batches
    assert mock_write.call_count == 2

    first_call = mock_write.call_args_list[0].kwargs
    first_request = WriteBatchesRequest.FromString(first_call["request"])

    assert first_request.data_source_rid == "test-dataset-rid"
    assert len(first_request.batches) == 1
    assert first_request.batches[0].channel == "channel1"
    assert first_request.batches[0].points.HasField("double_points")
    assert list(first_request.batches[0].points.double_points.points) == [42.0, 43.0]

    second_call = mock_write.call_args_list[1].kwargs
    second_request = WriteBatchesRequest.FromString(second_call["request"])

    assert second_request.data_source_rid == "test-dataset-rid"
    assert len(second_request.batches) == 1
    assert second_request.batches[0].channel == "channel2"
    assert second_request.batches[0].points.HasField("string_points")
    assert list(second_request.batches[0].points.string_points.points) == ["value1", "value2"]


# ============== Array Streaming Tests ==============


def test_process_batch_float_arrays(mock_connection):
    """Test processing a batch of float array items in columnar format."""
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("test_channel", dt_to_nano(timestamp), [1.0, 2.0, 3.0]),
        BatchItem("test_channel", dt_to_nano(timestamp + timedelta(seconds=1)), [4.0, 5.0, 6.0]),
    ]

    process_batch(
        batch=batch,
        nominal_data_source_rid=mock_connection.nominal_data_source_rid,
        auth_header=mock_connection._clients.auth_header,
        proto_write=mock_connection._clients.proto_write,
    )

    mock_write = mock_connection._clients.proto_write.write_nominal_columnar_batches
    mock_write.assert_called_once()

    kwargs = mock_write.call_args.kwargs
    actual_request = WriteBatchesRequest.FromString(kwargs["request"])

    assert len(actual_request.batches) == 1
    batch_proto = actual_request.batches[0]
    assert batch_proto.channel == "test_channel"

    points = batch_proto.points
    assert points.HasField("array_points")
    assert points.array_points.HasField("double_array_points")

    assert len(points.timestamps) == 2
    double_array_points = points.array_points.double_array_points.points
    assert len(double_array_points) == 2
    assert list(double_array_points[0].value) == [1.0, 2.0, 3.0]
    assert list(double_array_points[1].value) == [4.0, 5.0, 6.0]


def test_process_batch_string_arrays(mock_connection):
    """Test processing a batch of string array items in columnar format."""
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("test_channel", dt_to_nano(timestamp), ["a", "b", "c"]),
        BatchItem("test_channel", dt_to_nano(timestamp + timedelta(seconds=1)), ["d", "e", "f"]),
    ]

    process_batch(
        batch=batch,
        nominal_data_source_rid=mock_connection.nominal_data_source_rid,
        auth_header=mock_connection._clients.auth_header,
        proto_write=mock_connection._clients.proto_write,
    )

    mock_write = mock_connection._clients.proto_write.write_nominal_columnar_batches
    mock_write.assert_called_once()

    kwargs = mock_write.call_args.kwargs
    actual_request = WriteBatchesRequest.FromString(kwargs["request"])

    assert len(actual_request.batches) == 1
    batch_proto = actual_request.batches[0]

    points = batch_proto.points
    assert points.HasField("array_points")
    assert points.array_points.HasField("string_array_points")

    assert len(points.timestamps) == 2
    string_array_points = points.array_points.string_array_points.points
    assert len(string_array_points) == 2
    assert list(string_array_points[0].value) == ["a", "b", "c"]
    assert list(string_array_points[1].value) == ["d", "e", "f"]


def test_process_batch_arrays_with_tags(mock_connection):
    """Test processing array items with tags in columnar format."""
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("test_channel", dt_to_nano(timestamp), [1.0, 2.0], {"tag1": "value1"}),
        BatchItem("test_channel", dt_to_nano(timestamp + timedelta(seconds=1)), [3.0, 4.0], {"tag1": "value1"}),
    ]

    process_batch(
        batch=batch,
        nominal_data_source_rid=mock_connection.nominal_data_source_rid,
        auth_header=mock_connection._clients.auth_header,
        proto_write=mock_connection._clients.proto_write,
    )

    mock_write = mock_connection._clients.proto_write.write_nominal_columnar_batches
    mock_write.assert_called_once()

    kwargs = mock_write.call_args.kwargs
    actual_request = WriteBatchesRequest.FromString(kwargs["request"])

    assert len(actual_request.batches) == 1
    batch_proto = actual_request.batches[0]
    assert dict(batch_proto.tags) == {"tag1": "value1"}


# ============== BatchItem Unit Tests ==============


def test_batch_item_sort_key_float_array():
    """Test that BatchItem.sort_key returns correct values for float arrays."""
    timestamp = dt_to_nano(datetime(2024, 1, 1, 12, 0, 0))
    item = BatchItem("channel1", timestamp, [1.0, 2.0], {"tag": "value"})

    key = BatchItem.sort_key(item)
    assert key[0] == "channel1"
    assert key[1] == [("tag", "value")]
    assert key[2] == "DOUBLE_ARRAY"


def test_batch_item_sort_key_string_array():
    """Test that BatchItem.sort_key returns correct values for string arrays."""
    timestamp = dt_to_nano(datetime(2024, 1, 1, 12, 0, 0))
    item = BatchItem("channel1", timestamp, ["a", "b"], {"tag": "value"})

    key = BatchItem.sort_key(item)
    assert key[0] == "channel1"
    assert key[1] == [("tag", "value")]
    assert key[2] == "STRING_ARRAY"


def test_empty_array_without_explicit_type_raises_error():
    """Test that creating a BatchItem with an empty array without explicit type raises an error."""
    from nominal.core._stream.write_stream import BatchItem, infer_point_type

    timestamp = dt_to_nano(datetime(2024, 1, 1, 12, 0, 0))

    # Empty array without explicit type should raise an error when getting point type
    item = BatchItem("channel1", timestamp, [])

    with pytest.raises(ValueError, match="Cannot infer type from empty array"):
        item.get_point_type()

    # Direct call to infer_point_type should also raise
    with pytest.raises(ValueError, match="Cannot infer type from empty array"):
        infer_point_type([])


def test_empty_array_with_explicit_type_works():
    """Test that creating a BatchItem with an empty array with explicit type works."""
    from nominal.core._stream.write_stream import BatchItem, PointType

    timestamp = dt_to_nano(datetime(2024, 1, 1, 12, 0, 0))

    # Empty float array with explicit type should work
    float_item = BatchItem("channel1", timestamp, [], point_type_override=PointType.DOUBLE_ARRAY)
    assert float_item.get_point_type() == PointType.DOUBLE_ARRAY

    # Empty string array with explicit type should work
    string_item = BatchItem("channel1", timestamp, [], point_type_override=PointType.STRING_ARRAY)
    assert string_item.get_point_type() == PointType.STRING_ARRAY
