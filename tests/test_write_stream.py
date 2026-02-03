from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from nominal_api_protos.nominal_write_pb2 import (
    Series,
    WriteRequestNominal,
)

from nominal.core._stream.batch_processor_proto import process_batch
from nominal.core._stream.write_stream import BatchItem
from nominal.core.connection import StreamingConnection
from nominal.core.dataset import Dataset
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


def dt_to_nano(dt: datetime) -> IntegralNanosecondsUTC:
    return _SecondsNanos.from_datetime(dt).to_nanoseconds()


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
    mock_write = mock_connection._clients.proto_write.write_nominal_batches
    mock_write.assert_called_once()

    # Check the arguments using kwargs instead of args
    kwargs = mock_write.call_args.kwargs
    assert kwargs["auth_header"] == "test-auth-header"
    assert kwargs["data_source_rid"] == "test-datasource-rid"
    actual_request = kwargs["request"]

    # Convert bytes back to WriteRequestNominal
    actual_request = WriteRequestNominal.FromString(actual_request)

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
    mock_write = mock_connection._clients.proto_write.write_nominal_batches
    mock_write.assert_called_once()

    # Check the arguments using kwargs instead of args
    kwargs = mock_write.call_args.kwargs
    assert kwargs["auth_header"] == "test-auth-header"
    assert kwargs["data_source_rid"] == "test-datasource-rid"
    actual_request = kwargs["request"]

    actual_request = WriteRequestNominal.FromString(actual_request)

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
    mock_write = mock_connection._clients.proto_write.write_nominal_batches
    mock_write.assert_called_once()

    # Check the arguments using kwargs instead of args
    kwargs = mock_write.call_args.kwargs
    assert kwargs["auth_header"] == "test-auth-header"
    assert kwargs["data_source_rid"] == "test-datasource-rid"
    actual_request = kwargs["request"]

    actual_request = WriteRequestNominal.FromString(actual_request)

    # Verify tags were included
    assert len(actual_request.series) == 1
    series = actual_request.series[0]
    assert series.tags == {"tag1": "value1"}


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
    mock_write = mock_connection._clients.proto_write.write_nominal_batches
    mock_write.assert_called_once()

    # Check the basic arguments
    kwargs = mock_write.call_args.kwargs
    assert kwargs["auth_header"] == "test-auth-header"
    assert kwargs["data_source_rid"] == "test-datasource-rid"
    actual_request = kwargs["request"]

    actual_request = WriteRequestNominal.FromString(actual_request)

    # Verify series structure
    assert len(actual_request.series) == 1
    series = actual_request.series[0]
    assert series.channel.name == "test_channel"

    # Verify int points
    assert series.points.HasField("integer_points")
    assert not series.points.HasField("double_points")
    assert not series.points.HasField("string_points")

    int_points = series.points.integer_points.points
    assert len(int_points) == 2
    assert int_points[0].value == 42
    assert int_points[1].value == 43


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
    mock_write = mock_connection._clients.proto_write.write_nominal_batches
    mock_write.assert_called_once()

    # Check the basic arguments
    kwargs = mock_write.call_args.kwargs
    assert kwargs["auth_header"] == "test-auth-header"
    assert kwargs["data_source_rid"] == "test-datasource-rid"
    actual_request = kwargs["request"]

    actual_request = WriteRequestNominal.FromString(actual_request)

    # Verify we have four series
    assert len(actual_request.series) == 4

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

    # Check channel4 (int points)
    series4 = [s for s in actual_request.series if s.channel.name == "channel4"][0]
    assert series4.points.HasField("integer_points")
    int_points = series4.points.integer_points.points
    assert len(int_points) == 2
    assert int_points[0].value == 10
    assert int_points[1].value == 20


def test_multiple_write_streams(mock_connection):
    # Create test data with fixed timestamp
    timestamp = datetime(2024, 1, 1, 12, 0, 0)

    # First stream
    with mock_connection.get_write_stream(
        batch_size=2, max_wait=timedelta(seconds=1), data_format="protobuf"
    ) as stream1:
        stream1.enqueue("channel1", timestamp, 42.0)
        stream1.enqueue("channel1", timestamp + timedelta(seconds=1), 43.0)
        # Force a small sleep to allow the batch to be processed

    # Second stream
    with mock_connection.get_write_stream(
        batch_size=2, max_wait=timedelta(seconds=1), data_format="protobuf"
    ) as stream2:
        stream2.enqueue("channel2", timestamp, "value1")
        stream2.enqueue("channel2", timestamp + timedelta(seconds=1), "value2")

    # Verify both streams wrote their data
    mock_write = mock_connection._clients.proto_write.write_nominal_batches
    assert mock_write.call_count == 2
    # return
    # Check first call (stream1)
    first_call = mock_write.call_args_list[0].kwargs
    first_request = first_call["request"]

    first_request = WriteRequestNominal.FromString(first_request)

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

    second_request = WriteRequestNominal.FromString(second_request)

    assert len(second_request.series) == 1
    assert second_request.series[0].channel.name == "channel2"
    assert second_request.series[0].points.HasField("string_points")
    string_points = second_request.series[0].points.string_points.points
    assert len(string_points) == 2
    assert string_points[0].value == "value1"
    assert string_points[1].value == "value2"


def test_process_batch_double_points_dataset(mock_dataset):
    # Create test data with fixed timestamp
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("test_channel", dt_to_nano(timestamp), 42.0),
        BatchItem("test_channel", dt_to_nano(timestamp + timedelta(seconds=1)), 43.0),
    ]

    # Process the batch using the imported process_batch function
    process_batch(
        batch=batch,
        nominal_data_source_rid=mock_dataset.rid,
        auth_header=mock_dataset._clients.auth_header,
        proto_write=mock_dataset._clients.proto_write,
    )

    # Get the actual request that was sent
    mock_write = mock_dataset._clients.proto_write.write_nominal_batches
    mock_write.assert_called_once()

    # Check the arguments using kwargs instead of args
    kwargs = mock_write.call_args.kwargs
    assert kwargs["auth_header"] == "test-auth-header"
    assert kwargs["data_source_rid"] == "test-dataset-rid"
    actual_request = kwargs["request"]

    # Convert bytes back to WriteRequestNominal
    actual_request = WriteRequestNominal.FromString(actual_request)

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


def test_process_batch_string_points_dataset(mock_dataset):
    # Create test data with fixed timestamp
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("test_channel", dt_to_nano(timestamp), "value1"),
        BatchItem("test_channel", dt_to_nano(timestamp + timedelta(seconds=1)), "value2"),
    ]

    # Process the batch using the imported process_batch function
    process_batch(
        batch=batch,
        nominal_data_source_rid=mock_dataset.rid,
        auth_header=mock_dataset._clients.auth_header,
        proto_write=mock_dataset._clients.proto_write,
    )

    # Get the actual request that was sent
    mock_write = mock_dataset._clients.proto_write.write_nominal_batches
    mock_write.assert_called_once()

    # Check the arguments using kwargs instead of args
    kwargs = mock_write.call_args.kwargs
    assert kwargs["auth_header"] == "test-auth-header"
    assert kwargs["data_source_rid"] == "test-dataset-rid"
    actual_request = kwargs["request"]

    actual_request = WriteRequestNominal.FromString(actual_request)

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


def test_process_batch_with_tags_dataset(mock_dataset):
    # Create test data with fixed timestamp
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("test_channel", dt_to_nano(timestamp), 42.0, {"tag1": "value1"}),
        BatchItem("test_channel", dt_to_nano(timestamp + timedelta(seconds=1)), 43.0, {"tag1": "value1"}),
    ]

    # Process the batch using the imported process_batch function
    process_batch(
        batch=batch,
        nominal_data_source_rid=mock_dataset.rid,
        auth_header=mock_dataset._clients.auth_header,
        proto_write=mock_dataset._clients.proto_write,
    )

    # Get the actual request that was sent
    mock_write = mock_dataset._clients.proto_write.write_nominal_batches
    mock_write.assert_called_once()

    # Check the arguments using kwargs instead of args
    kwargs = mock_write.call_args.kwargs
    assert kwargs["auth_header"] == "test-auth-header"
    assert kwargs["data_source_rid"] == "test-dataset-rid"
    actual_request = kwargs["request"]

    actual_request = WriteRequestNominal.FromString(actual_request)

    # Verify tags were included
    assert len(actual_request.series) == 1
    series = actual_request.series[0]
    assert series.tags == {"tag1": "value1"}


def test_process_batch_invalid_type_dataset(mock_dataset):
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
            nominal_data_source_rid=mock_dataset.rid,
            auth_header=mock_dataset._clients.auth_header,
            proto_write=mock_dataset._clients.proto_write,
        )


def test_process_batch_int_points_dataset(mock_dataset):
    # Create test data with fixed timestamp
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("test_channel", dt_to_nano(timestamp), 42),
        BatchItem("test_channel", dt_to_nano(timestamp + timedelta(seconds=1)), 43),
    ]

    # Process the batch
    process_batch(
        batch=batch,
        nominal_data_source_rid=mock_dataset.rid,
        auth_header=mock_dataset._clients.auth_header,
        proto_write=mock_dataset._clients.proto_write,
    )

    # Get the actual request that was sent
    mock_write = mock_dataset._clients.proto_write.write_nominal_batches
    mock_write.assert_called_once()

    # Check the basic arguments
    kwargs = mock_write.call_args.kwargs
    assert kwargs["auth_header"] == "test-auth-header"
    assert kwargs["data_source_rid"] == "test-dataset-rid"
    actual_request = kwargs["request"]

    actual_request = WriteRequestNominal.FromString(actual_request)

    # Verify series structure
    assert len(actual_request.series) == 1
    series = actual_request.series[0]
    assert series.channel.name == "test_channel"

    # Verify int points
    assert series.points.HasField("integer_points")
    assert not series.points.HasField("double_points")
    assert not series.points.HasField("string_points")

    int_points = series.points.integer_points.points
    assert len(int_points) == 2
    assert int_points[0].value == 42
    assert int_points[1].value == 43


def test_process_batch_multiple_channels_dataset(mock_dataset):
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
        nominal_data_source_rid=mock_dataset.rid,
        auth_header=mock_dataset._clients.auth_header,
        proto_write=mock_dataset._clients.proto_write,
    )

    # Get the actual request that was sent
    mock_write = mock_dataset._clients.proto_write.write_nominal_batches
    mock_write.assert_called_once()

    # Check the basic arguments
    kwargs = mock_write.call_args.kwargs
    assert kwargs["auth_header"] == "test-auth-header"
    assert kwargs["data_source_rid"] == "test-dataset-rid"
    actual_request = kwargs["request"]

    actual_request = WriteRequestNominal.FromString(actual_request)

    # Verify we have four series
    assert len(actual_request.series) == 4

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

    # Check channel4 (int points)
    series4 = [s for s in actual_request.series if s.channel.name == "channel4"][0]
    assert series4.points.HasField("integer_points")
    int_points = series4.points.integer_points.points
    assert len(int_points) == 2
    assert int_points[0].value == 10
    assert int_points[1].value == 20


def test_multiple_write_streams_dataset(mock_dataset):
    # Create test data with fixed timestamp
    timestamp = datetime(2024, 1, 1, 12, 0, 0)

    # First stream
    with mock_dataset.get_write_stream(batch_size=2, max_wait=timedelta(seconds=1), data_format="protobuf") as stream1:
        stream1.enqueue("channel1", timestamp, 42.0)
        stream1.enqueue("channel1", timestamp + timedelta(seconds=1), 43.0)
        # Force a small sleep to allow the batch to be processed

    # Second stream
    with mock_dataset.get_write_stream(batch_size=2, max_wait=timedelta(seconds=1), data_format="protobuf") as stream2:
        stream2.enqueue("channel2", timestamp, "value1")
        stream2.enqueue("channel2", timestamp + timedelta(seconds=1), "value2")

    # Verify both streams wrote their data
    mock_write = mock_dataset._clients.proto_write.write_nominal_batches
    assert mock_write.call_count == 2
    # return
    # Check first call (stream1)
    first_call = mock_write.call_args_list[0].kwargs
    first_request = first_call["request"]

    first_request = WriteRequestNominal.FromString(first_request)

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

    second_request = WriteRequestNominal.FromString(second_request)

    assert len(second_request.series) == 1
    assert second_request.series[0].channel.name == "channel2"
    assert second_request.series[0].points.HasField("string_points")
    string_points = second_request.series[0].points.string_points.points
    assert len(string_points) == 2
    assert string_points[0].value == "value1"
    assert string_points[1].value == "value2"


# ============== Array Streaming Tests ==============


def test_process_batch_float_arrays(mock_connection):
    """Test processing a batch of float array items using unified BatchItem."""
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("test_channel", dt_to_nano(timestamp), [1.0, 2.0, 3.0]),
        BatchItem("test_channel", dt_to_nano(timestamp + timedelta(seconds=1)), [4.0, 5.0, 6.0]),
    ]

    # Process the batch using the unified process_batch function
    process_batch(
        batch=batch,
        nominal_data_source_rid=mock_connection.nominal_data_source_rid,
        auth_header=mock_connection._clients.auth_header,
        proto_write=mock_connection._clients.proto_write,
    )

    # Get the actual request that was sent
    mock_write = mock_connection._clients.proto_write.write_nominal_batches
    mock_write.assert_called_once()

    # Check the arguments using kwargs
    kwargs = mock_write.call_args.kwargs
    assert kwargs["auth_header"] == "test-auth-header"
    assert kwargs["data_source_rid"] == "test-datasource-rid"
    actual_request = kwargs["request"]

    # Convert bytes back to WriteRequestNominal
    actual_request = WriteRequestNominal.FromString(actual_request)

    # Verify it's the correct type
    assert isinstance(actual_request, WriteRequestNominal)

    # Verify series structure
    assert len(actual_request.series) == 1
    series = actual_request.series[0]
    assert isinstance(series, Series)
    assert series.channel.name == "test_channel"

    # Verify points - should be array_points
    points = series.points
    assert points.HasField("array_points")
    assert points.array_points.HasField("double_array_points")

    double_array_points = points.array_points.double_array_points.points
    assert len(double_array_points) == 2

    # Verify individual point values
    assert list(double_array_points[0].value) == [1.0, 2.0, 3.0]
    assert list(double_array_points[1].value) == [4.0, 5.0, 6.0]


def test_process_batch_string_arrays(mock_connection):
    """Test processing a batch of string array items using unified BatchItem."""
    timestamp = datetime(2024, 1, 1, 12, 0, 0)
    batch = [
        BatchItem("test_channel", dt_to_nano(timestamp), ["a", "b", "c"]),
        BatchItem("test_channel", dt_to_nano(timestamp + timedelta(seconds=1)), ["d", "e", "f"]),
    ]

    # Process the batch
    process_batch(
        batch=batch,
        nominal_data_source_rid=mock_connection.nominal_data_source_rid,
        auth_header=mock_connection._clients.auth_header,
        proto_write=mock_connection._clients.proto_write,
    )

    # Get the actual request that was sent
    mock_write = mock_connection._clients.proto_write.write_nominal_batches
    mock_write.assert_called_once()

    kwargs = mock_write.call_args.kwargs
    actual_request = WriteRequestNominal.FromString(kwargs["request"])

    # Verify series structure
    assert len(actual_request.series) == 1
    series = actual_request.series[0]
    assert series.channel.name == "test_channel"

    # Verify points - should be array_points with string arrays
    points = series.points
    assert points.HasField("array_points")
    assert points.array_points.HasField("string_array_points")

    string_array_points = points.array_points.string_array_points.points
    assert len(string_array_points) == 2

    # Verify individual point values
    assert list(string_array_points[0].value) == ["a", "b", "c"]
    assert list(string_array_points[1].value) == ["d", "e", "f"]


def test_process_batch_arrays_with_tags(mock_connection):
    """Test processing array items with tags using unified BatchItem."""
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

    mock_write = mock_connection._clients.proto_write.write_nominal_batches
    mock_write.assert_called_once()

    kwargs = mock_write.call_args.kwargs
    actual_request = WriteRequestNominal.FromString(kwargs["request"])

    # Verify tags were included
    assert len(actual_request.series) == 1
    series = actual_request.series[0]
    assert series.tags == {"tag1": "value1"}


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


def test_write_stream_enqueue_float_array(mock_dataset):
    """Test enqueue_float_array on a write stream."""
    timestamp = datetime(2024, 1, 1, 12, 0, 0)

    with mock_dataset.get_write_stream(batch_size=2, max_wait=timedelta(seconds=1), data_format="protobuf") as stream:
        stream.enqueue_float_array("channel1", timestamp, [1.0, 2.0, 3.0])
        stream.enqueue_float_array("channel1", timestamp + timedelta(seconds=1), [4.0, 5.0, 6.0])

    # Verify the write was called
    mock_write = mock_dataset._clients.proto_write.write_nominal_batches
    assert mock_write.call_count >= 1

    # Check the last call contains array data
    last_call = mock_write.call_args_list[-1].kwargs
    actual_request = WriteRequestNominal.FromString(last_call["request"])

    # Find the series with array points
    array_series = [s for s in actual_request.series if s.points.HasField("array_points")]
    assert len(array_series) >= 1

    series = array_series[0]
    assert series.channel.name == "channel1"
    assert series.points.array_points.HasField("double_array_points")


def test_write_stream_enqueue_string_array(mock_dataset):
    """Test enqueue_string_array on a write stream."""
    timestamp = datetime(2024, 1, 1, 12, 0, 0)

    with mock_dataset.get_write_stream(batch_size=2, max_wait=timedelta(seconds=1), data_format="protobuf") as stream:
        stream.enqueue_string_array("channel1", timestamp, ["a", "b", "c"])
        stream.enqueue_string_array("channel1", timestamp + timedelta(seconds=1), ["d", "e", "f"])

    # Verify the write was called
    mock_write = mock_dataset._clients.proto_write.write_nominal_batches
    assert mock_write.call_count >= 1

    # Check the last call contains array data
    last_call = mock_write.call_args_list[-1].kwargs
    actual_request = WriteRequestNominal.FromString(last_call["request"])

    # Find the series with array points
    array_series = [s for s in actual_request.series if s.points.HasField("array_points")]
    assert len(array_series) >= 1

    series = array_series[0]
    assert series.channel.name == "channel1"
    assert series.points.array_points.HasField("string_array_points")


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
    float_item = BatchItem("channel1", timestamp, [], point_type=PointType.DOUBLE_ARRAY)
    assert float_item.get_point_type() == PointType.DOUBLE_ARRAY

    # Empty string array with explicit type should work
    string_item = BatchItem("channel1", timestamp, [], point_type=PointType.STRING_ARRAY)
    assert string_item.get_point_type() == PointType.STRING_ARRAY
