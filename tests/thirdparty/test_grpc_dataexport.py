import gzip
import io
from dataclasses import replace
from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from nominal.core._utils.dataexport import export_timestamp_format
from nominal.core.channel import Channel, ChannelDataType
from nominal.core.dataset import Dataset
from nominal.protos.dataexports.v1 import data_exports_pb2 as export_pb
from nominal.protos.types.time import time_pb2
from nominal.thirdparty.matlab._matlab import export_channels_to_matlab
from nominal.thirdparty.pandas._pandas import channel_to_series, datasource_to_dataframe
from nominal.thirdparty.polars.polars_export_handler import PolarsExportHandler, _export_job, _ExportJob, _TimeRange
from nominal.ts import Epoch, Relative


def _channel(clients):
    return Channel(
        name="value",
        data_source="dataset",
        data_type=ChannelDataType.DOUBLE,
        unit=None,
        description=None,
        _clients=clients,
    )


def _serve_bytes(clients, data):
    call = clients.grpc_dataexport.StreamExport.return_value
    call.__next__.side_effect = [
        export_pb.StreamExportResponse(data=data[:7]),
        export_pb.StreamExportResponse(data=data[7:]),
        StopIteration,
    ]
    return call


@pytest.mark.parametrize("compressed", [False, True])
def test_channel_csv_preserves_gzip_tags_and_nanosecond_bounds(compressed):
    """Channel CSV consumers parse chunked exports and retain compression, tags, and exact bounds."""
    clients = MagicMock()
    csv = b"timestamp,value\n2026-01-01T00:00:00Z,1.5\n"
    call = _serve_bytes(clients, gzip.compress(csv) if compressed else csv)
    result = channel_to_series(
        _channel(clients), start=-1, end=1_700_000_000_123_456_789, tags={"vehicle": "a"}, enable_gzip=compressed
    )
    assert result.tolist() == [1.5]
    request = clients.grpc_dataexport.StreamExport.call_args.args[0].definition
    assert request.start_time.ToNanoseconds() == -1
    assert request.end_time.ToNanoseconds() == 1_700_000_000_123_456_789
    selection = request.time_domain.columns[0].series.numeric.channel_selection.data_source
    assert selection.data_source.literal == "dataset"
    assert selection.tags.exact_values.values["vehicle"].literal == "a"
    assert request.compression == (
        export_pb.EXPORT_COMPRESSION_GZIP if compressed else export_pb.EXPORT_COMPRESSION_UNSPECIFIED
    )
    call.cancel.assert_called_once()


def test_polars_stream_retains_schema_and_epoch_timestamp_units():
    """Polars reads chunked gzip CSV and keeps typed values and numeric epoch timestamps."""
    clients = MagicMock()
    client = MagicMock(_clients=clients)
    client.get_dataset.return_value.get_channels.return_value = [_channel(clients)]
    call = _serve_bytes(clients, gzip.compress(b"timestamp,value\n0,1.5\n1,2.5\n"))
    job = _ExportJob(
        datasource_rid="dataset",
        channel_names=["value"],
        channel_types={"value": ChannelDataType.DOUBLE},
        time_slice=_TimeRange(0, 10),
        tags={},
    )
    result = _export_job(job, client)
    assert result["value"].to_list() == [1.5, 2.5]
    definition = clients.grpc_dataexport.StreamExport.call_args.args[0].definition
    assert definition.time_domain.timestamp_format.relative.unit == time_pb2.SECONDS
    assert definition.end_time.ToNanoseconds() == 10
    call.cancel.assert_called_once()


def test_dataframe_batches_numeric_and_categorical_columns():
    """Dataframe batch exports retain typed channel expressions and parse the shared timestamp index."""
    clients = MagicMock()
    dataset = Dataset(
        rid="dataset",
        name="test",
        description=None,
        bounds=None,
        properties={},
        labels=[],
        is_archived=False,
        _clients=clients,
    )
    numeric = _channel(clients)
    categorical = Channel(
        name="state",
        data_source="dataset",
        data_type=ChannelDataType.STRING,
        unit=None,
        description=None,
        _clients=clients,
    )
    call = _serve_bytes(clients, b"timestamp,value,state\n2026-01-01T00:00:00Z,1.5,on\n")
    result = datasource_to_dataframe(dataset, channels=[numeric, categorical], enable_gzip=False, num_workers=1)
    assert result["value"].tolist() == [1.5]
    assert result["state"].tolist() == ["on"]
    assert result.index.name == "timestamp"
    columns = clients.grpc_dataexport.StreamExport.call_args.args[0].definition.time_domain.columns
    assert [column.name for column in columns] == ["value", "state"]
    assert columns[0].series.HasField("numeric")
    assert columns[1].series.HasField("categorical")
    assert not columns[0].series.numeric.channel_selection.data_source.HasField("tags")
    call.cancel.assert_called_once()


def test_csv_parser_failure_cancels_export():
    """Consumer parse failures close the file-like stream and cancel the RPC."""
    clients = MagicMock()
    call = _serve_bytes(clients, b"invalid gzip")
    with pytest.raises(gzip.BadGzipFile):
        channel_to_series(_channel(clients))
    call.cancel.assert_called_once()


@pytest.mark.parametrize("lookback", [0, 123])
def test_matlab_retains_large_exports_and_zero_forward_fill(tmp_path, lookback):
    """MATLAB keeps legacy support for indivisible exports over 1000 columns and zero forward-fill windows."""
    clients = MagicMock()
    client = MagicMock(_clients=clients)
    clients.dataexport.export_channel_data.return_value = io.BytesIO(gzip.compress(b"matfile payload"))
    channels = [replace(_channel(clients), name=f"channel{i}") for i in range(1001)]
    output = tmp_path / "nested" / "result.mat"
    export_channels_to_matlab(
        client, output, channels, resolution=1_000_000_001, forward_fill_lookback=lookback, chunk_size=3
    )
    assert output.read_bytes() == b"matfile payload"
    request = clients.dataexport.export_channel_data.call_args.args[1]
    assert request.format.matfile is not None
    assert request.resolution.nanoseconds == 1_000_000_001
    assert len(request.channels.time_domain.channels) == 1001
    assert (
        request.channels.time_domain.merge_timestamp_strategy.all_timestamps_forward_fill.look_back_period.nanos
        == lookback
    )
    clients.grpc_dataexport.StreamExport.assert_not_called()


@pytest.mark.parametrize("timestamp_type", [Epoch("milliseconds"), Relative("milliseconds", start=-1)])
def test_numeric_timestamp_format_preserves_units_and_epoch_presence(timestamp_type):
    """Epoch and relative timestamp formats preserve unit selection and explicit zero offsets."""
    result = export_timestamp_format(timestamp_type)
    assert result.relative.unit == time_pb2.MILLISECONDS
    assert result.relative.HasField("relative_to")
    assert result.relative.relative_to.ToNanoseconds() == (-1 if isinstance(timestamp_type, Relative) else 0)


@pytest.mark.parametrize(("name", "tags"), [("x" * 1001, None), ("value", {str(i): "v" for i in range(257)})])
def test_csv_uses_legacy_for_valid_inputs_outside_proto_limits(name, tags):
    """Long existing channel names and large tag maps remain exportable without changing CSV headers."""
    clients = MagicMock()
    channel = replace(_channel(clients), name=name)
    clients.dataexport.export_channel_data.return_value = io.BytesIO(
        f"timestamp,{name}\n2026-01-01T00:00:00Z,1.5\n".encode()
    )
    result = channel_to_series(channel, tags=tags, enable_gzip=False)
    assert result.name == name
    assert result.tolist() == [1.5]
    clients.grpc_dataexport.StreamExport.assert_not_called()
    request = clients.dataexport.export_channel_data.call_args.args[1]
    assert request.channels.time_domain.channels[0].column_name == name


def test_negative_export_bucket_count_preserves_one_nanosecond_resolution():
    """Legacy negative bucket counts evaluate to a one-nanosecond step, which protobuf can represent directly."""
    clients = MagicMock()
    client = MagicMock(_clients=clients)
    client.get_dataset.return_value.get_channels.return_value = [_channel(clients)]
    _serve_bytes(clients, gzip.compress(b"timestamp,value\n0,1.5\n"))
    job = _ExportJob(
        datasource_rid="dataset",
        channel_names=["value"],
        channel_types={"value": ChannelDataType.DOUBLE},
        time_slice=_TimeRange(0, 10),
        tags={},
        buckets=-2,
    )
    assert _export_job(job, client)["value"].to_list() == [1.5]
    definition = clients.grpc_dataexport.StreamExport.call_args.args[0].definition
    assert definition.resolution.WhichOneof("resolution") == "step"
    assert (definition.resolution.step.seconds, definition.resolution.step.nanos) == (0, 1)


def test_dataframe_caps_request_size_without_losing_columns():
    """A requested batch over the protobuf limit is split while preserving all dataframe columns."""
    clients = MagicMock()
    dataset = Dataset(
        rid="dataset",
        name="test",
        description=None,
        bounds=None,
        properties={},
        labels=[],
        is_archived=False,
        _clients=clients,
    )
    channels = [replace(_channel(clients), name=f"channel{i}") for i in range(1001)]

    def respond(request):
        names = [column.name for column in request.definition.time_domain.columns]
        data = (
            "timestamp," + ",".join(names) + "\n2026-01-01T00:00:00Z," + ",".join("1" for _ in names) + "\n"
        ).encode()
        call = MagicMock()
        call.__next__.side_effect = [export_pb.StreamExportResponse(data=data), StopIteration]
        return call

    clients.grpc_dataexport.StreamExport.side_effect = respond
    result = datasource_to_dataframe(
        dataset, channels=channels, channel_batch_size=2000, enable_gzip=False, num_workers=1
    )
    assert set(result.columns) == {channel.name for channel in channels}
    assert result.shape == (1, 1001)
    assert [
        len(call.args[0].definition.time_domain.columns) for call in clients.grpc_dataexport.StreamExport.call_args_list
    ] == [1000, 1]


def test_polars_caps_planned_channel_groups_without_losing_columns():
    """Polars' configurable group size cannot exceed the export limit, and every channel remains scheduled."""
    clients = MagicMock()
    channels = [replace(_channel(clients), name=f"channel{i}") for i in range(1001)]
    handler = PolarsExportHandler(MagicMock(), channels_per_request=2000, points_per_request=100000)
    time_range = _TimeRange(0, 1_000_000_000)
    jobs = handler._build_jobs_for_datasource(
        datasource_rid="dataset",
        channels=channels,
        points_per_second={(channel.data_source, channel.name): 1.0 for channel in channels},
        time_slices=[time_range],
        batch_duration=timedelta(seconds=1),
        timestamp_type="epoch_seconds",
        tags=None,
        buckets=None,
        resolution=None,
    )[time_range]
    assert [len(job.channel_names) for job in jobs] == [1000, 1]
    assert {name for job in jobs for name in job.channel_names} == {channel.name for channel in channels}
