from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest

from nominal.core.channel import ChannelDataType
from nominal.core.datasource import CreateChannelRequest, DataSource
from nominal.exceptions import NominalPermissionDeniedError
from nominal.protos.channelmetadata.v1 import channel_metadata_pb2 as metadata_proto
from nominal.protos.seriesmetadata.v1 import series_metadata_pb2 as proto
from nominal.protos.types import series_pb2


@pytest.fixture
def clients():
    clients = MagicMock()
    clients.series_metadata = MagicMock()
    # Default metadata reads to an empty result.
    clients.channel_metadata.BatchGetChannelMetadata.return_value.channel_metadata = []
    return clients


@pytest.fixture
def datasource(clients):
    return DataSource(rid="ri.datasource.test", _clients=clients)


@pytest.mark.parametrize(
    "data_type,expected",
    [
        (ChannelDataType.DOUBLE, proto.NOMINAL_DATA_TYPE_DOUBLE),
        (ChannelDataType.STRING, proto.NOMINAL_DATA_TYPE_STRING),
        (ChannelDataType.LOG, proto.NOMINAL_DATA_TYPE_LOG),
        (ChannelDataType.INT, proto.NOMINAL_DATA_TYPE_INT64),
        (ChannelDataType.UINT, proto.NOMINAL_DATA_TYPE_UINT64),
        (ChannelDataType.DOUBLE_ARRAY, proto.NOMINAL_DATA_TYPE_DOUBLE_ARRAY),
        (ChannelDataType.STRING_ARRAY, proto.NOMINAL_DATA_TYPE_STRING_ARRAY),
        (ChannelDataType.STRUCT, proto.NOMINAL_DATA_TYPE_STRUCT),
        (ChannelDataType.VIDEO, proto.NOMINAL_DATA_TYPE_VIDEO),
        (ChannelDataType.UNKNOWN, proto.NOMINAL_DATA_TYPE_UNSPECIFIED),
    ],
)
def test_nominal_locator_conversion(datasource, clients, data_type, expected):
    """Channel types retain their storage type and locator channel through protobuf conversion."""
    datasource.batch_add_channels([CreateChannelRequest("velocity", data_type, "speed", "m/s")])
    request = clients.series_metadata.BatchCreateSeriesMetadata.call_args.args[0].requests[0]
    assert request == proto.CreateSeriesMetadataRequest(
        channel="velocity",
        data_source_rid=datasource.rid,
        locator_template=proto.LocatorTemplate(
            nominal=proto.NominalLocatorTemplate(
                channel="velocity",
                data_type=expected,
            )
        ),
        description="speed",
        unit="m/s",
        if_exists=proto.RETURN_EXISTING,
    )


@pytest.mark.parametrize(
    "method,policy",
    [
        ("batch_add_channels", proto.RETURN_EXISTING),
        ("batch_update_or_create_channels", proto.UPDATE_EXISTING),
    ],
)
def test_batches_preserve_policy_order_and_missing(datasource, clients, method, policy):
    """Writes keep 500-item batches and report requests absent from channel metadata hydration."""
    channels = [CreateChannelRequest(f"ch{i}", ChannelDataType.DOUBLE) for i in range(501)]
    clients.channel_metadata.BatchGetChannelMetadata.side_effect = [
        metadata_proto.BatchGetChannelMetadataResponse(
            channel_metadata=[
                metadata_proto.ChannelMetadata(
                    channel_identifier=metadata_proto.ChannelIdentifier(channel_name="ch0", data_source_rid=datasource.rid),
                    data_type=series_pb2.DOUBLE,
                    unit="m",
                    description="stored",
                )
            ]
        ),
        metadata_proto.BatchGetChannelMetadataResponse(channel_metadata=[]),
    ]
    result = getattr(datasource, method)(channels)
    batches = [call.args[0] for call in clients.series_metadata.BatchCreateSeriesMetadata.call_args_list]
    assert [len(batch.requests) for batch in batches] == [500, 1]
    assert [request.channel for batch in batches for request in batch.requests] == [c.name for c in channels]
    assert all(request.if_exists == policy for batch in batches for request in batch.requests)
    assert [(c.name, c.data_type, c.unit, c.description) for c in result.channels] == [
        ("ch0", ChannelDataType.DOUBLE, "m", "stored")
    ]
    assert result.missing == channels[1:]


@pytest.mark.parametrize("value", [None, ""])
def test_upsert_optional_presence(datasource, clients, value):
    """Omitted metadata stays absent while explicit empty strings remain present."""
    datasource.batch_update_or_create_channels([CreateChannelRequest("ch", ChannelDataType.DOUBLE, value, value)])
    request = clients.series_metadata.BatchCreateSeriesMetadata.call_args.args[0].requests[0]
    assert request.HasField("description") is (value is not None)
    assert request.HasField("unit") is (value is not None)
    assert not request.HasField("channel_kind")


@pytest.mark.parametrize("method", ["batch_add_channels", "batch_update_or_create_channels"])
def test_empty_batch_has_no_io(datasource, clients, method):
    """Empty input returns empty results without writes or hydration."""
    result = getattr(datasource, method)([])
    assert result.channels == result.missing == []
    assert clients.mock_calls == []


def test_add_channel_hydrates_existing_metadata(datasource, clients):
    """Single creates return existing channel metadata rather than the requested replacement values."""
    clients.channel_metadata.BatchGetChannelMetadata.return_value = (
        metadata_proto.BatchGetChannelMetadataResponse(
            channel_metadata=[
                metadata_proto.ChannelMetadata(
                    channel_identifier=metadata_proto.ChannelIdentifier(channel_name="ch", data_source_rid=datasource.rid),
                    description="existing",
                )
            ]
        )
    )
    channel = datasource.add_channel("ch", ChannelDataType.DOUBLE, description="new")
    request = clients.series_metadata.CreateSeriesMetadata.call_args.args[0]
    assert request.if_exists == proto.RETURN_EXISTING
    assert channel.description == "existing"
    assert channel.name == "ch"


@pytest.mark.parametrize(
    "method,rpc",
    [
        ("add_channel", "CreateSeriesMetadata"),
        ("batch_add_channels", "BatchCreateSeriesMetadata"),
        ("batch_update_or_create_channels", "BatchCreateSeriesMetadata"),
    ],
)
def test_write_errors_are_translated_before_hydration(datasource, clients, fake_rpc_error, method, rpc):
    """Write failures become Nominal errors without attempting the follow-up metadata lookup."""
    getattr(clients.series_metadata, rpc).side_effect = fake_rpc_error(grpc.StatusCode.PERMISSION_DENIED)
    args = (
        ("ch", ChannelDataType.DOUBLE)
        if method == "add_channel"
        else ([CreateChannelRequest("ch", ChannelDataType.DOUBLE)],)
    )
    with pytest.raises(NominalPermissionDeniedError):
        getattr(datasource, method)(*args)
    clients.channel_metadata.BatchGetChannelMetadata.assert_not_called()


def test_repeated_create_names_reach_service(datasource, clients):
    """Repeated create names retain RETURN_EXISTING so the backend can accept the batch."""
    channels = [CreateChannelRequest("ch", ChannelDataType.DOUBLE)] * 2
    datasource.batch_add_channels(channels)
    requests = clients.series_metadata.BatchCreateSeriesMetadata.call_args.args[0].requests
    assert [request.channel for request in requests] == ["ch", "ch"]
    assert all(request.if_exists == proto.RETURN_EXISTING for request in requests)


def test_later_batch_failure_stops_writes_and_hydration(datasource, clients, fake_rpc_error):
    """A failure after a successful batch stops later writes and does not report partial hydrated results."""
    clients.series_metadata.BatchCreateSeriesMetadata.side_effect = [
        proto.BatchCreateSeriesMetadataResponse(),
        fake_rpc_error(grpc.StatusCode.PERMISSION_DENIED),
    ]
    channels = [CreateChannelRequest(f"ch{i}", ChannelDataType.DOUBLE) for i in range(1001)]
    with pytest.raises(NominalPermissionDeniedError):
        datasource.batch_add_channels(channels)
    assert clients.series_metadata.BatchCreateSeriesMetadata.call_count == 2
    clients.channel_metadata.BatchGetChannelMetadata.assert_not_called()
