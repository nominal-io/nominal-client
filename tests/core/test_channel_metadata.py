from unittest.mock import MagicMock

import grpc
import pytest
from nominal_api import datasource_api

from nominal.core.channel import Channel, ChannelDataType
from nominal.core.datasource import DataSource
from nominal.core.unit import Unit
from nominal.exceptions import NominalNotFoundError
from nominal.protos.channelmetadata.v1 import channel_metadata_pb2 as pb
from nominal.protos.types.series import series_pb2


def _metadata(name: str = "temperature", **kwargs) -> pb.ChannelMetadata:
    return pb.ChannelMetadata(
        channel_identifier=pb.ChannelIdentifier(channel_name=name, data_source_rid="ri.datasource.test"), **kwargs
    )


@pytest.mark.parametrize("unit", [None, "", "m/s", Unit(name="speed", symbol="m/s")])
def test_update_unit_presence_and_identity(unit) -> None:
    """Supplied units select the mask, including clearing, and refresh the same object."""
    clients = MagicMock()
    channel = Channel._from_proto(clients, _metadata(unit="old", description="before"))
    clients.channel_metadata.UpdateChannelMetadata.return_value = pb.UpdateChannelMetadataResponse(
        channel_metadata=_metadata(description="after")
    )
    assert channel.update(unit=unit) is channel
    request = pb.UpdateChannelMetadataRequest.FromString(
        clients.channel_metadata.UpdateChannelMetadata.call_args.args[0].SerializeToString()
    )
    assert list(request.update_mask.paths) == ["unit"]
    assert request.channel_metadata.unit == (unit.symbol if isinstance(unit, Unit) else unit or "")
    assert request.channel_metadata.HasField("unit") == (unit is not None)
    assert not request.channel_metadata.HasField("description")
    assert channel.unit is None
    assert channel.description == "after"


def test_description_empty_is_supplied_and_unit_omitted() -> None:
    """An empty description selects its field without replacing an omitted unit."""
    clients = MagicMock()
    channel = Channel._from_proto(clients, _metadata())
    clients.channel_metadata.UpdateChannelMetadata.return_value = pb.UpdateChannelMetadataResponse(
        channel_metadata=_metadata(unit="m")
    )
    channel.update(description="")
    request = pb.UpdateChannelMetadataRequest.FromString(
        clients.channel_metadata.UpdateChannelMetadata.call_args.args[0].SerializeToString()
    )
    assert list(request.update_mask.paths) == ["description"]
    assert request.channel_metadata.HasField("description")
    assert request.channel_metadata.description == ""
    assert not request.channel_metadata.HasField("unit")
    assert channel.unit == "m"


def test_noop_update_refreshes_in_place() -> None:
    """An omitted update fetches current state instead of sending an invalid empty mask."""
    clients = MagicMock()
    channel = Channel._from_proto(clients, _metadata())
    clients.channel_metadata.GetChannelMetadata.return_value = pb.GetChannelMetadataResponse(
        channel_metadata=_metadata(unit="m", description="latest")
    )
    assert channel.update(description=None) is channel
    assert channel.unit == "m"
    assert channel.description == "latest"
    clients.channel_metadata.UpdateChannelMetadata.assert_not_called()


@pytest.mark.parametrize(
    "data_type, expected",
    [
        (None, None),
        (0, ChannelDataType.UNKNOWN),
        (series_pb2.DOUBLE, ChannelDataType.DOUBLE),
        (series_pb2.SPATIAL, ChannelDataType.UNKNOWN),
        (999, ChannelDataType.UNKNOWN),
    ],
)
def test_metadata_presence_and_query_type(data_type, expected) -> None:
    """Absent values stay absent and query types remain distinct from stored integer types."""
    channel = Channel._from_proto(MagicMock(), _metadata(data_type=data_type, iceberg_data_type=series_pb2.INT))
    assert channel.data_type is expected
    assert channel.unit is None
    assert channel.description is None


def test_batch_get_uses_returned_identifiers_and_batches() -> None:
    """Omitted channels never shift metadata onto other names, across bounded reads."""
    clients = MagicMock()
    clients.channel_metadata.BatchGetChannelMetadata.side_effect = [
        pb.BatchGetChannelMetadataResponse(channel_metadata=[_metadata("ch499")]),
        pb.BatchGetChannelMetadataResponse(channel_metadata=[_metadata("ch500")]),
    ]
    datasource = DataSource(rid="ri.datasource.test", _clients=clients)
    channels = list(datasource.get_channels(names=(f"ch{i}" for i in range(501))))
    assert [channel.name for channel in channels] == ["ch499", "ch500"]
    requests = [call.args[0] for call in clients.channel_metadata.BatchGetChannelMetadata.call_args_list]
    assert [len(request.channel_identifiers) for request in requests] == [500, 1]
    assert requests[1].channel_identifiers[0].channel_name == "ch500"


def test_missing_channel_preserves_value_error() -> None:
    """Single-channel lookup retains its existing missing-channel exception."""
    clients = MagicMock()
    clients.channel_metadata.BatchGetChannelMetadata.return_value = pb.BatchGetChannelMetadataResponse()
    with pytest.raises(ValueError, match="not found"):
        DataSource(rid="ri.datasource.test", _clients=clients).get_channel("missing")


@pytest.mark.parametrize("operation", ["refresh", "update", "batch_get"])
def test_grpc_errors_translate(operation, fake_rpc_error) -> None:
    """Channel metadata RPC failures become the SDK's public error type."""
    clients = MagicMock()
    error = fake_rpc_error(grpc.StatusCode.NOT_FOUND)
    clients.channel_metadata.GetChannelMetadata.side_effect = error
    clients.channel_metadata.UpdateChannelMetadata.side_effect = error
    clients.channel_metadata.BatchGetChannelMetadata.side_effect = error
    channel = Channel._from_proto(clients, _metadata())
    with pytest.raises(NominalNotFoundError):
        if operation == "refresh":
            channel.refresh()
        elif operation == "update":
            channel.update(unit=None)
        else:
            list(DataSource(rid=channel.data_source, _clients=clients).get_channels(names=[channel.name]))


def test_small_unit_update_preserves_clearing_in_one_batch() -> None:
    """A small unit update stays in one atomic batch, with None clearing only the selected unit."""
    clients = MagicMock()
    clients.datasource.search_channels.return_value.results = []
    clients.datasource.search_channels.return_value.next_page_token = None
    clients.channel_metadata.BatchGetChannelMetadata.return_value = pb.BatchGetChannelMetadataResponse(
        channel_metadata=[_metadata("speed"), _metadata("temperature")]
    )
    # get_channels discovers names through the still-Conjure search endpoint.
    clients.datasource.search_channels.return_value.results = [
        datasource_api.ChannelMetadata(name="speed", data_source="ri.datasource.test"),
        datasource_api.ChannelMetadata(name="temperature", data_source="ri.datasource.test"),
    ]
    datasource = DataSource(rid="ri.datasource.test", _clients=clients)
    datasource.set_channel_units(
        {"speed": Unit(name="speed", symbol="m/s"), "temperature": None}, allow_display_only_units=True
    )
    clients.channel_metadata.BatchUpdateChannelMetadata.assert_called_once()
    requests = clients.channel_metadata.BatchUpdateChannelMetadata.call_args.args[0].requests
    assert [request.channel_metadata.channel_identifier.channel_name for request in requests] == [
        "speed",
        "temperature",
    ]
    assert [list(request.update_mask.paths) for request in requests] == [["unit"], ["unit"]]
    assert requests[0].channel_metadata.unit == "m/s"
    assert not requests[1].channel_metadata.HasField("unit")
    assert all(not request.channel_metadata.HasField("description") for request in requests)


def _clients_with_channels(count: int) -> MagicMock:
    clients = MagicMock()
    clients.datasource.search_channels.return_value.results = [
        datasource_api.ChannelMetadata(name=f"ch{i}", data_source="ri.datasource.test") for i in range(count)
    ]
    clients.datasource.search_channels.return_value.next_page_token = None
    clients.channel_metadata.BatchGetChannelMetadata.side_effect = lambda request: pb.BatchGetChannelMetadataResponse(
        channel_metadata=[
            pb.ChannelMetadata(channel_identifier=identifier) for identifier in request.channel_identifiers
        ]
    )
    return clients


@pytest.mark.parametrize(
    "count, batch_sizes", [(0, []), (10_000, [10_000]), (10_001, [10_000, 1]), (25_000, [10_000, 10_000, 5_000])]
)
def test_set_channel_units_batches_all_updates(count, batch_sizes) -> None:
    """Every channel is updated once in bounded requests, preserving unit clearing across the wire."""
    clients = _clients_with_channels(count)
    units = {f"ch{i}": None if i % 2 else "m/s" for i in range(count)}
    DataSource(rid="ri.datasource.test", _clients=clients).set_channel_units(units, allow_display_only_units=True)
    requests = [
        pb.BatchUpdateChannelMetadataRequest.FromString(call.args[0].SerializeToString())
        for call in clients.channel_metadata.BatchUpdateChannelMetadata.call_args_list
    ]
    assert [len(request.requests) for request in requests] == batch_sizes
    updates = [update for request in requests for update in request.requests]
    assert [update.channel_metadata.channel_identifier.channel_name for update in updates] == list(units)
    for update, unit in zip(updates, units.values(), strict=True):
        assert list(update.update_mask.paths) == ["unit"]
        assert update.channel_metadata.HasField("unit") == (unit is not None)
        assert update.channel_metadata.unit == (unit or "")
        assert not update.channel_metadata.HasField("description")


def test_set_channel_units_stops_after_failed_batch(fake_rpc_error) -> None:
    """A later RPC failure is translated and stops subsequent batches after earlier writes succeeded."""
    clients = _clients_with_channels(25_000)
    clients.channel_metadata.BatchUpdateChannelMetadata.side_effect = [
        pb.BatchUpdateChannelMetadataResponse(),
        fake_rpc_error(grpc.StatusCode.NOT_FOUND),
    ]
    with pytest.raises(NominalNotFoundError):
        DataSource(rid="ri.datasource.test", _clients=clients).set_channel_units(
            {f"ch{i}": "m/s" for i in range(25_000)}, allow_display_only_units=True
        )
    calls = clients.channel_metadata.BatchUpdateChannelMetadata.call_args_list
    assert len(calls) == 2
    assert [len(call.args[0].requests) for call in calls] == [10_000, 10_000]
    assert calls[1].args[0].requests[0].channel_metadata.channel_identifier.channel_name == "ch10000"


def test_set_channel_units_validates_all_symbols_before_first_write() -> None:
    """An invalid unit in a later batch is rejected before any metadata is changed."""
    clients = _clients_with_channels(10_001)
    clients.units.GetBatchUnits.return_value.responses = {}
    units: dict[str, str | None] = {f"ch{i}": None for i in range(10_001)}
    units["ch10000"] = "invalid-unit"
    with pytest.raises(ValueError, match="invalid-unit"):
        DataSource(rid="ri.datasource.test", _clients=clients).set_channel_units(units)
    clients.channel_metadata.BatchUpdateChannelMetadata.assert_not_called()
