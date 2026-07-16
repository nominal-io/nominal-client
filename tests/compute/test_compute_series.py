from __future__ import annotations

import io
from unittest.mock import MagicMock

import pytest
from nominal_api import scout_compute_api

from nominal.core.channel import Channel, ChannelDataType
from nominal.experimental.compute import compute_series
from nominal.experimental.compute._series import _to_conjure_series


@pytest.fixture
def client(mock_clients: MagicMock) -> MagicMock:
    """A mock NominalClient whose ``_clients`` is the shared mock_clients fixture."""
    client = MagicMock()
    client._clients = mock_clients
    return client


def _channel(mock_clients: MagicMock, name: str, data_source: str = "ds-1") -> Channel:
    return Channel(
        name=name,
        data_source=data_source,
        data_type=ChannelDataType.DOUBLE,
        unit=None,
        description=None,
        _clients=mock_clients,
    )


def _csv_response(text: str) -> io.BytesIO:
    return io.BytesIO(text.encode())


# --- bridge: nominal_compute series -> scout_compute_api.Series ---


def test_bridge_numeric_series() -> None:
    """A nominal_compute NumericSeries bridges into the numeric arm of the Series union."""
    nc = pytest.importorskip("nominal_compute")
    series = _to_conjure_series(nc.NumericSeries.Reference("a") - nc.NumericSeries.Reference("m"))
    assert isinstance(series, scout_compute_api.Series)
    assert series.numeric is not None
    assert series.enum is None
    assert series.numeric.type == "subtract"


def test_bridge_categorical_series() -> None:
    """A nominal_compute CategoricalSeries bridges into the enum arm of the Series union."""
    nc = pytest.importorskip("nominal_compute")
    series = _to_conjure_series(nc.CategoricalSeries.Reference("c"))
    assert isinstance(series, scout_compute_api.Series)
    assert series.enum is not None
    assert series.numeric is None


# --- compute_series ---


def test_compute_series_builds_request_and_parses(client: MagicMock, mock_clients: MagicMock) -> None:
    """compute_series binds references to channels, sends the bridged node, and parses the CSV into a Series."""
    nc = pytest.importorskip("nominal_compute")
    expr = nc.NumericSeries.Reference("a") - nc.NumericSeries.Reference("m")
    inputs = {"a": _channel(mock_clients, "chan_a"), "m": _channel(mock_clients, "chan_m", data_source="ds-2")}
    mock_clients.dataexport.export_channel_data.return_value = _csv_response(
        "timestamp,rmse\n2026-01-01T00:00:00Z,1.5\n2026-01-01T00:00:01Z,2.5\n"
    )

    result = compute_series(client, expr, inputs, name="rmse", enable_gzip=False)

    # parsed data
    assert list(result) == [1.5, 2.5]
    assert result.name == "rmse"
    assert result.index.name == "timestamp"

    # request wiring
    auth, request = mock_clients.dataexport.export_channel_data.call_args[0]
    assert auth == "Bearer test-token"
    td = request.channels.time_domain
    assert [c.column_name for c in td.channels] == ["rmse"]
    node = td.channels[0].compute_node
    assert isinstance(node, scout_compute_api.Series)
    assert node.numeric is not None and node.numeric.type == "subtract"

    # references bound to the right channels
    variables = request.context.variables
    assert set(variables) == {"a", "m"}
    assert variables["a"].channel.data_source.channel.literal == "chan_a"
    assert variables["a"].channel.data_source.data_source_rid.literal == "ds-1"
    assert variables["m"].channel.data_source.channel.literal == "chan_m"
    assert variables["m"].channel.data_source.data_source_rid.literal == "ds-2"


def test_compute_series_binds_tags(client: MagicMock, mock_clients: MagicMock) -> None:
    """Tags narrow a reference to a tagged series; references omitted from tags bind with no tag filter."""
    nc = pytest.importorskip("nominal_compute")
    expr = nc.NumericSeries.Reference("a") - nc.NumericSeries.Reference("m")
    inputs = {"a": _channel(mock_clients, "chan_a"), "m": _channel(mock_clients, "chan_m")}
    mock_clients.dataexport.export_channel_data.return_value = _csv_response(
        "timestamp,value\n2026-01-01T00:00:00Z,1.0\n"
    )

    compute_series(client, expr, inputs, tags={"a": {"vehicle": "1", "run": "7"}}, enable_gzip=False)

    variables = mock_clients.dataexport.export_channel_data.call_args[0][1].context.variables
    tagged = variables["a"].channel.data_source.tags
    assert {k: v.literal for k, v in tagged.items()} == {"vehicle": "1", "run": "7"}
    assert variables["m"].channel.data_source.tags == {}


def test_compute_series_defaults_full_range(client: MagicMock, mock_clients: MagicMock) -> None:
    """With no start/end, compute_series requests the full supported time range."""
    nc = pytest.importorskip("nominal_compute")
    mock_clients.dataexport.export_channel_data.return_value = _csv_response(
        "timestamp,value\n2026-01-01T00:00:00Z,1.0\n"
    )

    compute_series(client, nc.NumericSeries.Reference("a"), {"a": _channel(mock_clients, "chan_a")}, enable_gzip=False)

    _, request = mock_clients.dataexport.export_channel_data.call_args[0]
    assert request.start_time.seconds == 0
    assert request.end_time.seconds == 9223372036
