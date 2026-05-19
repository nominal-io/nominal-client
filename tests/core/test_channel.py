from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from conjure_python_client import ConjureDecoder
from nominal_api import api, scout_compute_api

from nominal.core.channel import Channel, ChannelDataType, LastValue
from nominal.ts import _SecondsNanos


def _make_response(value: scout_compute_api.Value, ts: datetime) -> scout_compute_api.ComputeNodeResponse:
    api_ts = api.Timestamp(seconds=int(ts.timestamp()), nanos=ts.microsecond * 1000)
    return scout_compute_api.ComputeNodeResponse(
        single_point=scout_compute_api.SinglePoint(precision_loss=False, timestamp=api_ts, value=value),
    )


def _ns(ts: datetime) -> int:
    return _SecondsNanos.from_datetime(ts).to_nanoseconds()


@pytest.fixture
def mock_clients():
    clients = MagicMock()
    clients.compute = MagicMock()
    return clients


@pytest.fixture
def mock_channel(mock_clients):
    return Channel(
        name="ch1",
        data_source="ds-rid",
        data_type=ChannelDataType.DOUBLE,
        unit=None,
        description=None,
        _clients=mock_clients,
    )


@pytest.mark.parametrize(
    ("data_type", "value", "expected"),
    [
        # int64 is wire-encoded as a string to preserve precision; pick a value larger than the
        # JS-safe-integer / float64-mantissa range so a regression that routes through float fails.
        (ChannelDataType.DOUBLE, scout_compute_api.Value(float64_value=42.5), 42.5),
        (ChannelDataType.INT, scout_compute_api.Value(int64_value="9007199254740993"), 9007199254740993),
        (ChannelDataType.STRING, scout_compute_api.Value(string_value="hello"), "hello"),
    ],
    ids=["double", "int", "string"],
)
def test_get_last_value_returns_value(
    mock_channel: Channel,
    mock_clients: MagicMock,
    data_type: ChannelDataType,
    value: scout_compute_api.Value,
    expected: float | int | str,
):
    """A single-point response is returned as a LastValue with the correctly-typed value."""
    mock_channel.data_type = data_type
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    mock_clients.compute.compute.return_value = _make_response(value, ts)

    result = mock_channel.get_last_value()

    assert result == LastValue(_ns(ts), expected)
    assert type(result.value) is type(expected)
    assert isinstance(result.timestamp, int)


def test_get_last_value_empty_window_returns_none(mock_channel: Channel, mock_clients: MagicMock):
    """End-to-end: an empty single-point response goes through the real conjure decoder and is treated as no data."""

    def _decode_empty_singlepoint(*_args, **_kwargs):
        ConjureDecoder().decode(
            {"type": "singlePoint", "singlePoint": None},
            scout_compute_api.ComputeNodeResponse,
        )

    mock_clients.compute.compute.side_effect = _decode_empty_singlepoint

    result = mock_channel.get_last_value()

    assert result is None


def test_get_last_value_unrelated_value_error_propagates(mock_channel: Channel, mock_clients: MagicMock):
    """ValueErrors with a different message are not swallowed by the empty-window guard."""
    mock_clients.compute.compute.side_effect = ValueError("some other validation error")

    with pytest.raises(ValueError, match="some other validation error"):
        mock_channel.get_last_value()


def test_get_last_value_request_fields(mock_channel: Channel, mock_clients: MagicMock):
    """Request carries the explicit start, the explicit end, last_value_point node, and channel identity.

    Uses non-zero microsecond components on `start` and `end` to also exercise sub-second round-trip
    from `datetime` -> `_SecondsNanos` -> `api.Timestamp.nanos` on the request side.
    """
    start = datetime(2026, 1, 2, 2, 54, 5, 123456, tzinfo=timezone.utc)
    end = datetime(2026, 1, 2, 3, 4, 5, 789012, tzinfo=timezone.utc)
    mock_clients.compute.compute.return_value = _make_response(scout_compute_api.Value(float64_value=1.0), end)

    mock_channel.get_last_value(start=start, end=end, tags={"a": "b"})

    _, request = mock_clients.compute.compute.call_args[0]
    assert request.start.seconds == int(start.timestamp())
    assert request.start.nanos == start.microsecond * 1000
    assert request.end.seconds == int(end.timestamp())
    assert request.end.nanos == end.microsecond * 1000
    # The compute node must be a SelectValue with last_value_point set.
    series = request.node.value.last_value_point
    assert series is not None
    # For a DOUBLE channel, the series is a NumericSeries; tags ride on its ChannelSeries.data_source.
    data_source = series.numeric.channel.data_source
    assert data_source.channel.literal == "ch1"
    assert data_source.data_source_rid.literal == "ds-rid"
    assert {k: v.literal for k, v in data_source.tags.items()} == {"a": "b"}


def test_get_last_value_end_only_defaults_start_to_end_minus_one_hour(
    mock_channel: Channel, mock_clients: MagicMock
):
    """Omitting `start` falls back to `end - 1hr` (anchored to the provided end, not wall-clock now)."""
    end = datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    mock_clients.compute.compute.return_value = _make_response(scout_compute_api.Value(float64_value=1.0), end)

    mock_channel.get_last_value(end=end)

    _, request = mock_clients.compute.compute.call_args[0]
    expected_start = _SecondsNanos.from_flexible(end - timedelta(hours=1)).to_api()
    assert request.start.seconds == expected_start.seconds
    assert request.start.nanos == expected_start.nanos
    assert request.end.seconds == int(end.timestamp())


def test_get_last_value_start_after_end_raises(mock_channel: Channel, mock_clients: MagicMock):
    """`start > end` is rejected before any API call."""
    start = datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="start .* must be at or before end"):
        mock_channel.get_last_value(start=start, end=end)

    mock_clients.compute.compute.assert_not_called()


def test_get_last_value_zero_width_window_allowed(mock_channel: Channel, mock_clients: MagicMock):
    """`start == end` is a legal "value at exactly T" query; the server decides whether a point exists."""
    instant = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    mock_clients.compute.compute.return_value = _make_response(scout_compute_api.Value(float64_value=1.0), instant)

    mock_channel.get_last_value(start=instant, end=instant)

    _, request = mock_clients.compute.compute.call_args[0]
    assert request.start.seconds == int(instant.timestamp())
    assert request.start.nanos == 0
    assert request.end.seconds == int(instant.timestamp())
    assert request.end.nanos == 0


def test_get_last_value_default_start_clamps_at_epoch(mock_channel: Channel, mock_clients: MagicMock):
    """When `end` is within the first hour of the unix epoch, the default-start clamp avoids negative seconds."""
    end = datetime(1970, 1, 1, 0, 30, 0, tzinfo=timezone.utc)  # 30 minutes after epoch
    mock_clients.compute.compute.return_value = _make_response(scout_compute_api.Value(float64_value=1.0), end)

    mock_channel.get_last_value(end=end)

    _, request = mock_clients.compute.compute.call_args[0]
    assert request.start.seconds == 0
    assert request.start.nanos == 0
    assert request.end.seconds == int(end.timestamp())


def test_get_last_value_unsupported_data_type_raises(mock_channel: Channel, mock_clients: MagicMock):
    """Non-numeric, non-string channels are rejected up front without an API call."""
    mock_channel.data_type = ChannelDataType.LOG

    with pytest.raises(TypeError, match="get_last_value only supports numeric"):
        mock_channel.get_last_value()

    mock_clients.compute.compute.assert_not_called()


def test_get_last_value_missing_single_point_raises(mock_channel: Channel, mock_clients: MagicMock):
    """A response without `single_point` set should not be silently accepted."""
    mock_clients.compute.compute.return_value = scout_compute_api.ComputeNodeResponse(
        numeric=scout_compute_api.NumericPlot(timestamps=[], values=[]),
    )

    with pytest.raises(RuntimeError, match="Expected response type to be `single_point`"):
        mock_channel.get_last_value()


def test_get_last_value_unhandled_value_variant_raises(mock_channel: Channel, mock_clients: MagicMock):
    """A Value carrying an unhandled variant (array/struct) raises rather than returning junk."""
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    api_ts = api.Timestamp(seconds=int(ts.timestamp()), nanos=0)
    mock_clients.compute.compute.return_value = scout_compute_api.ComputeNodeResponse(
        single_point=scout_compute_api.SinglePoint(
            precision_loss=False,
            timestamp=api_ts,
            value=scout_compute_api.Value(array_value=[]),
        ),
    )

    with pytest.raises(RuntimeError, match="Unexpected value variant in `single_point` response"):
        mock_channel.get_last_value()


def test_conjure_empty_singlepoint_error_message_unchanged():
    """Pin on the conjure decode-error string that `Channel.get_last_value` matches.

    If this test fails after a `nominal-api` or `conjure-python-client` bump, it likely
    means the upstream fix from palantir/conjure-python#1050 has propagated, meaning
    `ComputeNodeResponse.single_point` is now `None` on empty windows instead of raising.
    In that case, remove the `try/except ValueError` workaround in `Channel.get_last_value`
    along with this test.
    """
    payload = {"type": "singlePoint", "singlePoint": None}
    with pytest.raises(ValueError, match="^a union value must not be None$"):
        ConjureDecoder().decode(payload, scout_compute_api.ComputeNodeResponse)
