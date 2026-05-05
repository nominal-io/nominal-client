from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from conjure_python_client import ConjureDecoder
from nominal_api import api, scout_compute_api

from nominal.core.channel import Channel, ChannelDataType, LatestValue
from nominal.ts import _MAX_TIMESTAMP


def _make_response(value: scout_compute_api.Value, ts: datetime) -> scout_compute_api.ComputeNodeResponse:
    api_ts = api.Timestamp(seconds=int(ts.timestamp()), nanos=ts.microsecond * 1000)
    return scout_compute_api.ComputeNodeResponse(
        single_point=scout_compute_api.SinglePoint(precision_loss=False, timestamp=api_ts, value=value),
    )


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


def test_get_latest_value_returns_float64(mock_channel: Channel, mock_clients: MagicMock):
    """A float64 single-point response is returned as a LatestValue with a float."""
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    mock_clients.compute.compute.return_value = _make_response(scout_compute_api.Value(float64_value=42.5), ts)

    result = mock_channel.get_latest_value(lookback=timedelta(minutes=5))

    assert result == LatestValue(ts, 42.5)
    assert isinstance(result.value, float)
    assert result.timestamp.tzinfo is timezone.utc


def test_get_latest_value_returns_int64_parsed_from_string(mock_channel: Channel, mock_clients: MagicMock):
    """int64 is wire-encoded as a string; get_latest_value parses it back to int."""
    mock_channel.data_type = ChannelDataType.INT
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    mock_clients.compute.compute.return_value = _make_response(
        # int64_value type is Optional[str]
        scout_compute_api.Value(int64_value="9007199254740993"),
        ts,
    )

    result = mock_channel.get_latest_value(lookback=timedelta(minutes=5))

    assert result == LatestValue(ts, 9007199254740993)
    assert isinstance(result.value, int)


def test_get_latest_value_returns_string(mock_channel: Channel, mock_clients: MagicMock):
    """A string single-point response is returned as a LatestValue with a str."""
    mock_channel.data_type = ChannelDataType.STRING
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    mock_clients.compute.compute.return_value = _make_response(scout_compute_api.Value(string_value="hello"), ts)

    result = mock_channel.get_latest_value(lookback=timedelta(minutes=5))

    assert result == LatestValue(ts, "hello")


def test_get_latest_value_empty_window_returns_none(mock_channel: Channel, mock_clients: MagicMock):
    """An empty window surfaces as the conjure 'a union value must not be None' ValueError; treated as no data."""
    mock_clients.compute.compute.side_effect = ValueError("a union value must not be None")

    result = mock_channel.get_latest_value(lookback=timedelta(minutes=5))

    assert result is None


def test_get_latest_value_unrelated_value_error_propagates(mock_channel: Channel, mock_clients: MagicMock):
    """ValueErrors with a different message are not swallowed by the empty-window guard."""
    mock_clients.compute.compute.side_effect = ValueError("some other validation error")

    with pytest.raises(ValueError, match="some other validation error"):
        mock_channel.get_latest_value(lookback=timedelta(minutes=5))


def test_get_latest_value_request_fields(mock_channel: Channel, mock_clients: MagicMock):
    """Request carries start = end - lookback, the explicit end, last_value_point node, and channel identity."""
    end = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    lookback = timedelta(minutes=10)
    mock_clients.compute.compute.return_value = _make_response(scout_compute_api.Value(float64_value=1.0), end)

    mock_channel.get_latest_value(lookback=lookback, end=end, tags={"a": "b"})

    _, request = mock_clients.compute.compute.call_args[0]
    expected_start = end - lookback
    assert request.start.seconds == int(expected_start.timestamp())
    assert request.end.seconds == int(end.timestamp())
    # The compute node must be a SelectValue with last_value_point set.
    series = request.node.value.last_value_point
    assert series is not None
    # For a DOUBLE channel, the series is a NumericSeries; tags ride on its ChannelSeries.data_source.
    data_source = series.numeric.channel.data_source
    assert data_source.channel.literal == "ch1"
    assert data_source.data_source_rid.literal == "ds-rid"
    assert {k: v.literal for k, v in data_source.tags.items()} == {"a": "b"}


def test_get_latest_value_explicit_end_overrides_default_now(
    mock_channel: Channel, mock_clients: MagicMock, monkeypatch: pytest.MonkeyPatch
):
    """In lookback mode, an explicit `end` is honored instead of `datetime.now(UTC)`."""
    fixed_now = datetime(2030, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    explicit_end = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now if tz is timezone.utc else datetime.now(tz)

    monkeypatch.setattr("nominal.core.channel.datetime", _FixedDatetime)
    mock_clients.compute.compute.return_value = _make_response(
        scout_compute_api.Value(float64_value=1.0), explicit_end
    )

    mock_channel.get_latest_value(lookback=timedelta(minutes=1), end=explicit_end)

    _, request = mock_clients.compute.compute.call_args[0]
    assert request.end.seconds == int(explicit_end.timestamp())
    assert request.start.seconds == int((explicit_end - timedelta(minutes=1)).timestamp())
    assert request.end.seconds != int(fixed_now.timestamp())


def test_get_latest_value_default_end_is_now_utc(
    mock_channel: Channel, mock_clients: MagicMock, monkeypatch: pytest.MonkeyPatch
):
    """When end is omitted, the upper bound defaults to datetime.now(UTC)."""
    fixed_now = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now if tz is timezone.utc else datetime.now(tz)

    monkeypatch.setattr("nominal.core.channel.datetime", _FixedDatetime)
    mock_clients.compute.compute.return_value = _make_response(scout_compute_api.Value(float64_value=1.0), fixed_now)

    mock_channel.get_latest_value(lookback=timedelta(seconds=30))

    _, request = mock_clients.compute.compute.call_args[0]
    assert request.end.seconds == int(fixed_now.timestamp())
    assert request.start.seconds == int((fixed_now - timedelta(seconds=30)).timestamp())


@pytest.mark.parametrize("bad_lookback", [timedelta(0), timedelta(seconds=-1)])
def test_get_latest_value_non_positive_lookback_raises(
    mock_channel: Channel, mock_clients: MagicMock, bad_lookback: timedelta
):
    """A non-positive lookback is rejected up front without an API call."""
    with pytest.raises(ValueError, match="lookback must be strictly positive"):
        mock_channel.get_latest_value(lookback=bad_lookback)

    mock_clients.compute.compute.assert_not_called()


def test_get_latest_value_start_end_mode_request_fields(mock_channel: Channel, mock_clients: MagicMock):
    """In start/end mode, the request carries the explicit start and end."""
    start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    mock_clients.compute.compute.return_value = _make_response(scout_compute_api.Value(float64_value=1.0), end)

    mock_channel.get_latest_value(start=start, end=end)

    _, request = mock_clients.compute.compute.call_args[0]
    assert request.start.seconds == int(start.timestamp())
    assert request.end.seconds == int(end.timestamp())


def test_get_latest_value_start_only_defaults_end_to_max(mock_channel: Channel, mock_clients: MagicMock):
    """In start/end mode, omitting `end` falls back to `_MAX_TIMESTAMP` for parity with `search_logs`."""
    start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    mock_clients.compute.compute.return_value = _make_response(scout_compute_api.Value(float64_value=1.0), start)

    mock_channel.get_latest_value(start=start)

    _, request = mock_clients.compute.compute.call_args[0]
    expected_end = _MAX_TIMESTAMP.to_api()
    assert request.start.seconds == int(start.timestamp())
    assert request.end.seconds == expected_end.seconds
    assert request.end.nanos == expected_end.nanos


def test_get_latest_value_requires_exactly_one_of_start_or_lookback(
    mock_channel: Channel, mock_clients: MagicMock
):
    """Passing neither `start` nor `lookback`, or both, is rejected up front."""
    with pytest.raises(ValueError, match="exactly one of `start` or `lookback`"):
        mock_channel.get_latest_value()

    with pytest.raises(ValueError, match="exactly one of `start` or `lookback`"):
        mock_channel.get_latest_value(
            start=datetime(2026, 1, 1, tzinfo=timezone.utc), lookback=timedelta(minutes=5)
        )

    mock_clients.compute.compute.assert_not_called()


@pytest.mark.parametrize(
    "start,end",
    [
        (datetime(2026, 1, 2, tzinfo=timezone.utc), datetime(2026, 1, 1, tzinfo=timezone.utc)),
        (datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 1, 1, tzinfo=timezone.utc)),
    ],
)
def test_get_latest_value_end_not_after_start_raises(
    mock_channel: Channel, mock_clients: MagicMock, start: datetime, end: datetime
):
    """`end <= start` is rejected up front without an API call."""
    with pytest.raises(ValueError, match="`end` must be strictly after `start`"):
        mock_channel.get_latest_value(start=start, end=end)

    mock_clients.compute.compute.assert_not_called()


def test_get_latest_value_unsupported_data_type_raises(mock_channel: Channel, mock_clients: MagicMock):
    """Non-numeric, non-string channels are rejected up front without an API call."""
    mock_channel.data_type = ChannelDataType.LOG

    with pytest.raises(TypeError, match="get_latest_value only supports numeric"):
        mock_channel.get_latest_value(lookback=timedelta(minutes=5))

    mock_clients.compute.compute.assert_not_called()


def test_get_latest_value_missing_single_point_raises(mock_channel: Channel, mock_clients: MagicMock):
    """A response without `single_point` set should not be silently accepted."""
    mock_clients.compute.compute.return_value = scout_compute_api.ComputeNodeResponse(
        numeric=scout_compute_api.NumericPlot(timestamps=[], values=[]),
    )

    with pytest.raises(RuntimeError, match="Expected response type to be `single_point`"):
        mock_channel.get_latest_value(lookback=timedelta(minutes=5))


def test_get_latest_value_unhandled_value_variant_raises(mock_channel: Channel, mock_clients: MagicMock):
    """A Value carrying an unhandled variant (array/struct) raises rather than returning junk."""
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    api_ts = api.Timestamp(seconds=int(ts.timestamp()), nanos=0)
    mock_clients.compute.compute.return_value = scout_compute_api.ComputeNodeResponse(
        single_point=scout_compute_api.SinglePoint(
            precision_loss=False, timestamp=api_ts, value=scout_compute_api.Value(array_value=[]),
        ),
    )

    with pytest.raises(RuntimeError, match="Unexpected value variant in `single_point` response"):
        mock_channel.get_latest_value(lookback=timedelta(minutes=5))


def test_conjure_empty_singlepoint_error_message_unchanged():
    """Pins the conjure decode-error string that `get_latest_value` matches for empty windows.

    Fails at CI if a `nominal-api` or `conjure-python-client` bump rewords the message.
    """
    payload = {"type": "singlePoint", "singlePoint": None}
    with pytest.raises(ValueError, match="^a union value must not be None$"):
        ConjureDecoder().decode(payload, scout_compute_api.ComputeNodeResponse)
