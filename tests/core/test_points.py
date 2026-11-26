from __future__ import annotations

from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from threading import Event
from typing import Any
from unittest.mock import MagicMock

import grpc
import pytest
from conjure_python_client import ServiceConfiguration
from google.rpc.status_pb2 import Status
from nominal_api import scout_catalog

from nominal.core import Dataset, TimeAxes, TimeFrequencyAxes
from nominal.core._clientsbunch import ClientsBunch
from nominal.core._utils.grpc_tools import create_grpc_channel
from nominal.core._utils.networking import StaticHeaderProvider
from nominal.exceptions import NominalError, NominalPermissionDeniedError
from nominal.protos.channel_writer.v2 import nominal_channel_writer_pb2 as writer_pb2
from nominal.protos.channel_writer.v2 import nominal_channel_writer_pb2_grpc


@pytest.fixture
def dataset(mock_clients: MagicMock, make_enriched_dataset: Callable[..., scout_catalog.EnrichedDataset]) -> Dataset:
    """Use the ordinary resource wrapper with service I/O replaced at the test boundary."""
    return Dataset._from_conjure(mock_clients, make_enriched_dataset())


@pytest.fixture
def close_channels(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Close real shared channels constructed by the production client factory."""
    channels: list[grpc.Channel] = []

    def track_channel(**kwargs: Any) -> grpc.Channel:
        """Retain the real channel without changing its configuration."""
        channel = create_grpc_channel(**kwargs)
        channels.append(channel)
        return channel

    monkeypatch.setattr("nominal.core._clientsbunch.create_grpc_channel", track_channel)
    try:
        yield
    finally:
        for channel in channels:
            channel.close()


@contextmanager
def _writer_server(
    handler: Callable[
        [Iterator[writer_pb2.WriteStreamRequest], grpc.ServicerContext], Iterator[writer_pb2.WriteStreamResponse]
    ],
) -> Iterator[str]:
    """Serve the generated bidirectional RPC on a local, ephemeral port."""

    class Writer(nominal_channel_writer_pb2_grpc.NominalChannelWriterServiceServicer):
        """Expose only the operation under test."""

        def WriteStream(
            self, requests: Iterator[writer_pb2.WriteStreamRequest], context: grpc.ServicerContext
        ) -> Iterator[writer_pb2.WriteStreamResponse]:
            """Delegate server behavior to this test's finite protocol exchange."""
            return handler(requests, context)

    with ThreadPoolExecutor(max_workers=2) as executor:
        server = grpc.server(executor)
        nominal_channel_writer_pb2_grpc.add_NominalChannelWriterServiceServicer_to_server(Writer(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            yield f"http://127.0.0.1:{port}/api"
        finally:
            server.stop(0).wait()


def _transport_dataset(dataset: Dataset, base_url: str, timeout: float = 30) -> Dataset:
    """Use production auth, deadline and retry configuration with a synthetic credential."""
    clients = ClientsBunch.from_config(
        ServiceConfiguration(uris=[base_url], read_timeout=timeout, backoff_slot_size=1),
        base_url,
        "points-test",
        "synthetic-token",
        None,
        header_provider=StaticHeaderProvider({"X-Test-Context": "retained"}),
    )
    return replace(dataset, _clients=clients)


@pytest.mark.usefixtures("close_channels")
@pytest.mark.parametrize("unit", [None, ""])
def test_points_preserve_coordinates_metadata_and_call_context(dataset: Dataset, unit: str | None) -> None:
    """The real stream preserves coordinates and metadata, and half-closes after its one chunk."""
    requests_seen = []
    headers = {}
    deadlines = []

    def handler(
        requests: Iterator[writer_pb2.WriteStreamRequest], context: grpc.ServicerContext
    ) -> Iterator[writer_pb2.WriteStreamResponse]:
        """Acknowledge declaration, observe its chunk, then accept it after half-close."""
        headers.update(context.invocation_metadata())
        deadlines.append(context.time_remaining())
        declaration = next(requests)
        requests_seen.append(declaration)
        yield writer_pb2.WriteStreamResponse(accepted=writer_pb2.Accepted())
        yield writer_pb2.WriteStreamResponse(accepted=writer_pb2.Accepted(sequence_numbers=[1]))
        requests_seen.extend(requests)
        yield writer_pb2.WriteStreamResponse(accepted=writer_pb2.Accepted(sequence_numbers=[2]))

    with _writer_server(handler) as url:
        resource = _transport_dataset(dataset, url)
        resource.write_points(
            "Pressure",
            axes=TimeFrequencyAxes([1767268800000000123, -1, -1], [30, 10, 20]),
            values=[1.25, 0.5, 0.125],
            unit=unit,
            tags={"sensor": "x"},
        )
    assert len(requests_seen) == 2
    declaration, chunk = requests_seen
    assert declaration.sequence_number == 1
    (schema,) = declaration.declare_schemas.schemas
    assert schema.schema_id == chunk.chunk.channels[0].schema_id == 1
    assert schema.dataset_rid == dataset.rid and schema.channel == "Pressure"
    assert schema.type == writer_pb2.CHANNEL_TYPE_DOUBLE
    assert dict(schema.tags) == {"sensor": "x"}
    assert schema.HasField("unit") == (unit is not None)
    assert schema.unit == ""
    assert chunk.sequence_number == 2
    assert len(chunk.chunk.channels) == 1
    points = chunk.chunk.channels[0].points
    axes = points.axes.time_frequency
    assert [(ts.seconds, ts.nanos) for ts in axes.timestamps] == [(1767268800, 123), (-1, 999999999), (-1, 999999999)]
    assert list(axes.frequencies) == [30, 10, 20]
    assert list(points.values.doubles.values) == [1.25, 0.5, 0.125]
    assert headers["authorization"] == "Bearer synthetic-token"
    assert headers["x-test-context"] == "retained"
    assert "points-test" in headers["user-agent"]
    # The gRPC timeout header can round up; interceptor tests cover the exact configured deadline.
    assert 0 < deadlines[0] <= 31


@pytest.mark.usefixtures("close_channels")
@pytest.mark.parametrize("sequence_number", [1, 2])
def test_rejection_stops_the_operation_and_preserves_the_first_error(dataset: Dataset, sequence_number: int) -> None:
    """Declaration failure sends no chunk; either rejection surfaces its embedded status immediately."""
    seen = []
    stopped = Event()

    def handler(
        requests: Iterator[writer_pb2.WriteStreamRequest], context: grpc.ServicerContext
    ) -> Iterator[writer_pb2.WriteStreamResponse]:
        """Reject the selected phase and wait for client cancellation instead of ending the stream."""
        context.add_callback(stopped.set)
        seen.append(next(requests))
        if sequence_number == 2:
            yield writer_pb2.WriteStreamResponse(accepted=writer_pb2.Accepted(sequence_numbers=[1]))
            seen.append(next(requests))
        yield writer_pb2.WriteStreamResponse(
            rejected=writer_pb2.Rejected(
                sequence_number=sequence_number,
                status=Status(code=7, message="write denied"),
            )
        )
        try:
            seen.extend(requests)
        except grpc.RpcError:
            pass
        assert stopped.wait(5)
        context.abort(grpc.StatusCode.UNAVAILABLE, "later failure")

    with _writer_server(handler) as url:
        with pytest.raises(NominalPermissionDeniedError, match="write denied"):
            _transport_dataset(dataset, url).write_points("Pressure", axes=TimeAxes([0]), values=[1])
        assert stopped.wait(5)
    assert len(seen) == sequence_number


@pytest.mark.usefixtures("close_channels")
@pytest.mark.parametrize(
    "response",
    [
        writer_pb2.WriteStreamResponse(accepted=writer_pb2.Accepted(sequence_numbers=[9])),
        writer_pb2.WriteStreamResponse(accepted=writer_pb2.Accepted(sequence_numbers=[2])),
        writer_pb2.WriteStreamResponse(rejected=writer_pb2.Rejected(sequence_number=1, status=Status(code=0))),
        writer_pb2.WriteStreamResponse(rejected=writer_pb2.Rejected(sequence_number=1, status=Status(code=99))),
        writer_pb2.WriteStreamResponse(),
    ],
)
def test_invalid_protocol_responses_fail(dataset: Dataset, response: writer_pb2.WriteStreamResponse) -> None:
    """Uncorrelated acknowledgements and malformed rejection statuses cannot report success."""

    def handler(
        requests: Iterator[writer_pb2.WriteStreamRequest], context: grpc.ServicerContext
    ) -> Iterator[writer_pb2.WriteStreamResponse]:
        """Return one invalid response after receiving a declaration."""
        next(requests)
        yield response

    with _writer_server(handler) as url:
        with pytest.raises(NominalError, match="unexpected acknowledgement|invalid rejection status"):
            _transport_dataset(dataset, url).write_points("Pressure", axes=TimeAxes([0]), values=[1])


@pytest.mark.usefixtures("close_channels")
@pytest.mark.parametrize("accepted", [False, True])
def test_batch_acknowledgement_determines_success(dataset: Dataset, accepted: bool) -> None:
    """An acknowledged batch succeeds despite stream shutdown; clean EOF without acceptance fails."""

    def handler(
        requests: Iterator[writer_pb2.WriteStreamRequest], context: grpc.ServicerContext
    ) -> Iterator[writer_pb2.WriteStreamResponse]:
        """Receive the data, then omit acceptance or fail after it."""
        next(requests)
        yield writer_pb2.WriteStreamResponse(accepted=writer_pb2.Accepted(sequence_numbers=[1]))
        (chunk,) = list(requests)
        assert chunk.chunk.channels[0].points.axes.time.timestamps[0].nanos == 123
        if accepted:
            yield writer_pb2.WriteStreamResponse(accepted=writer_pb2.Accepted(sequence_numbers=[2]))
            context.abort(grpc.StatusCode.UNAVAILABLE, "The server is shutting down; open a new stream")

    with _writer_server(handler) as url:
        resource = _transport_dataset(dataset, url)
        if accepted:
            resource.write_points("Pressure", axes=TimeAxes([123]), values=[1])
        else:
            with pytest.raises(NominalError, match="before acknowledging"):
                resource.write_points("Pressure", axes=TimeAxes([123]), values=[1])


@pytest.mark.usefixtures("close_channels")
def test_timeout_cleans_up_the_operation_and_keeps_the_shared_channel_usable(dataset: Dataset) -> None:
    """A deadline while awaiting declaration cancels that stream and a subsequent call succeeds."""
    calls = []
    cancelled = Event()

    def handler(
        requests: Iterator[writer_pb2.WriteStreamRequest], context: grpc.ServicerContext
    ) -> Iterator[writer_pb2.WriteStreamResponse]:
        """Let the first declaration expire, then serve the next operation normally."""
        calls.append(next(requests))
        if len(calls) == 1:
            context.add_callback(cancelled.set)
            assert cancelled.wait(5)
            return
        yield writer_pb2.WriteStreamResponse(accepted=writer_pb2.Accepted(sequence_numbers=[1]))
        assert len(list(requests)) == 1
        yield writer_pb2.WriteStreamResponse(accepted=writer_pb2.Accepted(sequence_numbers=[2]))

    with _writer_server(handler) as url:
        resource = _transport_dataset(dataset, url, timeout=0.5)
        with pytest.raises(NominalError, match="DEADLINE_EXCEEDED"):
            resource.write_points("Pressure", axes=TimeAxes([0]), values=[1])
        assert cancelled.wait(5)
        resource.write_points("Pressure", axes=TimeAxes([0]), values=[1])
    assert len(calls) == 2


@pytest.mark.usefixtures("close_channels")
def test_retryable_transport_failure_does_not_replay_the_operation(dataset: Dataset) -> None:
    """UNAVAILABLE before response headers does not replay a server-observed declaration."""
    calls = []

    def handler(
        requests: Iterator[writer_pb2.WriteStreamRequest], context: grpc.ServicerContext
    ) -> Iterator[writer_pb2.WriteStreamResponse]:
        """Fail without response headers at the native retry boundary."""
        calls.append(next(requests))
        context.abort(grpc.StatusCode.UNAVAILABLE, "admission unavailable")
        return iter(())

    with _writer_server(handler) as url:
        with pytest.raises(NominalError, match="admission unavailable"):
            _transport_dataset(dataset, url).write_points("Pressure", axes=TimeAxes([0]), values=[1])
    assert len(calls) == 1


@pytest.mark.parametrize(
    "axes,values,error",
    [
        (TimeAxes([1, 2]), [1], ValueError),
        (TimeFrequencyAxes([1], [10, 20]), [1], ValueError),
        (TimeAxes([True]), [1], TypeError),
        (TimeAxes([1.5]), [1], TypeError),
        (TimeAxes([2**100]), [1], ValueError),
    ],
)
def test_invalid_coordinates_fail_before_io(
    dataset: Dataset,
    mock_clients: MagicMock,
    axes: TimeAxes | TimeFrequencyAxes,
    values: list[float],
    error: type[Exception],
) -> None:
    """Lossy timestamps and misaligned columns fail before opening a stream."""
    with pytest.raises(error):
        dataset.write_points("Pressure", axes=axes, values=values)
    mock_clients.channel_writer.WriteStream.assert_not_called()
