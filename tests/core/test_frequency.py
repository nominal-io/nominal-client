from __future__ import annotations

from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from unittest.mock import MagicMock

import grpc
import pytest
from conjure_python_client import ServiceConfiguration
from nominal_api import scout_catalog

from nominal.core._clientsbunch import ClientsBunch
from nominal.core._utils.networking import StaticHeaderProvider
from nominal.core.dataset import Dataset
from nominal.exceptions import NominalError, NominalPermissionDeniedError
from nominal.protos.channel_writer.v1 import nominal_channel_writer_pb2 as writer_pb2
from nominal.protos.channel_writer.v1 import nominal_channel_writer_pb2_grpc


@pytest.fixture
def dataset(mock_clients: MagicMock, make_enriched_dataset: Callable[..., scout_catalog.EnrichedDataset]) -> Dataset:
    """Use the ordinary dataset wrapper with mocked service I/O."""
    return Dataset._from_conjure(mock_clients, make_enriched_dataset())


@pytest.mark.parametrize("unit", [None, "", "g^2/Hz"])
def test_frequency_preserves_coordinates_and_metadata(
    dataset: Dataset, mock_clients: MagicMock, unit: str | None
) -> None:
    """One generated batch preserves nanoseconds, order, negative bins and unit presence."""
    timestamps = [1767268801000000123, 1767268800000000123, 1767268800000000123]
    tags = {"sensor": "x"}
    assert (
        dataset.write_frequency("Acceleration", timestamps, [30, -10, 20], [1.25, 0.5, 0.125], unit=unit, tags=tags)
        is None
    )
    mock_clients.channel_writer.WriteColumnBatches.assert_called_once()
    (request,) = mock_clients.channel_writer.WriteColumnBatches.call_args.args
    assert request.data_source_rid == dataset.rid
    assert len(request.batches) == 1
    batch = request.batches[0]
    assert batch.channel == "Acceleration"
    assert dict(batch.tags) == {"sensor": "x"}
    assert batch.HasField("unit") == (unit is not None)
    if unit is not None:
        assert batch.unit == unit
    assert not batch.timestamps
    axes = batch.canonical_axes.time_frequency
    assert [(ts.seconds, ts.nanos) for ts in axes.timestamps] == [
        (1767268801, 123),
        (1767268800, 123),
        (1767268800, 123),
    ]
    assert list(axes.frequencies) == [30.0, -10.0, 20.0]
    assert list(batch.values.doubles.points) == [1.25, 0.5, 0.125]
    timestamps[0] = 0
    tags["sensor"] = "changed"
    assert axes.timestamps[0].seconds == 1767268801
    assert batch.tags["sensor"] == "x"


@pytest.mark.parametrize(
    ("timestamps", "frequencies", "values", "error"),
    [
        ([], [], [], ValueError),
        ([0], [1, 2], [1], ValueError),
        ([0] * 10_001, [1] * 10_001, [1] * 10_001, ValueError),
        ([True], [1], [1], TypeError),
        ([1.5], [1], [1], TypeError),
        ([-1], [1], [1], ValueError),
        ([2**63], [1], [1], ValueError),
        ([0], [float("nan")], [1], ValueError),
        ([0], [1], [float("inf")], ValueError),
        ([0], [1], [10**400], ValueError),
    ],
)
def test_frequency_rejects_invalid_columns_before_io(
    dataset: Dataset,
    mock_clients: MagicMock,
    timestamps: list[int],
    frequencies: list[float],
    values: list[float],
    error: type[Exception],
) -> None:
    """Invalid shapes, coordinates and numeric values cannot reach admission."""
    with pytest.raises(error):
        dataset.write_frequency("Acceleration", timestamps, frequencies, values)
    mock_clients.channel_writer.WriteColumnBatches.assert_not_called()


@pytest.mark.parametrize("oversized_rid", [False, True])
def test_frequency_size_limit_includes_rid_and_metadata(
    dataset: Dataset, mock_clients: MagicMock, oversized_rid: bool
) -> None:
    """The full protobuf byte bound includes both dataset identity and metadata."""
    if oversized_rid:
        dataset = replace(dataset, rid="r" * 262_144)
    tags = {} if oversized_rid else {"note": "x" * 262_144}
    with pytest.raises(ValueError, match="262,144"):
        dataset.write_frequency("Acceleration", [0], [1], [1], tags=tags)
    mock_clients.channel_writer.WriteColumnBatches.assert_not_called()


def test_frequency_accepts_timestamp_endpoints(dataset: Dataset, mock_clients: MagicMock) -> None:
    """Both admitted nanosecond endpoints retain their exact seconds and nanos."""
    dataset.write_frequency("Acceleration", [0, 2**63 - 1], [0, 1], [1, 2])
    request = mock_clients.channel_writer.WriteColumnBatches.call_args.args[0]
    assert [(ts.seconds, ts.nanos) for ts in request.batches[0].canonical_axes.time_frequency.timestamps] == [
        (0, 0),
        (9223372036, 854775807),
    ]


def test_frequency_accepts_point_limit(dataset: Dataset, mock_clients: MagicMock) -> None:
    """The point cap is inclusive when the full request also fits the byte cap."""
    dataset.write_frequency("Acceleration", [1767268800000000000] * 10_000, [1] * 10_000, [1] * 10_000)
    mock_clients.channel_writer.WriteColumnBatches.assert_called_once()
    request = mock_clients.channel_writer.WriteColumnBatches.call_args.args[0]
    assert len(request.batches[0].canonical_axes.time_frequency.timestamps) == 10_000


def test_frequency_accepts_exact_byte_limit(dataset: Dataset, mock_clients: MagicMock) -> None:
    """Accept the complete protobuf byte limit, then reject a one-byte increase."""
    dataset.write_frequency("Acceleration", [0], [1], [1], tags={"note": "x" * 250_000})
    request = mock_clients.channel_writer.WriteColumnBatches.call_args.args[0]
    padding = "x" * (250_000 + 262_144 - request.ByteSize())
    mock_clients.channel_writer.reset_mock()
    dataset.write_frequency("Acceleration", [0], [1], [1], tags={"note": padding})
    request = mock_clients.channel_writer.WriteColumnBatches.call_args.args[0]
    assert request.ByteSize() == 262_144
    with pytest.raises(ValueError, match="262,144"):
        dataset.write_frequency("Acceleration", [0], [1], [1], tags={"note": padding + "x"})
    mock_clients.channel_writer.WriteColumnBatches.assert_called_once()


@contextmanager
def _grpc_endpoint(
    status: grpc.StatusCode | None,
) -> Iterator[tuple[str, list[writer_pb2.WriteColumnBatchesRequest], dict[str, str | bytes], list[float]]]:
    """Observe generated gRPC writes before success or a potentially ambiguous failure."""
    calls: list[writer_pb2.WriteColumnBatchesRequest] = []
    headers: dict[str, str | bytes] = {}
    deadlines: list[float] = []

    class Writer(nominal_channel_writer_pb2_grpc.NominalChannelWriterServiceServicer):
        def WriteColumnBatches(self, request, context):
            """Record the delivered request before returning or failing admission."""
            calls.append(request)
            headers.update(context.invocation_metadata())
            deadlines.append(context.time_remaining())
            if status is not None:
                context.abort(status, "admission failed")
            return writer_pb2.WriteColumnBatchesResponse()

    with ThreadPoolExecutor(max_workers=1) as executor:
        server = grpc.server(executor)
        nominal_channel_writer_pb2_grpc.add_NominalChannelWriterServiceServicer_to_server(Writer(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            yield f"http://127.0.0.1:{port}/api", calls, headers, deadlines
        finally:
            server.stop(0).wait()


def _transport_dataset(dataset: Dataset, base_url: str) -> Dataset:
    """Build the real SDK transport with a synthetic credential and extra header."""
    clients = ClientsBunch.from_config(
        ServiceConfiguration(uris=[base_url], read_timeout=30, backoff_slot_size=1),
        base_url,
        "frequency-test",
        "synthetic-token",
        None,
        header_provider=StaticHeaderProvider({"X-Test-Context": "retained"}),
    )
    return replace(dataset, _clients=clients)


def test_frequency_grpc_preserves_request_and_configured_call_context(dataset: Dataset) -> None:
    """The generated writer receives exact coordinates, identity, metadata and the configured deadline."""
    with _grpc_endpoint(None) as (url, calls, headers, deadlines):
        dataset = _transport_dataset(dataset, url)
        assert (
            dataset.write_frequency("Acceleration", [1767268800000000123], [10], [0.25], unit="", tags={"sensor": "x"})
            is None
        )
    assert len(calls) == 1
    request = calls[0]
    assert request.data_source_rid == dataset.rid
    assert len(request.batches) == 1
    batch = request.batches[0]
    assert batch.channel == "Acceleration"
    assert dict(batch.tags) == {"sensor": "x"}
    assert batch.HasField("unit") and batch.unit == ""
    axes = batch.canonical_axes.time_frequency
    assert [(ts.seconds, ts.nanos) for ts in axes.timestamps] == [(1767268800, 123)]
    assert list(axes.frequencies) == [10.0]
    assert list(batch.values.doubles.points) == [0.25]
    assert headers["authorization"] == "Bearer synthetic-token"
    assert headers["x-test-context"] == "retained"
    assert "frequency-test" in headers["user-agent"]
    assert 0 < deadlines[0] <= 30.1


@pytest.mark.parametrize(
    ("status", "error"),
    [
        (grpc.StatusCode.UNAVAILABLE, NominalError),
        (grpc.StatusCode.RESOURCE_EXHAUSTED, NominalError),
        (grpc.StatusCode.PERMISSION_DENIED, NominalPermissionDeniedError),
    ],
)
def test_frequency_grpc_does_not_replay_failed_admission(
    dataset: Dataset, status: grpc.StatusCode, error: type[NominalError]
) -> None:
    """A server-observed write fails once with an SDK error even for normally retryable statuses."""
    with _grpc_endpoint(status) as (url, calls, _, _):
        dataset = _transport_dataset(dataset, url)
        with pytest.raises(error, match="admission failed") as exc:
            dataset.write_frequency("Acceleration", [0], [1], [1])
    assert len(calls) == 1
    assert isinstance(exc.value.__cause__, grpc.RpcError)
    assert exc.value.__cause__.code() == status
