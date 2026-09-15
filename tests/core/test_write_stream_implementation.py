from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from nominal.core._stream.write_stream import WriteStream
from nominal.core.connection import StreamingConnection
from nominal.core.dataset import Dataset

# The rust backend validates the rid it is handed, so the fixtures below use a well-formed one.
_DATASET_RID = "ri.catalog.ws.dataset.abc"


@pytest.fixture
def mock_clients():
    clients = MagicMock()
    clients.auth_header = "Bearer test-token"
    clients.storage_writer._uri = "https://api.example.com"
    return clients


@pytest.fixture
def mock_dataset(mock_clients):
    return Dataset(
        rid=_DATASET_RID,
        _clients=mock_clients,
        name="test dataset",
        description=None,
        properties={},
        labels=[],
        bounds=None,
        is_archived=False,
    )


@pytest.fixture
def mock_connection(mock_clients):
    return StreamingConnection(
        rid="test-connection-rid",
        name="Test Connection",
        description="A connection for testing",
        _clients=mock_clients,
        nominal_data_source_rid=_DATASET_RID,
    )


@pytest.fixture
def without_nominal_streaming():
    """Hide nominal-streaming, as it is on a platform it ships no binary for.

    Blanks the third-party package rather than ours, and evicts our already-imported wrapper so the
    import runs again and fails the way it would on such a machine.
    """
    with patch.dict(sys.modules):
        for name in list(sys.modules):
            if name.startswith("nominal.experimental.rust_streaming"):
                del sys.modules[name]
        sys.modules["nominal_streaming"] = None  # type: ignore[assignment]
        yield


def rust_write_stream_type() -> type:
    """Import RustWriteStream here rather than at module scope, skipping if it is unavailable.

    nominal-streaming carries platform markers, so a module-level import would fail collection of
    this whole file on the platforms whose fallback behavior it exists to pin down.
    """
    pytest.importorskip("nominal_streaming")
    from nominal.experimental.rust_streaming import RustWriteStream

    return RustWriteStream


def test_dataset_write_stream_defaults_to_rust(mock_dataset: Dataset):
    """Omitting implementation streams over the rust backend when nominal-streaming is installed."""
    with mock_dataset.get_write_stream() as stream:
        assert isinstance(stream, rust_write_stream_type())


def test_connection_write_stream_defaults_to_rust(mock_connection: StreamingConnection):
    """Connections pick the same rust default as datasets when implementation is omitted."""
    with mock_connection.get_write_stream() as stream:
        assert isinstance(stream, rust_write_stream_type())


def test_falls_back_to_python_without_nominal_streaming(
    mock_dataset: Dataset, mock_clients: MagicMock, without_nominal_streaming: None
):
    """Without nominal-streaming the default stream is the pure-python one, writing protobuf."""
    with mock_dataset.get_write_stream(batch_size=1) as stream:
        stream.enqueue("temperature", datetime(2025, 1, 1, tzinfo=timezone.utc), 42.0)

    mock_clients.proto_write.write_nominal_batches.assert_called_once()
    mock_clients.storage_writer.write_batches.assert_not_called()


def test_explicit_rust_raises_without_nominal_streaming(mock_dataset: Dataset, without_nominal_streaming: None):
    """Explicitly asking for rust without nominal-streaming installed is an error, not a silent downgrade."""
    with pytest.raises(ImportError, match="nominal-streaming is required"):
        mock_dataset.get_write_stream(implementation="rust")


def test_rust_only_argument_raises_without_nominal_streaming(mock_dataset: Dataset, without_nominal_streaming: None):
    """A rust-only argument is never silently dropped by the fallback."""
    with pytest.raises(ImportError, match="file_fallback"):
        mock_dataset.get_write_stream(file_fallback="fallback.avro")


@pytest.mark.parametrize("implementation", ["json", "protobuf"])
def test_superseded_python_spellings_resolve_to_python(
    mock_dataset: Dataset, mock_clients: MagicMock, implementation: str
):
    """'json' and 'protobuf' both now mean 'python', which writes protobuf on the wire."""
    with (
        pytest.warns(UserWarning, match="use implementation='python'"),
        mock_dataset.get_write_stream(batch_size=1, implementation=implementation) as stream,  # type: ignore[call-overload]
    ):
        stream.enqueue("temperature", datetime(2025, 1, 1, tzinfo=timezone.utc), 42.0)

    mock_clients.proto_write.write_nominal_batches.assert_called_once()
    mock_clients.storage_writer.write_batches.assert_not_called()


def test_rust_experimental_resolves_to_rust(mock_dataset: Dataset):
    """The superseded 'rust_experimental' spelling still yields a rust stream."""
    with (
        pytest.warns(UserWarning, match="use implementation='rust'"),
        mock_dataset.get_write_stream(implementation="rust_experimental") as stream,
    ):
        assert isinstance(stream, rust_write_stream_type())


def test_experimental_is_not_collapsed_into_python(mock_dataset: Dataset):
    """'experimental' is not aliased away while it is the only implementation streaming metrics."""
    from nominal.experimental.stream_v2._write_stream import WriteStreamV2

    with (
        pytest.warns(UserWarning, match="does not carry streaming metrics yet"),
        mock_dataset.get_write_stream(implementation="experimental") as stream,
    ):
        assert isinstance(stream, WriteStreamV2)


def test_data_format_still_selects_an_implementation(mock_dataset: Dataset):
    """The superseded `data_format` argument keeps working for callers who have not migrated."""
    with (
        pytest.warns(UserWarning, match="`data_format` argument is deprecated"),
        mock_dataset.get_write_stream(data_format="python") as stream,
    ):
        assert isinstance(stream, WriteStream)


def test_implementation_is_accepted_positionally(mock_dataset: Dataset):
    """The implementation slot is the third positional parameter `data_format` used to occupy."""
    with mock_dataset.get_write_stream(250_000, timedelta(seconds=0.25), "python") as stream:
        assert isinstance(stream, WriteStream)


@pytest.mark.filterwarnings("ignore::UserWarning")
def test_implementation_and_data_format_together_is_an_error(mock_dataset: Dataset):
    """Passing both spellings is rejected rather than silently preferring one.

    The overloads make this a type error too; the ignore is what a caller without a type checker
    would be doing, which is the case this guards.
    """
    with pytest.raises(ValueError, match="Pass only `implementation`"):
        mock_dataset.get_write_stream(implementation="rust", data_format="rust")  # type: ignore[call-overload]


def test_unknown_implementation_is_rejected(mock_dataset: Dataset):
    """An unrecognized implementation names the ones that are actually supported."""
    with pytest.raises(ValueError, match="python, rust, experimental"):
        mock_dataset.get_write_stream(implementation="parquet")  # type: ignore[call-overload]


def test_connection_writes_to_its_datasource_not_itself(mock_connection: StreamingConnection, mock_clients: MagicMock):
    """A streaming connection routes writes to its backing datasource rid, not its own rid."""
    with mock_connection.get_write_stream(batch_size=1, implementation="python") as stream:
        stream.enqueue("temperature", datetime(2025, 1, 1, tzinfo=timezone.utc), 42.0)

    kwargs = mock_clients.proto_write.write_nominal_batches.call_args.kwargs
    assert kwargs["data_source_rid"] == _DATASET_RID
    assert kwargs["data_source_rid"] != mock_connection.rid


def test_default_stream_accepts_enqueue_without_a_context_manager(mock_dataset: Dataset):
    """The default stream takes enqueues when closed explicitly, the lifecycle the docstring offers."""
    stream = mock_dataset.get_write_stream()
    try:
        stream.enqueue("temperature", datetime(2025, 1, 1, tzinfo=timezone.utc), 42.0)
    finally:
        stream.close()


def test_default_stream_accepts_arrays_and_structs(mock_dataset: Dataset):
    """The default stream takes the array and struct enqueues WriteStreamBase declares."""
    timestamp = datetime(2025, 1, 1, tzinfo=timezone.utc)

    with mock_dataset.get_write_stream() as stream:
        stream.enqueue_float_array("floats", timestamp, [1.0, 2.0])
        stream.enqueue_string_array("strings", timestamp, ["a", "b"])
        stream.enqueue_struct("struct", timestamp, {"key": 1})


def test_default_stream_flush_warns_and_does_nothing(mock_dataset: Dataset):
    """flush() on the default stream is a no-op that says so, and leaves the stream usable."""
    with mock_dataset.get_write_stream() as stream:
        with pytest.warns(UserWarning, match=r"flush\(\) does nothing"):
            stream.flush(wait=True)  # type: ignore[attr-defined]

        stream.enqueue("temperature", datetime(2025, 1, 1, tzinfo=timezone.utc), 42.0)
