from __future__ import annotations

import inspect
import sys
import warnings
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from nominal.core._stream.write_stream import WriteStream
from nominal.core._stream.write_stream_base import WriteStreamBase
from nominal.core.connection import StreamingConnection
from nominal.core.dataset import Dataset
from nominal.experimental.rust_streaming.rust_write_stream import RustWriteStream

# The rust backend validates the rid it is handed, so the fixtures below use a well-formed one.
_DATASET_RID = "ri.catalog.ws.dataset.abc"

# `_import_rust_write_stream` imports this module; poisoning the entry makes the import fail the
# way it does on a machine where nominal-streaming was never installed.
_RUST_MODULE = "nominal.experimental.rust_streaming.rust_write_stream"


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
    """Make importing the rust write stream fail, as it does when nominal-streaming is unavailable."""
    with patch.dict(sys.modules, {_RUST_MODULE: None}):
        yield


def test_dataset_write_stream_defaults_to_rust(mock_dataset: Dataset):
    """Omitting data_format streams over the rust backend when nominal-streaming is installed."""
    with mock_dataset.get_write_stream() as stream:
        assert isinstance(stream, RustWriteStream)


def test_connection_write_stream_defaults_to_rust(mock_connection: StreamingConnection):
    """Connections pick the same rust default as datasets when data_format is omitted."""
    with mock_connection.get_write_stream() as stream:
        assert isinstance(stream, RustWriteStream)


def test_default_stream_offers_the_same_enqueue_surface_as_json(mock_dataset: Dataset):
    """Switching the default to rust keeps every enqueue entry point JSON streaming offers."""
    expected = {name for name in dir(WriteStreamBase) if name.startswith("enqueue")}

    with mock_dataset.get_write_stream() as stream:
        assert isinstance(stream, WriteStreamBase)
        assert expected <= {name for name in dir(stream) if name.startswith("enqueue")}


def test_write_stream_rust_is_selectable_by_name(mock_dataset: Dataset):
    """data_format='rust' selects the rust backend without emitting a deprecation warning."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with mock_dataset.get_write_stream(data_format="rust") as stream:
            assert isinstance(stream, RustWriteStream)

    assert [str(warning.message) for warning in caught] == []


def test_write_stream_rust_experimental_still_works_and_warns(mock_dataset: Dataset):
    """The deprecated 'rust_experimental' alias still yields a rust stream, but warns to use 'rust'."""
    with (
        pytest.warns(UserWarning, match="rust_experimental.*deprecated"),
        mock_dataset.get_write_stream(data_format="rust_experimental") as stream,
    ):
        assert isinstance(stream, RustWriteStream)


def test_write_stream_falls_back_to_json_without_nominal_streaming(
    mock_dataset: Dataset, mock_clients: MagicMock, without_nominal_streaming: None
):
    """Without nominal-streaming, the default stream writes points over the legacy JSON API."""
    with mock_dataset.get_write_stream(batch_size=1) as stream:
        assert isinstance(stream, WriteStream)
        stream.enqueue("temperature", datetime(2025, 1, 1, tzinfo=timezone.utc), 42.0)

    mock_clients.storage_writer.write_batches.assert_called_once()
    _, request = mock_clients.storage_writer.write_batches.call_args[0]
    assert request.data_source_rid == _DATASET_RID
    assert [batch.channel for batch in request.batches] == ["temperature"]


def test_explicit_rust_raises_without_nominal_streaming(mock_dataset: Dataset, without_nominal_streaming: None):
    """Explicitly asking for rust without nominal-streaming installed is an error, not a silent downgrade."""
    with pytest.raises(ImportError, match="nominal-streaming is required"):
        mock_dataset.get_write_stream(data_format="rust")


def test_rust_only_argument_raises_without_nominal_streaming(mock_dataset: Dataset, without_nominal_streaming: None):
    """A rust-only argument is never silently dropped by the JSON fallback."""
    with pytest.raises(ImportError, match="file_fallback"):
        mock_dataset.get_write_stream(file_fallback="fallback.avro")


def test_unknown_data_format_is_rejected(mock_dataset: Dataset):
    """An unrecognized data_format names the formats that are actually supported."""
    with pytest.raises(ValueError, match="json, protobuf, experimental, rust"):
        mock_dataset.get_write_stream(data_format="parquet")  # type: ignore[call-overload]


@pytest.mark.parametrize("data_format", ["json", "protobuf"])
def test_rust_only_arguments_warn_for_non_rust_formats(
    mock_dataset: Dataset, caplog: pytest.LogCaptureFixture, data_format: str
):
    """Rust-only arguments log a warning when a non-rust data_format is explicitly chosen."""
    with mock_dataset.get_write_stream(data_format=data_format, num_workers=4):  # type: ignore[call-overload]
        pass

    assert "Argument num_workers has no effect unless `data_format='rust'`" in caplog.text


def test_rust_only_arguments_do_not_warn_for_rust(mock_dataset: Dataset, caplog: pytest.LogCaptureFixture):
    """Rust-only arguments are honored, not warned about, on the rust backend."""
    with mock_dataset.get_write_stream(data_format="rust", num_workers=4):
        pass

    assert "has no effect" not in caplog.text


@pytest.mark.parametrize("owner", [Dataset, StreamingConnection])
def test_write_stream_default_batching(owner: type) -> None:
    """Write streams buffer 250k points and flush every 250ms unless told otherwise."""
    defaults = inspect.signature(owner.get_write_stream).parameters

    assert defaults["batch_size"].default == 250_000
    assert defaults["max_wait"].default == timedelta(seconds=0.25)


def test_json_write_stream_uses_default_batching(mock_dataset: Dataset, without_nominal_streaming: None):
    """The documented defaults are the ones the constructed stream actually batches with."""
    with mock_dataset.get_write_stream() as stream:
        assert isinstance(stream, WriteStream)
        assert stream.batch_size == 250_000
        assert stream.max_wait == timedelta(seconds=0.25)
