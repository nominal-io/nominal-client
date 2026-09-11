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
from nominal.core.exceptions import StreamImplementationDeprecationWarning
from nominal.experimental.rust_streaming.rust_write_stream import RustWriteStream
from nominal.experimental.stream_v2._write_stream import WriteStreamV2

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
    """Omitting implementation streams over the rust backend when nominal-streaming is installed."""
    with mock_dataset.get_write_stream() as stream:
        assert isinstance(stream, RustWriteStream)


def test_connection_write_stream_defaults_to_rust(mock_connection: StreamingConnection):
    """Connections pick the same rust default as datasets when implementation is omitted."""
    with mock_connection.get_write_stream() as stream:
        assert isinstance(stream, RustWriteStream)


def test_default_stream_offers_the_full_enqueue_surface(mock_dataset: Dataset):
    """Switching the default to rust keeps every enqueue entry point the other implementations offer."""
    expected = {name for name in dir(WriteStreamBase) if name.startswith("enqueue")}

    with mock_dataset.get_write_stream() as stream:
        assert isinstance(stream, WriteStreamBase)
        assert expected <= {name for name in dir(stream) if name.startswith("enqueue")}


def test_rust_is_selectable_by_name_without_warning(mock_dataset: Dataset):
    """implementation='rust' selects the rust backend and is not deprecated."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with mock_dataset.get_write_stream(implementation="rust") as stream:
            assert isinstance(stream, RustWriteStream)

    assert [str(warning.message) for warning in caught] == []


def test_implementation_is_accepted_positionally(mock_dataset: Dataset):
    """The implementation slot is the third positional parameter `data_format` used to occupy."""
    with mock_dataset.get_write_stream(250_000, timedelta(seconds=0.25), "python") as stream:
        assert isinstance(stream, WriteStream)


@pytest.mark.parametrize("implementation", ["json", "protobuf", "experimental", "rust_experimental"])
def test_deprecated_implementations_still_work(mock_dataset: Dataset, implementation: str):
    """Every deprecated implementation still builds a stream, warning to move to 'rust'."""
    with (
        pytest.warns(StreamImplementationDeprecationWarning, match=f"implementation={implementation!r} is deprecated"),
        mock_dataset.get_write_stream(implementation=implementation) as stream,  # type: ignore[call-overload]
    ):
        assert isinstance(stream, WriteStreamBase)


def test_rust_experimental_resolves_to_rust(mock_dataset: Dataset):
    """The deprecated 'rust_experimental' spelling still yields a rust stream."""
    with (
        pytest.warns(StreamImplementationDeprecationWarning, match="use implementation='rust'"),
        mock_dataset.get_write_stream(implementation="rust_experimental") as stream,
    ):
        assert isinstance(stream, RustWriteStream)


def test_data_format_argument_is_deprecated_but_honored(mock_dataset: Dataset):
    """The old `data_format` argument still selects an implementation, warning to rename the call."""
    with (
        pytest.warns(StreamImplementationDeprecationWarning, match="`data_format` argument is deprecated"),
        mock_dataset.get_write_stream(data_format="python") as stream,
    ):
        assert isinstance(stream, WriteStream)


def test_deprecation_warning_blames_the_caller(mock_dataset: Dataset):
    """Deprecation warnings point at the user's call site, not at nominal's internals."""
    with pytest.warns(StreamImplementationDeprecationWarning) as caught:
        mock_dataset.get_write_stream(data_format="python").close()

    assert [warning.filename for warning in caught] == [__file__]


def test_implementation_and_data_format_together_is_an_error(mock_dataset: Dataset):
    """Passing both spellings is rejected rather than silently preferring one."""
    with pytest.raises(ValueError, match="Pass only `implementation`"):
        mock_dataset.get_write_stream(implementation="rust", data_format="rust")


def test_falls_back_to_python_without_nominal_streaming(
    mock_dataset: Dataset, mock_clients: MagicMock, without_nominal_streaming: None
):
    """Without nominal-streaming the default stream is the pure-python one, writing protobuf."""
    with mock_dataset.get_write_stream(batch_size=1) as stream:
        stream.enqueue("temperature", datetime(2025, 1, 1, tzinfo=timezone.utc), 42.0)

    mock_clients.proto_write.write_nominal_batches.assert_called_once()
    mock_clients.storage_writer.write_batches.assert_not_called()


def test_python_fallback_is_not_treated_as_a_deprecated_choice(mock_dataset: Dataset, without_nominal_streaming: None):
    """Landing on python via the fallback is nominal's choice, so it does not warn the caller."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        mock_dataset.get_write_stream().close()

    assert [str(warning.message) for warning in caught] == []


def test_explicit_rust_raises_without_nominal_streaming(mock_dataset: Dataset, without_nominal_streaming: None):
    """Explicitly asking for rust without nominal-streaming installed is an error, not a silent downgrade."""
    with pytest.raises(ImportError, match="nominal-streaming is required"):
        mock_dataset.get_write_stream(implementation="rust")


def test_rust_only_argument_raises_without_nominal_streaming(mock_dataset: Dataset, without_nominal_streaming: None):
    """A rust-only argument is never silently dropped by the fallback."""
    with pytest.raises(ImportError, match="file_fallback"):
        mock_dataset.get_write_stream(file_fallback="fallback.avro")


def test_unknown_implementation_is_rejected(mock_dataset: Dataset):
    """An unrecognized implementation names the ones that are actually supported."""
    with pytest.raises(ValueError, match="python, rust, experimental"):
        mock_dataset.get_write_stream(implementation="parquet")  # type: ignore[call-overload]


def test_rust_only_arguments_warn_for_other_implementations(mock_dataset: Dataset, caplog: pytest.LogCaptureFixture):
    """Rust-only arguments log a warning when a non-rust implementation is explicitly chosen."""
    with mock_dataset.get_write_stream(implementation="python", num_workers=4):  # type: ignore[call-overload]
        pass

    assert "Argument num_workers has no effect unless `implementation='rust'`" in caplog.text


def test_rust_only_arguments_do_not_warn_for_rust(mock_dataset: Dataset, caplog: pytest.LogCaptureFixture):
    """Rust-only arguments are honored, not warned about, on the rust backend."""
    with mock_dataset.get_write_stream(implementation="rust", num_workers=4):
        pass

    assert "has no effect" not in caplog.text


@pytest.mark.parametrize("owner", [Dataset, StreamingConnection])
def test_write_stream_default_batching(owner: type) -> None:
    """Write streams buffer 250k points and flush every 250ms unless told otherwise."""
    defaults = inspect.signature(owner.get_write_stream).parameters

    assert defaults["batch_size"].default == 250_000
    assert defaults["max_wait"].default == timedelta(seconds=0.25)


def test_constructed_stream_uses_default_batching(mock_dataset: Dataset, without_nominal_streaming: None):
    """The documented defaults are the ones the constructed stream actually batches with."""
    with mock_dataset.get_write_stream() as stream:
        assert isinstance(stream, WriteStream)
        assert stream.batch_size == 250_000
        assert stream.max_wait == timedelta(seconds=0.25)


@pytest.mark.parametrize("implementation", ["json", "protobuf"])
def test_superseded_python_spellings_resolve_to_python(
    mock_dataset: Dataset, mock_clients: MagicMock, implementation: str
):
    """'json' and 'protobuf' both now mean 'python', which writes protobuf on the wire."""
    with (
        pytest.warns(StreamImplementationDeprecationWarning, match="use implementation='python'"),
        mock_dataset.get_write_stream(batch_size=1, implementation=implementation) as stream,  # type: ignore[call-overload]
    ):
        stream.enqueue("temperature", datetime(2025, 1, 1, tzinfo=timezone.utc), 42.0)

    mock_clients.proto_write.write_nominal_batches.assert_called_once()
    mock_clients.storage_writer.write_batches.assert_not_called()


def test_experimental_is_not_collapsed_into_python(mock_dataset: Dataset):
    """'experimental' stays its own implementation: folding it in would drop its streaming metrics."""
    with (
        pytest.warns(StreamImplementationDeprecationWarning, match="kept only for its streaming metrics"),
        mock_dataset.get_write_stream(implementation="experimental") as stream,
    ):
        assert isinstance(stream, WriteStreamV2)
