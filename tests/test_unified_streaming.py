"""Tests for unified streaming: default format selection, deprecation warnings, and RustWriteStream."""

from __future__ import annotations

import warnings
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest

from nominal.core._stream.write_stream import WriteStream


@pytest.fixture
def mock_clients():
    clients = MagicMock()
    clients.storage_writer = MagicMock()
    clients.storage_writer._uri = "https://api.nominal.test"
    clients.auth_header = "Bearer test-token"
    clients.proto_write = MagicMock()
    return clients


class TestDefaultFormatSelection:
    """Test that _get_write_stream auto-selects the best available format."""

    def test_defaults_to_rust_when_available(self, mock_clients: MagicMock) -> None:
        """When nominal_streaming is available, data_format=None should select 'rust'."""
        with (
            patch("nominal.core.datasource._is_nominal_streaming_available", return_value=True),
            patch("nominal.core._stream.rust_write_stream.RustWriteStream") as mock_rust,
        ):
            mock_rust._from_datasource.return_value = MagicMock()

            from nominal.core.datasource import _get_write_stream

            _get_write_stream(
                batch_size=50_000,
                max_wait=timedelta(seconds=1),
                data_format=None,
                file_fallback=None,
                log_level=None,
                num_workers=None,
                write_rid="test-rid",
                clients=mock_clients,
            )

            mock_rust._from_datasource.assert_called_once()

    def test_defaults_to_protobuf_when_rust_unavailable(self, mock_clients: MagicMock) -> None:
        """When nominal_streaming is not available but protos are, should select 'protobuf'."""
        with (
            patch("nominal.core.datasource._is_nominal_streaming_available", return_value=False),
            patch("nominal.core._stream.batch_processor_proto.process_batch") as mock_proto_batch,
        ):
            from nominal.core.datasource import _get_write_stream

            stream = _get_write_stream(
                batch_size=50_000,
                max_wait=timedelta(seconds=1),
                data_format=None,
                file_fallback=None,
                log_level=None,
                num_workers=None,
                write_rid="test-rid",
                clients=mock_clients,
            )

            # Should return a WriteStream (Python batching)
            assert isinstance(stream, WriteStream)
            stream.close(wait=False)

    def test_defaults_to_json_when_nothing_available(self, mock_clients: MagicMock) -> None:
        """When neither Rust nor protos available, should fall back to 'json'."""
        import sys

        with (
            patch("nominal.core.datasource._is_nominal_streaming_available", return_value=False),
            patch.dict(sys.modules, {"nominal_api_protos": None}),
        ):
            from nominal.core.datasource import _get_write_stream

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                stream = _get_write_stream(
                    batch_size=50_000,
                    max_wait=timedelta(seconds=1),
                    data_format=None,
                    file_fallback=None,
                    log_level=None,
                    num_workers=None,
                    write_rid="test-rid",
                    clients=mock_clients,
                )

            assert isinstance(stream, WriteStream)
            stream.close(wait=False)


class TestDeprecationWarnings:
    """Test that deprecated formats emit DeprecationWarning."""

    def test_json_format_emits_deprecation_warning(self, mock_clients: MagicMock) -> None:
        from nominal.core.datasource import _get_write_stream

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            stream = _get_write_stream(
                batch_size=50_000,
                max_wait=timedelta(seconds=1),
                data_format="json",
                file_fallback=None,
                log_level=None,
                num_workers=None,
                write_rid="test-rid",
                clients=mock_clients,
            )
            stream.close(wait=False)

        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert any("json" in str(x.message).lower() for x in deprecation_warnings)

    def test_rust_experimental_alias_emits_deprecation_warning(self, mock_clients: MagicMock) -> None:
        with (
            warnings.catch_warnings(record=True) as w,
            patch("nominal.core._stream.rust_write_stream.RustWriteStream") as mock_rust,
        ):
            warnings.simplefilter("always")
            mock_rust._from_datasource.return_value = MagicMock()

            from nominal.core.datasource import _get_write_stream

            _get_write_stream(
                batch_size=50_000,
                max_wait=timedelta(seconds=1),
                data_format="rust_experimental",
                file_fallback=None,
                log_level=None,
                num_workers=None,
                write_rid="test-rid",
                clients=mock_clients,
            )

        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert any("rust_experimental" in str(x.message) for x in deprecation_warnings)

    def test_explicit_rust_format_no_deprecation(self, mock_clients: MagicMock) -> None:
        with (
            warnings.catch_warnings(record=True) as w,
            patch("nominal.core._stream.rust_write_stream.RustWriteStream") as mock_rust,
        ):
            warnings.simplefilter("always")
            mock_rust._from_datasource.return_value = MagicMock()

            from nominal.core.datasource import _get_write_stream

            _get_write_stream(
                batch_size=50_000,
                max_wait=timedelta(seconds=1),
                data_format="rust",
                file_fallback=None,
                log_level=None,
                num_workers=None,
                write_rid="test-rid",
                clients=mock_clients,
            )

        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(deprecation_warnings) == 0

    def test_invalid_format_raises_value_error(self, mock_clients: MagicMock) -> None:
        from nominal.core.datasource import _get_write_stream

        with pytest.raises(ValueError, match="Expected `data_format`"):
            _get_write_stream(
                batch_size=50_000,
                max_wait=timedelta(seconds=1),
                data_format="invalid_format",  # type: ignore[arg-type]
                file_fallback=None,
                log_level=None,
                num_workers=None,
                write_rid="test-rid",
                clients=mock_clients,
            )


class TestRustWriteStreamModule:
    """Test the new core RustWriteStream module."""

    def test_nominal_streaming_available_check(self) -> None:
        """_is_nominal_streaming_available should return a boolean."""
        from nominal.core.datasource import _is_nominal_streaming_available

        assert isinstance(_is_nominal_streaming_available(), bool)

    def test_rust_write_stream_raises_not_implemented_for_arrays(self) -> None:
        """RustWriteStream should raise NotImplementedError for unsupported types."""
        from nominal.core._stream.rust_write_stream import RustWriteStream

        mock_inner = MagicMock()
        stream = RustWriteStream(mock_inner)

        with pytest.raises(NotImplementedError, match="Array streaming"):
            stream.enqueue_float_array("ch", "2024-01-01T00:00:00Z", [1.0, 2.0])

        with pytest.raises(NotImplementedError, match="Array streaming"):
            stream.enqueue_string_array("ch", "2024-01-01T00:00:00Z", ["a", "b"])

        with pytest.raises(NotImplementedError, match="Struct streaming"):
            stream.enqueue_struct("ch", "2024-01-01T00:00:00Z", {"key": "value"})

    def test_rust_write_stream_delegates_enqueue(self) -> None:
        """RustWriteStream.enqueue should delegate to the inner NominalDatasetStream."""
        from nominal.core._stream.rust_write_stream import RustWriteStream

        mock_inner = MagicMock()
        stream = RustWriteStream(mock_inner)

        stream.enqueue("channel1", "2024-01-01T00:00:00Z", 42.0)
        mock_inner.enqueue.assert_called_once_with("channel1", "2024-01-01T00:00:00Z", 42.0, None)

    def test_rust_write_stream_delegates_close(self) -> None:
        """RustWriteStream.close should delegate to the inner stream."""
        from nominal.core._stream.rust_write_stream import RustWriteStream

        mock_inner = MagicMock()
        stream = RustWriteStream(mock_inner)

        stream.close(wait=True)
        mock_inner.close.assert_called_once_with(wait=True)

    def test_rust_write_stream_context_manager(self) -> None:
        """RustWriteStream should work as a context manager."""
        from nominal.core._stream.rust_write_stream import RustWriteStream

        mock_inner = MagicMock()
        stream = RustWriteStream(mock_inner)

        with stream as s:
            assert s is stream
            mock_inner.__enter__.assert_called_once()

        mock_inner.__exit__.assert_called_once()
