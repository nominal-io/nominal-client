"""Tests for nominal.experimental.migration.utils.file_utils."""

from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.experimental.migration.utils.file_utils import copy_file_to_dataset

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source_file(s3_key: str, timestamp_channel: str | None, timestamp_type: object | None) -> MagicMock:
    """Build a mock DatasetFile with an S3 handle."""
    source_file = MagicMock()
    source_file.dataset_rid = "ri.datasets.stack.dataset.abc"
    source_file.id = "ri.datasets.stack.dataset-file.abc"
    source_file.timestamp_channel = timestamp_channel
    source_file.timestamp_type = timestamp_type
    source_file.tag_columns = None
    source_file.file_tags = None

    api_file = MagicMock()
    api_file.handle.s3.key = s3_key
    source_file._get_latest_api.return_value = api_file

    uri_response = MagicMock()
    uri_response.uri = "https://s3.example.com/file"
    source_file._clients.catalog.get_dataset_file_uri.return_value = uri_response
    source_file._clients.auth_header = "Bearer token"

    return source_file


def _make_http_response(content: bytes = b"data") -> MagicMock:
    """Build a mock requests.Response whose .raw behaves like a readable stream."""
    response = MagicMock()
    # shutil.copyfileobj calls response.raw.read(length) in a loop
    response.raw.read.side_effect = [content, b""]
    return response


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCopyFileToDataset:
    @patch("nominal.experimental.migration.utils.file_utils.requests.get")
    def test_journal_json_calls_add_journal_json(self, mock_get: MagicMock) -> None:
        """Journal JSON files are ingested via add_journal_json, not add_from_io."""
        source_file = _make_source_file(
            s3_key="2026-01-01T00:00:00Z_system.jsonl",
            timestamp_channel=None,  # journal JSON has no timestamp_channel
            timestamp_type=None,
        )
        mock_get.return_value = _make_http_response(b'{"__REALTIME_TIMESTAMP": "1000"}')

        destination_dataset = MagicMock()
        new_file = MagicMock()
        destination_dataset.add_journal_json.return_value = new_file

        result = copy_file_to_dataset(source_file, destination_dataset)

        destination_dataset.add_journal_json.assert_called_once()
        destination_dataset.add_from_io.assert_not_called()
        assert result is new_file

    @patch("nominal.experimental.migration.utils.file_utils.requests.get")
    def test_journal_json_gz_calls_add_journal_json(self, mock_get: MagicMock) -> None:
        """.jsonl.gz files are also routed through add_journal_json."""
        source_file = _make_source_file(
            s3_key="2026-01-01T00:00:00Z_system.jsonl.gz",
            timestamp_channel=None,
            timestamp_type=None,
        )
        mock_get.return_value = _make_http_response(b"compressed-data")

        destination_dataset = MagicMock()
        destination_dataset.add_journal_json.return_value = MagicMock()

        copy_file_to_dataset(source_file, destination_dataset)

        destination_dataset.add_journal_json.assert_called_once()

    @patch("nominal.experimental.migration.utils.file_utils.requests.get")
    def test_journal_json_temp_file_cleaned_up(self, mock_get: MagicMock) -> None:
        """Temp file is deleted after add_journal_json returns, even on success."""
        source_file = _make_source_file(
            s3_key="2026-01-01T00:00:00Z_system.jsonl",
            timestamp_channel=None,
            timestamp_type=None,
        )
        mock_get.return_value = _make_http_response(b"log-data")

        captured_path: list[str] = []

        def capture_path(path: str) -> MagicMock:
            captured_path.append(path)
            return MagicMock()

        destination_dataset = MagicMock()
        destination_dataset.add_journal_json.side_effect = capture_path

        copy_file_to_dataset(source_file, destination_dataset)

        assert captured_path, "add_journal_json was not called"
        assert not os.path.exists(captured_path[0]), "Temp file was not cleaned up"

    @patch("nominal.experimental.migration.utils.file_utils.requests.get")
    def test_journal_json_temp_file_cleaned_up_on_error(self, mock_get: MagicMock) -> None:
        """Temp file is deleted even if add_journal_json raises."""
        source_file = _make_source_file(
            s3_key="2026-01-01T00:00:00Z_system.jsonl",
            timestamp_channel=None,
            timestamp_type=None,
        )
        mock_get.return_value = _make_http_response(b"log-data")

        captured_path: list[str] = []

        def capture_and_raise(path: str) -> None:
            captured_path.append(path)
            raise RuntimeError("ingest failed")

        destination_dataset = MagicMock()
        destination_dataset.add_journal_json.side_effect = capture_and_raise

        with pytest.raises(RuntimeError, match="ingest failed"):
            copy_file_to_dataset(source_file, destination_dataset)

        assert captured_path, "add_journal_json was not called"
        assert not os.path.exists(captured_path[0]), "Temp file was not cleaned up after error"

    @patch("nominal.experimental.migration.utils.file_utils.requests.get")
    def test_csv_calls_add_from_io(self, mock_get: MagicMock) -> None:
        """CSV files are ingested via add_from_io using the source timestamp metadata."""
        source_file = _make_source_file(
            s3_key="2026-01-01T00:00:00Z_telemetry.csv",
            timestamp_channel="timestamp",
            timestamp_type="iso_8601",
        )
        mock_get.return_value = _make_http_response(b"ts,val\n2026-01-01,1.0")

        destination_dataset = MagicMock()
        new_file = MagicMock()
        destination_dataset.add_from_io.return_value = new_file

        result = copy_file_to_dataset(source_file, destination_dataset)

        destination_dataset.add_from_io.assert_called_once()
        destination_dataset.add_journal_json.assert_not_called()
        call_kwargs = destination_dataset.add_from_io.call_args
        assert call_kwargs.kwargs["timestamp_column"] == "timestamp"
        assert call_kwargs.kwargs["timestamp_type"] == "iso_8601"
        assert result is new_file

    @patch("nominal.experimental.migration.utils.file_utils.requests.get")
    def test_missing_timestamp_metadata_raises(self, mock_get: MagicMock) -> None:
        """Non-journal files with no timestamp metadata raise ValueError."""
        source_file = _make_source_file(
            s3_key="2026-01-01T00:00:00Z_telemetry.csv",
            timestamp_channel=None,
            timestamp_type=None,
        )
        mock_get.return_value = _make_http_response(b"ts,val\n2026-01-01,1.0")

        destination_dataset = MagicMock()

        with pytest.raises(ValueError, match="missing timestamp information"):
            copy_file_to_dataset(source_file, destination_dataset)

    def test_non_s3_handle_raises(self) -> None:
        """Files without an S3 handle raise ValueError."""
        source_file = MagicMock()
        source_file.timestamp_channel = "ts"
        source_file.timestamp_type = "iso_8601"

        api_file = MagicMock()
        api_file.handle.s3 = None
        source_file._get_latest_api.return_value = api_file

        destination_dataset = MagicMock()

        with pytest.raises(ValueError, match="Unsupported file handle type"):
            copy_file_to_dataset(source_file, destination_dataset)
