"""Tests for nominal.experimental.migration.utils.file_utils."""

from __future__ import annotations

import os
import re
import sys
from unittest.mock import MagicMock, patch

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.experimental.migration.utils.file_utils import _resolve_destination_file_stem, copy_file_to_dataset

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


class TestResolveDestinationFileStem:
    """_resolve_destination_file_stem must URL-decode filenames extracted from S3 keys.

    Source S3 keys may contain percent-encoded characters (e.g. %20 for spaces).
    If these are not decoded before upload, quote_plus in multipart.py double-encodes
    them (%20 -> %2520), causing Azure SAS signature verification to fail with 403
    because Azure URL-decodes the blob path before computing its canonical string.
    """

    def test_decodes_percent_encoded_spaces(self) -> None:
        raw = "2026-06-02T16%3A25%3A51Z_DSC_Seq%20-%20Nominal%20Format.csv"
        assert _resolve_destination_file_stem(raw) == "DSC_Seq - Nominal Format"

    def test_decodes_percent_encoded_spaces_with_parens(self) -> None:
        # Regression: filename from the Azure tenant that triggered the 403 bug.
        raw = "2026-06-02T16%3A25%3A51Z_DSC_Seq%20-%20Nominal%20Format(PT-reduced).csv"
        assert _resolve_destination_file_stem(raw) == "DSC_Seq - Nominal Format(PT-reduced)"

    def test_plain_filename_unchanged(self) -> None:
        raw = "2026-06-02T16:25:51Z_telemetry.csv"
        assert _resolve_destination_file_stem(raw) == "telemetry"

    def test_no_timestamp_prefix(self) -> None:
        raw = "telemetry.csv"
        assert _resolve_destination_file_stem(raw) == "telemetry"

    def test_encoded_slash_does_not_break_stem(self) -> None:
        # %2F decoded to / would cause Path.stem to misinterpret the value as a
        # directory path, producing the wrong upload name.
        raw = "2026-06-02T16%3A25%3A51Z_folder%2Ftelemetry.csv"
        assert "/" not in _resolve_destination_file_stem(raw)


class TestUploadMultipartIoFilenameEncoding:
    """upload_multipart_io must produce filenames safe for Azure SAS uploads.

    Nominal's backend stores the filename literally as the Azure blob name. Any %XX
    sequence in the filename gets stored as literal characters, causing Azure SAS
    verification to fail with 403 because Azure URL-decodes the PUT path before
    computing its canonical string:

        blob name literal:   ..._DSC_Seq%28PT-reduced%29
        SAS canonical:       .../DSC_Seq%28PT-reduced%29
        PUT path decodes:    %28 -> (,  %29 -> )
        PUT canonical:       .../DSC_Seq(PT-reduced)   <- MISMATCH -> 403

    The fix replaces only characters illegal in cloud object names (/ and \\)
    and passes everything else through literally, avoiding %XX entirely.
    """

    def _capture_filename(self, name: str) -> str:
        """Run upload_multipart_io with a fake put_multipart_upload and return the filename it receives."""
        from io import BytesIO
        from unittest.mock import patch as mock_patch

        from nominal.core._utils.multipart import upload_multipart_io
        from nominal.core.filetype import FileTypes

        captured: dict[str, str] = {}

        def fake_put(auth_header, workspace_rid, f, filename, *args, **kwargs):
            captured["filename"] = filename
            return "s3://fake/path"

        with mock_patch("nominal.core._utils.multipart.put_multipart_upload", side_effect=fake_put):
            upload_multipart_io(
                auth_header="Bearer test",
                workspace_rid=None,
                f=BytesIO(b"data"),
                name=name,
                file_type=FileTypes.CSV,
                upload_client=MagicMock(),
            )

        return captured["filename"]

    def test_no_percent_sequences_for_parens(self) -> None:
        """( and ) must not become %28/%29 — regression for the Azure 403 bug."""
        assert not re.search(r"%[0-9A-Fa-f]{2}", self._capture_filename("DSC_Seq - Nominal Format(PT-reduced)"))

    def test_no_percent_sequences_for_brackets(self) -> None:
        """[ and ] must not become %5B/%5D."""
        assert not re.search(r"%[0-9A-Fa-f]{2}", self._capture_filename("data[1]"))

    def test_no_percent_sequences_for_special_chars(self) -> None:
        """Common filename chars like !, #, ' must not be percent-encoded."""
        assert not re.search(r"%[0-9A-Fa-f]{2}", self._capture_filename("run #3 - final!"))

    def test_spaces_preserved(self) -> None:
        """Spaces must be passed through literally, not converted to + or %20."""
        filename = self._capture_filename("my data file")
        assert " " in filename
        assert "+" not in filename

    def test_slash_replaced_with_underscore(self) -> None:
        """/ would create unexpected directory structure in blob storage."""
        assert "/" not in self._capture_filename("folder/file")

    def test_backslash_replaced_with_underscore(self) -> None:
        r"""\ would create unexpected directory structure on some backends."""
        assert "\\" not in self._capture_filename("folder\\file")

    def test_plain_alphanumeric_unchanged(self) -> None:
        """Simple names must be completely unaffected."""
        assert self._capture_filename("telemetry") == "telemetry.csv"

    @patch("nominal.experimental.migration.utils.file_utils.requests.get")
    def test_migration_pipeline_produces_no_percent_sequences(self, mock_get: MagicMock) -> None:
        """End-to-end: migration file with %20-encoded S3 key produces a clean upload filename."""
        source_file = _make_source_file(
            s3_key="2026-06-02T16%3A25%3A51Z_DSC_Seq%20-%20Nominal%20Format(PT-reduced).csv",
            timestamp_channel="timestamp",
            timestamp_type="iso_8601",
        )
        mock_get.return_value = _make_http_response(b"ts,val\n2026-01-01,1.0")

        destination_dataset = MagicMock()
        destination_dataset.add_from_io.return_value = MagicMock()

        copy_file_to_dataset(source_file, destination_dataset)

        file_name = destination_dataset.add_from_io.call_args.kwargs["file_name"]
        assert not re.search(r"%[0-9A-Fa-f]{2}", file_name), (
            f"file_name {file_name!r} has percent-encoded sequences that will cause Azure SAS 403 errors"
        )
