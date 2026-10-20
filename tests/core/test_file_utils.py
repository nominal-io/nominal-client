"""Tests for nominal.experimental.migration.utils.file_utils."""

from __future__ import annotations

from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest
import requests
from urllib3.response import HTTPResponse

from nominal.core import Dataset, FileType, FileTypes
from nominal.experimental.migration.utils.file_utils import copy_file_to_dataset

SOURCE_URI = "https://source-bucket.s3.example.com/opaque%2Fkey+name?signature=preserved"


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
    uri_response.uri = SOURCE_URI
    source_file._clients.catalog.get_dataset_file_uri.return_value = uri_response
    source_file._clients.auth_header = "Bearer token"

    return source_file


def _make_http_response(content: bytes = b"data") -> requests.Response:
    """Build an HTTP response with a non-seekable body to exercise stream identity and cleanup."""
    response = requests.Response()
    response.status_code = 200
    response.raw = HTTPResponse(body=BytesIO(content), preload_content=False)
    return response


@pytest.fixture
def destination_dataset() -> MagicMock:
    """Mock the SDK boundary used by migration, leaving upload internals to Dataset tests."""
    destination = MagicMock(spec=Dataset)
    destination.name = "Destination"
    destination.rid = "destination-dataset"
    destination._clients = MagicMock(workspace_rid="destination-workspace")
    return destination


@pytest.mark.parametrize(
    ("source_name", "file_type", "expected_name"),
    [
        ("system.jsonl", FileTypes.JOURNAL_JSONL, "system.jsonl"),
        ("system.jsonl.gz", FileTypes.JOURNAL_JSONL_GZ, "system.jsonl.gz"),
        ("my+logs%28raw%29.JSONL.GZ", FileTypes.JOURNAL_JSONL_GZ, "my logs(raw).JSONL.GZ"),
        ("system.jsonl.jsonl.gz", FileTypes.JOURNAL_JSONL_GZ, "system.jsonl.jsonl.gz"),
    ],
)
def test_journal_migration_preserves_known_type_and_compound_suffix(
    destination_dataset, source_name, file_type, expected_name
) -> None:
    """Journal migration passes the raw response and known format to the SDK, preserving every suffix and polling."""
    source_file = _make_source_file(f"source/2026-01-01T00:00:00Z_{source_name}", None, None)
    response = _make_http_response()

    with patch("nominal.experimental.migration.utils.file_utils.requests.get", return_value=response) as get:
        result = copy_file_to_dataset(source_file, destination_dataset)

    get.assert_called_once_with(SOURCE_URI, stream=True)
    destination_dataset.add_journal_json_from_io.assert_called_once_with(
        response.raw, file_type=file_type, file_name=expected_name
    )
    assert result is destination_dataset.add_journal_json_from_io.return_value
    result.poll_until_ingestion_completed.assert_called_once_with()
    assert response.raw.closed


@pytest.mark.parametrize(
    ("source_key", "expected_name", "file_type"),
    [
        ("source/2026-01-01T00:00:00Z_data.csv", "data.csv", FileTypes.CSV),
        ("source/2026-01-01T00:00:00Z_data.csv.gz", "data.csv.gz", FileTypes.CSV_GZ),
        ("source/data.PARQUET", "data.PARQUET", FileTypes.PARQUET),
        ("source/data.parquet.gz", "data.parquet.gz", FileTypes.PARQUET_GZ),
        ("source/data.parquet.tar", "data.parquet.tar", FileTypes.PARQUET_TAR),
        ("source/2026-01-01T00:00:00Z_data.v2.PARQUET.TAR.GZ", "data.v2.PARQUET.TAR.GZ", FileTypes.PARQUET_TAR_GZ),
        ("source/data.parquet.zip", "data.parquet.zip", FileTypes.PARQUET_ZIP),
        ("source/2026-01-01T00:00:00Z_telemetry.csv.csv", "telemetry.csv.csv", FileTypes.CSV),
        ("source/data.csv.csv.gz", "data.csv.csv.gz", FileTypes.CSV_GZ),
        ("source/data.v2.nonsense", "data.v2.nonsense", FileType(".v2.nonsense", "application/octet-stream")),
        (
            "source/data.nonsenseZ_suffix",
            "data.nonsenseZ_suffix",
            FileType(".nonsensez_suffix", "application/octet-stream"),
        ),
        ("source/2026-01-01T00:00:00Z_paren%28reduced%29.csv", "paren(reduced).csv", FileTypes.CSV),
        ("source/2026-01-01T00:00:00Z_my+file.csv", "my file.csv", FileTypes.CSV),
        ("source/2026-01-01T00:00:00Z_weird%7Bname%7D.csv", "weird_name_.csv", FileTypes.CSV),
    ],
)
def test_tabular_migration_preserves_complete_filename_and_ingest_metadata(
    destination_dataset: MagicMock, source_key: str, expected_name: str, file_type: FileType
) -> None:
    """Tabular migration preserves the raw stream, inferred type, filename, and timestamp/tag metadata."""
    source_file = _make_source_file(source_key, "timestamp", "epoch_seconds")
    source_file.tag_columns = {"vehicle": "vehicle_id"}
    source_file.file_tags = {"campaign": "test"}
    response = _make_http_response()

    with patch("nominal.experimental.migration.utils.file_utils.requests.get", return_value=response) as get:
        result = copy_file_to_dataset(source_file, destination_dataset)

    get.assert_called_once_with(SOURCE_URI, stream=True)
    destination_dataset.add_from_io.assert_called_once_with(
        dataset=response.raw,
        timestamp_column="timestamp",
        timestamp_type="epoch_seconds",
        file_type=file_type,
        file_name=expected_name,
        tag_columns={"vehicle": "vehicle_id"},
        tags={"campaign": "test"},
    )
    assert result is destination_dataset.add_from_io.return_value
    result.poll_until_ingestion_completed.assert_called_once_with()
    assert response.raw.closed


def test_journal_upload_failure_closes_source_response(destination_dataset: MagicMock) -> None:
    """A failed journal upload closes its source HTTP response and propagates the failure."""
    source_file = _make_source_file("system.jsonl.gz", None, None)
    response = _make_http_response()
    destination_dataset.add_journal_json_from_io.side_effect = RuntimeError("upload failed")

    with patch("nominal.experimental.migration.utils.file_utils.requests.get", return_value=response):
        with pytest.raises(RuntimeError, match="upload failed"):
            copy_file_to_dataset(source_file, destination_dataset)

    assert response.raw.closed


class TestCopyFileToDataset:
    @patch("nominal.experimental.migration.utils.file_utils.requests.get")
    def test_missing_timestamp_metadata_raises(self, mock_get: MagicMock) -> None:
        """Tabular files without timestamp metadata raise ValueError."""
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
