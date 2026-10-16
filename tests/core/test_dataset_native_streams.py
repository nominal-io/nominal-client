"""Core native-stream ingestion contracts, with only storage/service I/O mocked."""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pytest
from nominal_api import api, ingest_api, scout_catalog
from urllib3.response import HTTPResponse

from nominal import ts
from nominal.core import Dataset, FileTypes
from nominal.core.dataset_file import IngestStatus

LOCATION = "s3://backend-selected-bucket/workspace/opaque%2Fkey+name"


@pytest.fixture
def dataset() -> Dataset:
    clients = MagicMock()
    clients.auth_header = "Bearer test"
    clients.resolve_default_workspace_rid.return_value = "workspace"
    clients.header_provider = lambda: {"x-test": "header"}
    clients.ingest.ingest.return_value = ingest_api.IngestResponse(
        details=ingest_api.IngestDetails(
            dataset=ingest_api.IngestDatasetFileDetails(dataset_rid="dataset", dataset_file_id="native-file")
        )
    )
    clients.catalog.get_dataset_file.return_value = scout_catalog.DatasetFile(
        dataset_rid="dataset",
        id="native-file",
        name="native-file",
        uploaded_at="2026-01-01T00:00:00Z",
        handle=scout_catalog.Handle(s3=scout_catalog.S3Handle(bucket="backend-selected-bucket", key="native-file")),
        ingest_status=api.IngestStatusV2(in_progress=api.InProgressResult()),
    )
    return Dataset(
        rid="dataset",
        name="default-name",
        description=None,
        bounds=None,
        properties={},
        labels=[],
        is_archived=False,
        _clients=clients,
    )


@pytest.mark.parametrize(
    ("method", "kwargs", "expected_name", "expected_arm"),
    [
        (Dataset.add_ardupilot_dataflash_from_io, {"tags": {"vehicle": "v1"}}, "default-name.bin", "dataflash"),
        (
            Dataset.add_ardupilot_dataflash_from_io,
            {"file_name": "flight", "tags": {"vehicle": "v1"}},
            "flight.bin",
            "dataflash",
        ),
        (
            Dataset.add_ardupilot_dataflash_from_io,
            {"file_name": "flight.bin", "tags": {"vehicle": "v1"}},
            "flight.bin",
            "dataflash",
        ),
        (
            Dataset.add_ardupilot_dataflash_from_io,
            {"file_name": "flight.BIN", "tags": {"vehicle": "v1"}},
            "flight.bin",
            "dataflash",
        ),
        (
            Dataset.add_journal_json_from_io,
            {"file_type": FileTypes.JOURNAL_JSONL},
            "default-name.jsonl",
            "journal_json",
        ),
        (
            Dataset.add_journal_json_from_io,
            {"file_type": FileTypes.JOURNAL_JSONL_GZ, "file_name": "logs"},
            "logs.jsonl.gz",
            "journal_json",
        ),
    ],
)
def test_native_streams_preserve_storage_location_and_caller_owned_stream(
    dataset, method, kwargs, expected_name, expected_arm
) -> None:
    """Non-seekable binary streams use existing upload routing and return pending files without closing input."""
    stream = HTTPResponse(body=io.BytesIO(b"native-bytes"), preload_content=False)

    def upload(auth, workspace, source, filename, mimetype, client, **options):
        assert auth == dataset._clients.auth_header
        assert workspace == "workspace"
        assert client is dataset._clients.upload
        assert options["header_provider"] is dataset._clients.header_provider
        assert source is stream and not source.seekable()
        assert source.read() == b"native-bytes"
        assert filename == expected_name
        return LOCATION

    with patch("nominal.core._utils.multipart.put_multipart_upload", side_effect=upload):
        result = method(dataset, stream, **kwargs)
    _, request = dataset._clients.ingest.ingest.call_args.args
    options = request.options.dataflash if expected_arm == "dataflash" else request.options.journal_json
    assert options.source.s3.path == LOCATION
    assert options.target.existing.dataset_rid == dataset.rid
    assert result.id == "native-file"
    assert result.ingest_status is IngestStatus.IN_PROGRESS
    assert not stream.closed
    if expected_arm == "dataflash":
        assert options.additional_file_tags == {"vehicle": "v1"}
    else:
        assert options.timestamp_metadata is None
        assert options.channel is None
    stream.close()


@pytest.mark.parametrize(
    "file_type",
    [FileTypes.JOURNAL_JSONL_GZ, tuple(FileTypes.JOURNAL_JSONL_GZ)],
    ids=["file-type", "extension-mimetype"],
)
def test_journal_stream_keeps_known_type_channel_and_timestamp_options(dataset, file_type) -> None:
    """An explicit journal format preserves the upload suffix, custom epoch field/unit, and channel."""
    stream = io.BytesIO(b"prefixjournal-bytes")
    stream.seek(len(b"prefix"))
    with patch("nominal.core._utils.multipart.put_multipart_upload", return_value=LOCATION) as upload:
        dataset.add_journal_json_from_io(
            stream,
            file_type=file_type,
            file_name="logs",
            channel="events",
            timestamp_column="source_ts",
            timestamp_type="epoch_seconds",
        )
    assert upload.call_args.args[2].read() == b"journal-bytes"
    assert upload.call_args.args[3] == "logs.jsonl.gz"
    _, request = dataset._clients.ingest.ingest.call_args.args
    options = request.options.journal_json
    assert options.channel == "events"
    assert options.timestamp_metadata.field_name == "source_ts"
    assert options.timestamp_metadata.epoch_of_time_unit.time_unit == api.TimeUnit.SECONDS


@pytest.mark.parametrize(
    ("file_name", "kwargs", "expected_name"),
    [
        ("logs.jsonl", {}, "logs.jsonl"),
        ("logs.jsonl.gz", {}, "logs.jsonl.gz"),
        ("logs.JSONL.GZ", {}, "logs.jsonl.gz"),
        ("logs.jsonl.gz", {"file_type": FileTypes.JOURNAL_JSONL_GZ}, "logs.jsonl.gz"),
    ],
)
def test_journal_stream_preserves_filename_with_inferred_or_explicit_type(
    dataset, file_name, kwargs, expected_name
) -> None:
    """Journal filename suffixes are applied once with either inferred or explicit format metadata."""
    stream = HTTPResponse(body=io.BytesIO(b"journal-bytes"), preload_content=False)
    with patch("nominal.core._utils.multipart.put_multipart_upload", return_value=LOCATION) as upload:
        result = dataset.add_journal_json_from_io(stream, file_name=file_name, **kwargs)
    assert upload.call_args.args[2] is stream
    assert upload.call_args.args[3] == expected_name
    assert stream.read() == b"journal-bytes"
    assert not stream.closed
    assert result.id == "native-file"
    stream.close()


def test_journal_stream_requires_file_name_or_type_for_named_sources(dataset, tmp_path) -> None:
    """Stream metadata does not substitute for an explicit filename or format."""
    path = tmp_path / "source.jsonl.gz"
    path.write_bytes(b"prefixjournal-bytes")
    with path.open("rb") as stream:
        stream.seek(len(b"prefix"))
        with patch("nominal.core._utils.multipart.put_multipart_upload") as upload:
            with pytest.raises(ValueError, match="file_type or file_name"):
                dataset.add_journal_json_from_io(stream, file_type=None)
        upload.assert_not_called()
        assert stream.read() == b"journal-bytes"
        assert not stream.closed


@pytest.mark.parametrize(
    ("file_name", "message"),
    [(None, "file_type"), ("logs", "journal"), ("logs.tar.gz", "journal")],
)
def test_journal_stream_requires_a_known_format_before_upload(dataset, file_name, message) -> None:
    """Anonymous inputs and unsupported filename extensions fail locally instead of assuming JSONL."""
    stream = io.BytesIO(b"prefixjournal-bytes")
    stream.seek(len(b"prefix"))
    with patch("nominal.core._utils.multipart.put_multipart_upload") as upload:
        with pytest.raises(ValueError, match=message):
            dataset.add_journal_json_from_io(stream, file_name=file_name)
    upload.assert_not_called()
    assert stream.read() == b"journal-bytes"
    assert not stream.closed


@pytest.mark.parametrize("method", [Dataset.add_ardupilot_dataflash_from_io, Dataset.add_journal_json_from_io])
def test_native_streams_reject_text_before_upload(dataset, method) -> None:
    """Text streams are rejected locally, before the binary upload reader can block."""
    with patch("nominal.core._utils.multipart.put_multipart_upload") as upload:
        with pytest.raises(TypeError, match="binary"):
            method(dataset, io.StringIO("text"))
    upload.assert_not_called()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"file_type": FileTypes.CSV}, "journal"),
        ({"timestamp_column": "ts"}, "pass both"),
        ({"timestamp_type": "epoch_seconds"}, "pass both"),
        ({"timestamp_column": "ts", "timestamp_type": ts.Relative("seconds", start=0)}, "epoch"),
        ({"timestamp_column": "ts", "timestamp_type": "iso_8601"}, "epoch"),
    ],
)
def test_journal_stream_invalid_options_fail_before_upload(dataset, kwargs, message) -> None:
    """Unsupported journal formats or timestamp contracts fail before spending bytes on upload."""
    with patch("nominal.core._utils.multipart.put_multipart_upload") as upload:
        with pytest.raises(ValueError, match=message):
            dataset.add_journal_json_from_io(io.BytesIO(b"journal"), file_name="journal.jsonl", **kwargs)
    upload.assert_not_called()


@pytest.mark.parametrize(
    ("method", "kwargs"),
    [
        (Dataset.add_ardupilot_dataflash_from_io, {}),
        (Dataset.add_journal_json_from_io, {"file_type": FileTypes.JOURNAL_JSONL}),
    ],
)
def test_upload_failure_leaves_caller_stream_open(dataset, method, kwargs) -> None:
    """Failed native stream uploads leave input ownership with the caller."""
    stream = io.BytesIO(b"native")
    with patch("nominal.core._utils.multipart.put_multipart_upload", side_effect=RuntimeError("upload failed")):
        with pytest.raises(RuntimeError, match="upload failed"):
            method(dataset, stream, **kwargs)
    assert not stream.closed


@pytest.mark.parametrize(
    ("method", "filename", "kwargs", "expected_arm"),
    [
        (Dataset.add_ardupilot_dataflash, "flight.bin", {"tags": {"vehicle": "v1"}}, "dataflash"),
        (Dataset.add_ardupilot_dataflash, "flight.bin.bin", {"tags": {"vehicle": "v1"}}, "dataflash"),
        (
            Dataset.add_journal_json,
            "logs.jsonl.gz",
            {"channel": "events", "timestamp_column": "ts", "timestamp_type": "epoch_seconds"},
            "journal_json",
        ),
        (
            Dataset.add_journal_json,
            "logs.jsonl.gz.jsonl.gz",
            {"channel": "events", "timestamp_column": "ts", "timestamp_type": "epoch_seconds"},
            "journal_json",
        ),
    ],
)
def test_existing_path_methods_preserve_filename_and_own_file_handle(
    dataset, tmp_path, method, filename, kwargs, expected_arm
) -> None:
    """Path methods preserve the upload name, native options, and location while owning their file handle."""
    path = tmp_path / filename
    path.write_bytes(b"native")
    with patch("nominal.core._utils.multipart.put_multipart_upload", return_value=LOCATION) as upload:
        result = method(dataset, path, **kwargs)
    assert upload.call_args.args[2].closed
    assert upload.call_args.args[3] == filename
    assert result.id == "native-file"
    _, request = dataset._clients.ingest.ingest.call_args.args
    options = request.options.dataflash if expected_arm == "dataflash" else request.options.journal_json
    assert options.source.s3.path == LOCATION
    if expected_arm == "dataflash":
        assert options.additional_file_tags == {"vehicle": "v1"}
    else:
        assert options.channel == "events"
        assert options.timestamp_metadata.field_name == "ts"
        assert options.timestamp_metadata.epoch_of_time_unit.time_unit == api.TimeUnit.SECONDS


def test_journal_path_without_timestamp_arguments_uses_backend_defaults(dataset, tmp_path) -> None:
    """Omitting both timestamp arguments submits no override and returns a pending file."""
    path = tmp_path / "logs.jsonl"
    path.write_text('{"__REALTIME_TIMESTAMP": 1, "MESSAGE": "hello"}')
    with patch("nominal.core._utils.multipart.put_multipart_upload", return_value=LOCATION):
        result = dataset.add_journal_json(path)
    _, request = dataset._clients.ingest.ingest.call_args.args
    assert request.options.journal_json.timestamp_metadata is None
    assert result.id == "native-file"
    assert result.ingest_status is IngestStatus.IN_PROGRESS


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"timestamp_column": "ts"}, "pass both"),
        ({"timestamp_type": "epoch_seconds"}, "pass both"),
        ({"timestamp_column": "ts", "timestamp_type": ts.Relative("seconds", start=0)}, "epoch"),
        ({"timestamp_column": "ts", "timestamp_type": "iso_8601"}, "epoch"),
    ],
)
def test_journal_path_rejects_invalid_timestamps_before_path_errors(dataset, tmp_path, kwargs, message) -> None:
    """Invalid timestamp options retain priority over an unsupported or missing file path."""
    with pytest.raises(ValueError, match=message):
        dataset.add_journal_json(tmp_path / "missing.unsupported", **kwargs)


@pytest.mark.parametrize(
    ("method", "filename"),
    [(Dataset.add_ardupilot_dataflash, "flight.bin"), (Dataset.add_journal_json, "logs.jsonl")],
)
def test_path_ingestion_failure_closes_owned_file(dataset, tmp_path, method, filename) -> None:
    """A submission failure closes the path method's input while stream methods leave ownership with callers."""
    path = tmp_path / filename
    path.write_bytes(b"native")
    dataset._clients.ingest.ingest.side_effect = RuntimeError("ingest failed")
    with patch("nominal.core._utils.multipart.put_multipart_upload", return_value=LOCATION) as upload:
        with pytest.raises(RuntimeError, match="ingest failed"):
            method(dataset, path)
    assert upload.call_args.args[2].closed
