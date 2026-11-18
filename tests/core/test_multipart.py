"""Tests for nominal.core._utils.multipart upload-filename handling.

Regression lock for the encoding fix: the filename must reach object storage *un-encoded* (no
``quote_plus``), and unsafe filenames must be rejected before any upload begins.
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pytest
import requests
from nominal_api import ingest_api

from nominal.core._utils import multipart
from nominal.core._utils.multipart import (
    _complete_multipart_upload,
    _put_part,
)
from nominal.core.filetype import FileType, FileTypes
from nominal.exceptions import NominalMultipartUploadError, NominalMultipartUploadFailed


def _filename_passed_downstream(name: str, file_type: FileType = FileTypes.CSV) -> str:
    """Call upload_multipart_io and return the filename it forwards to put_multipart_upload."""
    with patch.object(multipart, "put_multipart_upload") as mock_put:
        mock_put.return_value = "s3://bucket/key"
        multipart.upload_multipart_io(
            "Bearer token",
            "ri.workspace",
            io.BytesIO(b"data"),
            name,
            file_type,
            MagicMock(),
        )
    # put_multipart_upload(auth, workspace, f, filename, mimetype, upload_client, ...)
    return mock_put.call_args.args[3]


@pytest.mark.parametrize(
    "name, expected",
    [
        ("plain", "plain.csv"),
        ("paren(reduced)", "paren(reduced).csv"),  # was quote_plus'd to paren%28reduced%29.csv
        ("with space", "with space.csv"),  # was with+space.csv
        ("unicode_résumé", "unicode_résumé.csv"),  # was %C3%A9-mangled
        ("amp&ersand", "amp&ersand.csv"),
        ("plus+plus", "plus+plus.csv"),
        ("hash#tag", "hash#tag.csv"),
        ("bracket[1]", "bracket[1].csv"),
        ("at@sign", "at@sign.csv"),
        ("equals=sign", "equals=sign.csv"),
        ("comma,comma", "comma,comma.csv"),
        ("semi;colon", "semi;colon.csv"),
        ("tilde~caret^", "tilde~caret^.csv"),
        ("dollar$sign", "dollar$sign.csv"),
    ],
)
def test_filename_forwarded_unencoded(name: str, expected: str) -> None:
    """Allowed filename characters reach the upload request without URL encoding."""
    assert _filename_passed_downstream(name) == expected


@pytest.mark.parametrize(
    ("name", "file_type", "expected"),
    [
        ("data.csv", FileTypes.CSV, "data.csv"),
        ("data.CSV", FileTypes.CSV, "data.CSV"),
        ("data", FileTypes.CSV_GZ, "data.csv.gz"),
        ("data.csv.gz", FileTypes.CSV_GZ, "data.csv.gz"),
        ("data.parquet", FileTypes.PARQUET, "data.parquet"),
        ("archive.parquet.tar.gz", FileTypes.PARQUET_TAR_GZ, "archive.parquet.tar.gz"),
        ("flight.bin", FileTypes.DATAFLASH, "flight.bin"),
        ("flight.mcap", FileTypes.MCAP, "flight.mcap"),
        ("clip.mp4", FileTypes.MP4, "clip.mp4"),
        ("clip.mkv", FileTypes.MKV, "clip.mkv"),
        ("logs.jsonl.gz", FileTypes.JOURNAL_JSONL_GZ, "logs.jsonl.gz"),
        ("records.avro.gz", FileTypes.AVRO_STREAM_GZ, "records.avro.gz"),
        ("report.pdf", FileTypes.BINARY, "report.pdf"),
        ("data.csv.csv", FileTypes.CSV, "data.csv.csv"),
        ("", FileTypes.CSV, ".csv"),
        ("flight.weird", FileTypes.DATAFLASH, "flight.weird"),
        ("data.csv", FileTypes.PARQUET, "data.csv"),
        ("readings.nominalraw", FileType(".nominalraw", "application/octet-stream"), "readings.nominalraw"),
        ("x.İ", FileType(".i\u0307", "application/octet-stream"), "x.İ"),
        ("x.i\u0307", FileType(".İ", "application/octet-stream"), "x.i\u0307"),
        ("x.İ.gz", FileType(".i\u0307.gz", "application/octet-stream"), "x.İ.gz"),
        ("data.csv", FileType("invalid/default", "application/octet-stream"), "data.csv"),
        (".config", FileTypes.CSV, ".config.csv"),
        (".config.local", FileTypes.CSV, ".config.local"),
        ("data.", FileTypes.CSV, "data."),
        ("data", FileType(".", "application/octet-stream"), "data."),
        ("data", FileType(".csv..gz", "application/octet-stream"), "data.csv..gz"),
    ],
)
def test_stream_uploads_default_only_missing_extensions(name, file_type, expected) -> None:
    """Existing suffixes are preserved verbatim; only names without one use the default extension."""
    assert _filename_passed_downstream(name, file_type) == expected


@pytest.mark.parametrize(
    ("name", "file_type", "expected"),
    [
        ("a.b.csv", FileTypes.CSV, "a.b.csv"),
        ("a.b.CSV", FileTypes.CSV, "a.b.CSV"),
        ("flight.bin.bin", FileTypes.DATAFLASH, "flight.bin.bin"),
        ("logs.jsonl.gz.jsonl.gz", FileTypes.JOURNAL_JSONL_GZ, "logs.jsonl.gz.jsonl.gz"),
        ("archive.parquet.tar.gz", FileTypes.PARQUET_TAR_GZ, "archive.parquet.tar.gz"),
        ("report.pdf", FileTypes.BINARY, "report.pdf"),
        ("flight..ulg", None, "flight..ulg"),
    ],
)
def test_path_uploads_use_the_actual_filename(tmp_path, name, file_type, expected) -> None:
    """Path names retain their case, repeated dots, and suffixes even with explicit or inferred types."""
    path = tmp_path / name
    path.write_bytes(b"data")
    with patch.object(multipart, "put_multipart_upload", return_value="s3://backend/opaque") as upload:
        multipart.upload_multipart_file("test", None, path, MagicMock(), file_type=file_type)
    assert upload.call_args.args[3] == expected


def test_unsafe_format_suffix_is_rejected_before_upload() -> None:
    """An explicit format cannot bypass filename safety by putting unsafe characters in its suffix."""
    with patch.object(multipart, "put_multipart_upload") as upload:
        with pytest.raises(ValueError, match="unsafe for storage"):
            multipart.upload_multipart_io(
                "test",
                None,
                io.BytesIO(b"data"),
                "safe",
                FileType(".bad/name", "application/octet-stream"),
                MagicMock(),
            )
    upload.assert_not_called()


@pytest.mark.parametrize("extension", ["csv", "csv.gz"])
def test_malformed_default_extension_is_rejected_before_upload(extension: str) -> None:
    """An applied default must be empty or start with a dot."""
    with patch.object(multipart, "put_multipart_upload") as upload:
        with pytest.raises(ValueError, match="extension"):
            multipart.upload_multipart_io(
                "test",
                None,
                io.BytesIO(b"data"),
                "traffic",
                FileType(extension, "application/octet-stream"),
                MagicMock(),
            )
    upload.assert_not_called()


@pytest.mark.parametrize("name", ["bad?name", "has/slash", "brace{x}", "quote'it", "pct%20"])
def test_unsafe_filename_rejected_before_upload(name: str) -> None:
    upload_client = MagicMock()
    with patch.object(multipart, "put_multipart_upload") as mock_put:
        with pytest.raises(ValueError, match="unsafe for storage"):
            multipart.upload_multipart_io(
                "Bearer token", "ri.workspace", io.BytesIO(b"data"), name, FileTypes.CSV, upload_client
            )
        mock_put.assert_not_called()  # no upload attempted for an unsafe name
    upload_client.initiate_multipart_upload.assert_not_called()


def _sign_response() -> MagicMock:
    r = MagicMock()
    r.url = "https://s3.example.com/signed"
    r.headers = {"x-amz-meta": "1"}
    return r


def test_complete_multipart_upload_builds_parts_from_etags_in_order() -> None:
    """Completion sends parts in ascending part-number order, which the storage provider requires."""
    client = MagicMock(spec=["complete_multipart_upload"])
    client.complete_multipart_upload.return_value = MagicMock(location="s3://bucket/key")

    location = _complete_multipart_upload(client, "auth", "key", "uid", {3: '"c"', 1: '"a"', 2: '"b"'})

    assert location == "s3://bucket/key"
    _, _, _, parts = client.complete_multipart_upload.call_args[0]
    assert [(p.part_number, p.etag) for p in parts] == [(1, '"a"'), (2, '"b"'), (3, '"c"')]


def test_complete_multipart_upload_raises_when_location_missing() -> None:
    """A completion response without a location is a failed upload, not a None return."""
    client = MagicMock(spec=["complete_multipart_upload"])
    client.complete_multipart_upload.return_value = MagicMock(location=None)

    with pytest.raises(NominalMultipartUploadFailed):
        _complete_multipart_upload(client, "auth", "key", "uid", {1: '"e"'})


@pytest.mark.parametrize("data", [b"", b"data"])
def test_put_multipart_upload_completes_single_part(data: bytes) -> None:
    """Even an empty stream sends part one and completes with the ETag listed by the server."""
    upload_client = MagicMock(
        spec=["initiate_multipart_upload", "sign_part", "list_parts", "complete_multipart_upload", "_verify"]
    )
    upload_client._verify = False
    upload_client.initiate_multipart_upload.return_value = MagicMock(key="key", upload_id="uid", bucket="uploads")
    upload_client.sign_part.return_value = _sign_response()
    upload_client.list_parts.return_value = [MagicMock(etag='"a"', part_number=1)]
    upload_client.complete_multipart_upload.return_value = MagicMock(location="s3://bucket/key")

    session = MagicMock(spec=["put", "close"])
    session.put.return_value = MagicMock(status_code=200)

    with patch.object(multipart, "create_multipart_request_session", return_value=session):
        location = multipart.put_multipart_upload(
            "Bearer token",
            "ri.workspace",
            io.BytesIO(data),
            "file.csv",
            "text/csv",
            upload_client,
            chunk_size=1_000_000,
            max_workers=1,
        )

    session.put.assert_called_once()
    assert session.put.call_args.kwargs["data"] == data
    assert upload_client.sign_part.call_args.args[2] == 1
    assert location == "s3://bucket/key"
    upload_client.list_parts.assert_called_once_with("Bearer token", "key", "uid", bucket="uploads")
    _, _, _, parts = upload_client.complete_multipart_upload.call_args[0]
    assert [(p.part_number, p.etag) for p in parts] == [(1, '"a"')]


@pytest.mark.parametrize("chunk_size", [0, -1])
def test_invalid_chunk_size_fails_before_upload(chunk_size: int) -> None:
    """Reject invalid sizes before consuming input or opening a remote upload."""
    client = MagicMock()
    stream = io.BytesIO(b"data")
    with pytest.raises(ValueError, match="chunk_size must be positive"):
        multipart.put_multipart_upload("auth", None, stream, "file.csv", "text/csv", client, chunk_size=chunk_size)
    assert stream.tell() == 0
    client.initiate_multipart_upload.assert_not_called()


def test_put_part_makes_exactly_one_request() -> None:
    """The PUT primitive makes exactly one request — retry policy belongs to its callers, not here."""
    session = MagicMock(spec=["put"])
    session.put.side_effect = requests.ConnectionError("boom")

    with pytest.raises(requests.ConnectionError):
        _put_part(session, _sign_response(), b"chunk", verify=False, timeout=9.0)
    assert session.put.call_count == 1  # exactly one PUT; retrying is the caller's job


def _upload_client_for_destination(bucket: str) -> MagicMock:
    client = MagicMock(
        spec=[
            "initiate_multipart_upload",
            "sign_part",
            "list_parts",
            "complete_multipart_upload",
            "abort_multipart_upload",
            "_verify",
        ]
    )
    client.initiate_multipart_upload.return_value = ingest_api.InitiateMultipartUploadResponse(
        upload_id="upload-1", key="object-key", bucket=bucket
    )
    client.sign_part.return_value = MagicMock(url="https://s3.example.com/signed", headers={})
    client.list_parts.return_value = [ingest_api.PartWithSize(part_number=1, etag="etag-1", size=4)]
    client.complete_multipart_upload.return_value = ingest_api.CompleteMultipartUploadResponse(
        location="s3://bucket/object-key"
    )
    client._verify = True
    return client


def test_file_store_upload_sends_its_destination_and_handle() -> None:
    """A File Store upload must name its destination, and every follow-up call must carry the handle."""
    client = _upload_client_for_destination("FILE_STORE")

    with patch.object(multipart, "create_multipart_request_session", return_value=MagicMock(spec=["put", "close"])):
        completed = multipart._put_multipart_upload_to(
            "Bearer token",
            "ri.workspace",
            io.BytesIO(b"data"),
            "run-001.csv",
            "text/csv",
            client,
            destination=ingest_api.UploadDestination.FILE_STORE,
        )

    assert completed.key == "object-key"
    assert client.initiate_multipart_upload.call_args.args[1].destination == ingest_api.UploadDestination.FILE_STORE
    assert client.sign_part.call_args.kwargs["bucket"] == "FILE_STORE"
    assert client.list_parts.call_args.kwargs["bucket"] == "FILE_STORE"
    assert client.complete_multipart_upload.call_args.kwargs["bucket"] == "FILE_STORE"


def test_short_reads_form_a_full_sized_replayable_put() -> None:
    """A pipe's short reads fill one numbered part and can replay after a failed PUT."""

    class Pipe(io.BytesIO):
        def read(self, size: int = -1) -> bytes:
            return super().read(min(size, 97))

    data = bytes(range(256)) * 1024
    source = Pipe(data + b"final part")
    part = multipart._read_part(source, len(data))
    request = requests.Request("PUT", "https://store.example/part1", data=part).prepare()
    assert request.headers["Content-Length"] == str(len(data))
    assert b"".join(request.body) == data
    assert b"".join(request.body) == data
    assert source.read(100) == b"final part"


@pytest.mark.parametrize("reads", [[None], [b"a", None]])
def test_nonblocking_stream_cannot_mistake_a_pause_for_eof(reads: list[bytes | None]) -> None:
    """A raw stream with no data available yet must fail instead of completing a truncated part."""
    stream = MagicMock(spec=io.RawIOBase)
    stream.read.side_effect = reads
    with pytest.raises(BlockingIOError):
        multipart._read_part(stream, 4)


def test_incomplete_server_listing_aborts_instead_of_completing_a_truncated_object() -> None:
    """A successful PUT for every chunk still requires a matching set of stored part numbers."""
    client = _upload_client_for_destination("uploads")
    with patch.object(multipart, "create_multipart_request_session", return_value=MagicMock(spec=["put", "close"])):
        with pytest.raises(NominalMultipartUploadError, match="expected parts"):
            multipart.put_multipart_upload(
                "auth", None, io.BytesIO(b"abcdefgh"), "test.csv", "text/csv", client, chunk_size=4
            )
    client.complete_multipart_upload.assert_not_called()
    client.abort_multipart_upload.assert_called_once()


def test_abort_carries_the_destination_bucket_when_the_upload_fails() -> None:
    """A failed File Store upload must abort against the handle initiate returned, or its parts are stranded."""
    client = _upload_client_for_destination("FILE_STORE")

    session = MagicMock(spec=["put", "close"])
    session.put.side_effect = requests.ConnectionError("boom")
    with patch.object(multipart, "create_multipart_request_session", return_value=session):
        with pytest.raises(NominalMultipartUploadFailed):
            multipart._put_multipart_upload_to(
                "Bearer token",
                "ri.workspace",
                io.BytesIO(b"data"),
                "run-001.csv",
                "text/csv",
                client,
                destination=ingest_api.UploadDestination.FILE_STORE,
            )

    assert client.abort_multipart_upload.call_args.kwargs["bucket"] == "FILE_STORE"
    assert session.put.call_count == 3


def test_failed_put_and_abort_report_safe_storage_diagnostics(caplog: pytest.LogCaptureFixture) -> None:
    """A failed abort preserves the PUT error and logs no signed URL from either failure."""
    signed_url = "https://storage.example/file?signature=sensitive-query"
    response = requests.Response()
    response.status_code = 403
    response.url = signed_url
    response.headers.update({"x-amz-request-id": "request-id", "Location": signed_url})
    client = _upload_client_for_destination("uploads")
    client.abort_multipart_upload.side_effect = requests.ConnectionError(signed_url)
    with multipart.StreamUploader(client, "auth", None, max_workers=1, max_buffer_bytes=4, max_part_retries=1) as up:
        up.session.put = MagicMock(return_value=response)
        with pytest.raises(NominalMultipartUploadFailed) as failed:
            up.upload_stream(io.BytesIO(b"data"), "file.csv", "text/csv", chunk_size=4)

    attempt = failed.value.exceptions[0]
    assert "HTTP 403" in str(attempt)
    assert "request-id" in str(attempt)
    assert signed_url not in str(attempt)
    assert isinstance(attempt.__cause__, requests.HTTPError)
    assert attempt.__cause__.response is response
    assert "ConnectionError" in caplog.text
    assert signed_url not in caplog.text
