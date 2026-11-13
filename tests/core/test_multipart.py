"""Tests for nominal.core._utils.multipart upload-filename handling.

Regression lock for the encoding fix: the filename must reach object storage *un-encoded* (no
``quote_plus``), and unsafe filenames must be rejected before any upload begins.
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pytest
import requests

from nominal.core._utils import multipart
from nominal.core._utils.multipart import (
    _complete_multipart_upload,
    _put_part,
    _sign_and_put_part,
)
from nominal.core.filetype import FileType, FileTypes
from nominal.exceptions import NominalMultipartUploadFailed
from nominal.protos.uploads.v1 import uploads_pb2


def _filename_passed_downstream(name: str, file_type: FileType = FileTypes.CSV) -> str:
    """Call upload_multipart_io and return the filename it forwards to put_multipart_upload."""
    with patch.object(multipart, "put_multipart_upload") as mock_put:
        mock_put.return_value = "s3://bucket/key"
        multipart.upload_multipart_io(
            "ri.workspace",
            io.BytesIO(b"data"),
            name,
            file_type,
            MagicMock(),
        )
    # put_multipart_upload(auth, workspace, f, filename, mimetype, upload_client, ...)
    return mock_put.call_args.args[2]


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
        multipart.upload_multipart_file(None, path, MagicMock(), file_type=file_type)
    assert upload.call_args.args[2] == expected


def test_unsafe_format_suffix_is_rejected_before_upload() -> None:
    """An explicit format cannot bypass filename safety by putting unsafe characters in its suffix."""
    with patch.object(multipart, "put_multipart_upload") as upload:
        with pytest.raises(ValueError, match="unsafe for storage"):
            multipart.upload_multipart_io(
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
            multipart.upload_multipart_io("ri.workspace", io.BytesIO(b"data"), name, FileTypes.CSV, upload_client)
        mock_put.assert_not_called()  # no upload attempted for an unsafe name
    upload_client.CreateMultipartUpload.assert_not_called()


def _sign_response() -> uploads_pb2.UploadPartUrl:
    return uploads_pb2.UploadPartUrl(url="https://s3.example.com/signed")


def test_sign_and_put_part_raises_after_retries() -> None:
    """The legacy retry wrapper spends exactly its retry budget, then fails with the public error."""
    client = MagicMock(spec=["CreateUploadPartUrl"])
    client.CreateUploadPartUrl.return_value = uploads_pb2.CreateUploadPartUrlResponse(upload_part_url=_sign_response())
    session = MagicMock(spec=["put"])
    session.put.side_effect = requests.ConnectionError("boom")

    with pytest.raises(NominalMultipartUploadFailed):
        _sign_and_put_part(client, session, "key", "uid", 3, b"chunk", num_retries=2)

    assert session.put.call_count == 2


def test_complete_multipart_upload_builds_parts_from_etags_in_order() -> None:
    """Completion sends parts in ascending part-number order, which the storage provider requires."""
    client = MagicMock(spec=["CompleteMultipartUpload"])
    client.CompleteMultipartUpload.return_value = uploads_pb2.CompleteMultipartUploadResponse(
        object_path="s3://bucket/key"
    )

    location = _complete_multipart_upload(client, "uid", {3: '"c"', 1: '"a"', 2: '"b"'})

    assert location == "s3://bucket/key"
    parts = client.CompleteMultipartUpload.call_args.args[0].parts
    assert [(p.part_number, p.etag) for p in parts] == [(1, '"a"'), (2, '"b"'), (3, '"c"')]


def test_complete_multipart_upload_raises_when_location_missing() -> None:
    """A completion response without a location is a failed upload, not a None return."""
    client = MagicMock(spec=["CompleteMultipartUpload"])
    client.CompleteMultipartUpload.return_value = uploads_pb2.CompleteMultipartUploadResponse()

    with pytest.raises(NominalMultipartUploadFailed):
        _complete_multipart_upload(client, "uid", {1: '"e"'})


def test_put_multipart_upload_completes_via_list_parts() -> None:
    """Regression lock: `put_multipart_upload` (the legacy path) never collects per-part ETags of
    its own, so its completion must still ask the server for them via list_parts -- not the
    ETag-based primitive the newer MultipartUploader uses.
    """
    upload_client = MagicMock(
        spec=["CreateMultipartUpload", "CreateUploadPartUrl", "ListUploadParts", "CompleteMultipartUpload"]
    )
    upload_client.CreateMultipartUpload.return_value = uploads_pb2.CreateMultipartUploadResponse(
        multipart_upload=uploads_pb2.MultipartUpload(object_key="key", upload_id="uid")
    )
    upload_client.CreateUploadPartUrl.return_value = uploads_pb2.CreateUploadPartUrlResponse(
        upload_part_url=_sign_response()
    )
    upload_client.ListUploadParts.return_value = uploads_pb2.ListUploadPartsResponse(
        parts=[uploads_pb2.UploadPart(etag='"a"', part_number=1)]
    )
    upload_client.CompleteMultipartUpload.return_value = uploads_pb2.CompleteMultipartUploadResponse(
        object_path="s3://bucket/key"
    )

    session = MagicMock(spec=["put", "close"])
    session.put.return_value = MagicMock(status_code=200)

    with patch.object(multipart, "create_multipart_request_session", return_value=session):
        location = multipart.put_multipart_upload(
            "ri.workspace",
            io.BytesIO(b"data"),
            "file.csv",
            "text/csv",
            upload_client,
            chunk_size=1_000_000,
            max_workers=1,
            verify="/custom/ca.pem",
        )

    assert location == "s3://bucket/key"
    assert session.put.call_args.kwargs["verify"] == "/custom/ca.pem"
    upload_client.ListUploadParts.assert_called_once_with(
        uploads_pb2.ListUploadPartsRequest(upload_id="uid", page_size=1000)
    )
    parts = upload_client.CompleteMultipartUpload.call_args.args[0].parts
    assert [(p.part_number, p.etag) for p in parts] == [(1, '"a"')]


def test_put_part_makes_exactly_one_request() -> None:
    """The PUT primitive makes exactly one request — retry policy belongs to its callers, not here."""
    session = MagicMock(spec=["put"])
    session.put.side_effect = requests.ConnectionError("boom")

    with pytest.raises(requests.ConnectionError):
        _put_part(session, _sign_response(), b"chunk", verify=False, timeout=9.0)
    assert session.put.call_count == 1  # exactly one PUT; retrying is the caller's job


def test_grpc_upload_completion_collects_every_page() -> None:
    """Completion includes all uploaded parts, including those beyond the first page."""
    client = MagicMock()
    client.CreateMultipartUpload.return_value = uploads_pb2.CreateMultipartUploadResponse(
        multipart_upload=uploads_pb2.MultipartUpload(object_key="key", upload_id="opaque-id")
    )
    client.ListUploadParts.side_effect = [
        uploads_pb2.ListUploadPartsResponse(
            parts=[uploads_pb2.UploadPart(part_number=1, etag="first")], next_page_token="next"
        ),
        uploads_pb2.ListUploadPartsResponse(parts=[uploads_pb2.UploadPart(part_number=2, etag="last")]),
    ]
    client.CompleteMultipartUpload.return_value = uploads_pb2.CompleteMultipartUploadResponse(object_path="s3://file")
    with patch.object(multipart, "_sign_and_upload_part_job", return_value=MagicMock()):
        # Avoid queued work: this test isolates the recovery-list/completion boundary.
        result = multipart.put_multipart_upload(None, io.BytesIO(), "file", "text/csv", client)
    assert result == "s3://file"
    requests = [call.args[0] for call in client.ListUploadParts.call_args_list]
    assert [(r.upload_id, r.page_token) for r in requests] == [("opaque-id", ""), ("opaque-id", "next")]
    request = client.CompleteMultipartUpload.call_args.args[0]
    assert request.upload_id == "opaque-id"
    assert [(p.part_number, p.etag) for p in request.parts] == [(1, "first"), (2, "last")]
