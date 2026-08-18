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
    _sign_and_put_part,
)
from nominal.core.exceptions import NominalMultipartUploadFailed
from nominal.core.filetype import FileTypes


def _filename_passed_downstream(name: str) -> str:
    """Call upload_multipart_io and return the filename it forwards to put_multipart_upload."""
    with patch.object(multipart, "put_multipart_upload") as mock_put:
        mock_put.return_value = "s3://bucket/key"
        multipart.upload_multipart_io(
            "Bearer token",
            "ri.workspace",
            io.BytesIO(b"data"),
            name,
            FileTypes.CSV,
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
    ],
)
def test_filename_forwarded_unencoded(name: str, expected: str) -> None:
    assert _filename_passed_downstream(name) == expected


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


def test_sign_and_put_part_raises_after_retries() -> None:
    """The legacy retry wrapper spends exactly its retry budget, then fails with the public error."""
    client = MagicMock(spec=["sign_part", "_verify"])
    client._verify = True
    client.sign_part.return_value = _sign_response()
    session = MagicMock(spec=["put"])
    session.put.side_effect = requests.ConnectionError("boom")

    with pytest.raises(NominalMultipartUploadFailed):
        _sign_and_put_part(client, session, "auth", "key", "uid", 3, b"chunk", num_retries=2)

    assert session.put.call_count == 2


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


def test_put_multipart_upload_completes_via_list_parts() -> None:
    """Regression lock: `put_multipart_upload` (the legacy path) never collects per-part ETags of
    its own, so its completion must still ask the server for them via list_parts -- not the
    ETag-based primitive the newer MultipartUploader uses.

    Also pins that the destination handle `initiate` returns is passed back to list_parts, since
    every follow-up call must carry it regardless of whether a non-default destination was asked for.
    """
    upload_client = MagicMock(
        spec=["initiate_multipart_upload", "sign_part", "list_parts", "complete_multipart_upload", "_verify"]
    )
    upload_client._verify = False
    upload_client.initiate_multipart_upload.return_value = MagicMock(
        key="key", upload_id="uid", bucket="uploads-bucket"
    )
    upload_client.sign_part.return_value = _sign_response()
    upload_client.list_parts.return_value = [MagicMock(etag='"a"', part_number=1)]
    upload_client.complete_multipart_upload.return_value = MagicMock(location="s3://bucket/key")

    session = MagicMock(spec=["put", "close"])
    session.put.return_value = MagicMock(status_code=200)

    with patch.object(multipart, "create_multipart_request_session", return_value=session):
        location = multipart.put_multipart_upload(
            "Bearer token",
            "ri.workspace",
            io.BytesIO(b"data"),
            "file.csv",
            "text/csv",
            upload_client,
            chunk_size=1_000_000,
            max_workers=1,
        )

    assert location == "s3://bucket/key"
    upload_client.list_parts.assert_called_once_with("Bearer token", "key", "uid", bucket="uploads-bucket")
    args, kwargs = upload_client.complete_multipart_upload.call_args
    _, _, _, parts = args
    assert [(p.part_number, p.etag) for p in parts] == [(1, '"a"')]
    assert kwargs["bucket"] == "uploads-bucket"


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


def _no_op_session() -> MagicMock:
    """A session double for tests that patch `_put_part` directly and never touch the socket."""
    return MagicMock(spec=["put", "close"])


def test_file_store_upload_sends_its_destination_and_handle() -> None:
    """A File Store upload must name its destination, and every follow-up call must carry the handle."""
    client = _upload_client_for_destination("FILE_STORE")

    with (
        patch.object(multipart, "create_multipart_request_session", return_value=_no_op_session()),
        patch.object(multipart, "_put_part", return_value=MagicMock(headers={"ETag": "etag-1"})),
    ):
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
    assert completed.bucket == "FILE_STORE"
    assert client.initiate_multipart_upload.call_args.args[1].destination == ingest_api.UploadDestination.FILE_STORE
    assert client.sign_part.call_args.kwargs["bucket"] == "FILE_STORE"
    assert client.list_parts.call_args.kwargs["bucket"] == "FILE_STORE"
    assert client.complete_multipart_upload.call_args.kwargs["bucket"] == "FILE_STORE"


def test_ordinary_upload_passes_back_the_bucket_it_was_given() -> None:
    """The handle is passed back on every path, not only for File Store."""
    client = _upload_client_for_destination("nominal-uploads-prod")

    with (
        patch.object(multipart, "create_multipart_request_session", return_value=_no_op_session()),
        patch.object(multipart, "_put_part", return_value=MagicMock(headers={"ETag": "etag-1"})),
    ):
        location = multipart.put_multipart_upload(
            "Bearer token", "ri.workspace", io.BytesIO(b"data"), "run-001.csv", "text/csv", client
        )

    assert location == "s3://bucket/object-key"
    assert client.initiate_multipart_upload.call_args.args[1].destination is None
    assert client.list_parts.call_args.kwargs["bucket"] == "nominal-uploads-prod"
    assert client.complete_multipart_upload.call_args.kwargs["bucket"] == "nominal-uploads-prod"


def test_abort_carries_the_destination_bucket_when_the_upload_fails() -> None:
    """A failed upload must abort against the same bucket handle initiate returned -- on every
    destination, not only the default one.
    """
    client = _upload_client_for_destination("FILE_STORE")

    with (
        patch.object(multipart, "create_multipart_request_session", return_value=_no_op_session()),
        patch.object(multipart, "_put_part", side_effect=requests.ConnectionError("boom")),
    ):
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
