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
    _list_parts_then_complete,
    _put_part,
    _sign_and_put_part,
    _sign_part,
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


def test_sign_and_put_part_success_passes_timeout() -> None:
    client = MagicMock(spec=["sign_part", "_verify"])
    client._verify = False
    client.sign_part.return_value = _sign_response()
    session = MagicMock(spec=["put"])
    put_response = MagicMock()
    put_response.status_code = 200
    session.put.return_value = put_response

    result = _sign_and_put_part(client, session, "auth", "key", "uid", 1, b"chunk", timeout=12.5)

    assert result is put_response
    client.sign_part.assert_called_once_with("auth", "key", 1, "uid")
    _, kwargs = session.put.call_args
    assert kwargs["data"] == b"chunk"
    assert kwargs["verify"] is False
    assert kwargs["timeout"] == 12.5


def test_sign_and_put_part_raises_after_retries() -> None:
    client = MagicMock(spec=["sign_part", "_verify"])
    client._verify = True
    client.sign_part.return_value = _sign_response()
    session = MagicMock(spec=["put"])
    session.put.side_effect = requests.ConnectionError("boom")

    with pytest.raises(NominalMultipartUploadFailed):
        _sign_and_put_part(client, session, "auth", "key", "uid", 3, b"chunk", num_retries=2)

    assert session.put.call_count == 2


def test_complete_multipart_upload_builds_parts_from_etags_in_order() -> None:
    client = MagicMock(spec=["complete_multipart_upload"])
    client.complete_multipart_upload.return_value = MagicMock(location="s3://bucket/key")

    location = _complete_multipart_upload(client, "auth", "key", "uid", {3: '"c"', 1: '"a"', 2: '"b"'})

    assert location == "s3://bucket/key"
    _, _, _, parts = client.complete_multipart_upload.call_args[0]
    assert [(p.part_number, p.etag) for p in parts] == [(1, '"a"'), (2, '"b"'), (3, '"c"')]


def test_complete_multipart_upload_raises_when_location_missing() -> None:
    client = MagicMock(spec=["complete_multipart_upload"])
    client.complete_multipart_upload.return_value = MagicMock(location=None)

    with pytest.raises(NominalMultipartUploadFailed):
        _complete_multipart_upload(client, "auth", "key", "uid", {1: '"e"'})


def test_list_parts_then_complete_reads_etags_from_the_server() -> None:
    """The legacy path has no ETags of its own, so it must still ask the server for them."""
    client = MagicMock(spec=["list_parts", "complete_multipart_upload"])
    client.list_parts.return_value = [MagicMock(etag='"b"', part_number=2), MagicMock(etag='"a"', part_number=1)]
    client.complete_multipart_upload.return_value = MagicMock(location="s3://bucket/key")

    assert _list_parts_then_complete(client, "auth", "key", "uid") == "s3://bucket/key"

    client.list_parts.assert_called_once_with("auth", "key", "uid")
    _, _, _, parts = client.complete_multipart_upload.call_args[0]
    assert [(p.part_number, p.etag) for p in parts] == [(1, '"a"'), (2, '"b"')]


def test_put_multipart_upload_completes_via_list_parts() -> None:
    """Regression lock: `put_multipart_upload` (the legacy path) never collects per-part ETags of
    its own, so its completion must still ask the server for them via list_parts -- not the
    ETag-based primitive the newer MultipartUploader uses.
    """
    upload_client = MagicMock(
        spec=["initiate_multipart_upload", "sign_part", "list_parts", "complete_multipart_upload", "_verify"]
    )
    upload_client._verify = False
    upload_client.initiate_multipart_upload.return_value = MagicMock(key="key", upload_id="uid")
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
    upload_client.list_parts.assert_called_once_with("Bearer token", "key", "uid")
    _, _, _, parts = upload_client.complete_multipart_upload.call_args[0]
    assert [(p.part_number, p.etag) for p in parts] == [(1, '"a"')]


def test_sign_part_and_put_part_compose_without_retrying() -> None:
    client = MagicMock(spec=["sign_part"])
    client.sign_part.return_value = _sign_response()
    session = MagicMock(spec=["put"])
    session.put.side_effect = requests.ConnectionError("boom")

    signed = _sign_part(client, "auth", "key", "uid", 7)
    assert signed.url == "https://s3.example.com/signed"
    client.sign_part.assert_called_once_with("auth", "key", 7, "uid")

    with pytest.raises(requests.ConnectionError):
        _put_part(session, signed, b"chunk", verify=False, timeout=9.0)
    assert session.put.call_count == 1  # exactly one PUT; retrying is the caller's job
