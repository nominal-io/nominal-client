"""Tests for nominal.core._utils.multipart upload-filename handling.

Regression lock for the encoding fix: the filename must reach object storage *un-encoded* (no
``quote_plus``), and unsafe filenames must be rejected before any upload begins.
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pytest

from nominal.core._utils import multipart
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
