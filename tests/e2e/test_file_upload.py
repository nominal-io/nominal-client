"""Storage-backend filename round trips, run once per AWS/Azure environment."""

from io import BytesIO
from typing import Callable
from uuid import uuid4

import pytest

from nominal.core import NominalClient
from tests.e2e import POLL_INTERVAL

ArchiveFn = Callable[[object], None]


@pytest.mark.parametrize(
    "stem",
    [
        # A representative set of special characters the client allows (see UNSAFE_UPLOAD_CHARS for
        # the rejected ones). Each is uploaded for real against every backend in this job's matrix, so
        # if any character that passes our client-side validation starts failing server-side — in
        # scout or the cloud storage layer — this test goes red and we hear about it immediately.
        "paren(reduced)",  # the original Azure bug — was stored as paren%28reduced%29.csv
        "with space",
        "amp&ersand",
        "plus+plus",
        "hash#tag",
        "bracket[1]",
        "at@sign",
        "equals=sign",
        "comma,comma",
        "semi;colon",
        "tilde~caret^",
        "dollar$sign",
        "unicode_résumé",
    ],
)
def test_special_char_filename_round_trips(
    client: NominalClient, csv_data: bytes, archive: ArchiveFn, stem: str
) -> None:
    """Filenames with safe special characters upload, ingest, and keep their name intact.

    Regression test for the filename-encoding fix. This runs against S3 (gov staging), which
    tolerates the *upload* of any character — so the load-bearing assertion is the stored NAME:
    before the fix the client quote_plus'd the name (``paren%28reduced%29.csv``); after it, the
    literal name round-trips.
    """
    ds = client.create_dataset(f"charname-{uuid4().hex[:8]}")
    archive(ds)
    dataset_file = ds.add_from_io(
        BytesIO(csv_data), "timestamp", "iso_8601", file_name=stem
    ).poll_until_ingestion_completed(interval=POLL_INTERVAL)

    assert dataset_file.name == f"{stem}.csv"
