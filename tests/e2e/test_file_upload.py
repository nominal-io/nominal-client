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
        # Keep the original Azure regression isolated; cover the other allowed characters
        # together. Local tests cover each character's validation and filename forwarding.
        "paren(reduced)",
        "space +&#[1]@=,;~^$",
        "unicode_résumé_日本",
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
