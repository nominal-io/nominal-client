"""Storage-backend filename round trips, run once per AWS/Azure environment."""

from io import BytesIO
from typing import Callable
from uuid import uuid4

import pytest

from nominal.core import NominalClient

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
    """Safe special-character filenames upload and retain their literal catalog name on AWS and Azure."""
    ds = client.create_dataset(f"charname-{uuid4().hex[:8]}")
    archive(ds)
    # add_from_io completes the storage upload and reads the file metadata from the catalog.
    dataset_file = ds.add_from_io(BytesIO(csv_data), "timestamp", "iso_8601", file_name=stem)

    assert dataset_file.name == f"{stem}.csv"
