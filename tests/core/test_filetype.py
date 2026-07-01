"""Tests for nominal.core.filetype.FileType extension inference."""

from __future__ import annotations

import pytest

from nominal.core.filetype import FileType, FileTypes


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        # Single-suffix types
        ("x.zip", FileTypes.ZIP),
        ("x.csv", FileTypes.CSV),
        ("x.parquet", FileTypes.PARQUET),
        ("x.jsonl", FileTypes.JOURNAL_JSONL),
        ("x.mp4", FileTypes.MP4),
        # Multi-suffix types must beat their shorter counterparts regardless of
        # the declaration order of FileTypes members (longest match wins).
        ("x.parquet.zip", FileTypes.PARQUET_ZIP),
        ("x.parquet.gz", FileTypes.PARQUET_GZ),
        ("x.parquet.tar", FileTypes.PARQUET_TAR),
        ("x.parquet.tar.gz", FileTypes.PARQUET_TAR_GZ),
        ("x.csv.gz", FileTypes.CSV_GZ),
        ("x.jsonl.gz", FileTypes.JOURNAL_JSONL_GZ),
        # Extension matching is case-insensitive.
        ("X.ZIP", FileTypes.ZIP),
        ("X.PARQUET.ZIP", FileTypes.PARQUET_ZIP),
        # A leading directory path does not affect suffix matching.
        ("some/dir/x.parquet.zip", FileTypes.PARQUET_ZIP),
    ],
)
def test_from_path_infers_registered_type(path: str, expected: FileType) -> None:
    assert FileType.from_path(path) == expected
