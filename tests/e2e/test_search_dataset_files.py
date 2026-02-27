"""End-to-end tests for NominalClient.search_dataset_files / Dataset.search_files."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from io import BytesIO
from typing import Iterator
from uuid import uuid4

import pytest

from nominal.core import NominalClient
from nominal.core.dataset import Dataset
from nominal.core.dataset_file import DatasetFile, wait_for_files_to_ingest

_HEADER = b"timestamp,temperature,pressure\n"


@dataclass
class DatasetSearchContext:
    dataset: Dataset
    file_jan: DatasetFile
    """Jan 2024 · source=alpha, region=us-east · spans [2024-01-01T00:00:00Z, 2024-01-01T01:00:00Z]"""
    file_jun: DatasetFile
    """Jun 2024 · source=beta,  region=eu-west · spans [2024-06-01T00:00:00Z, 2024-06-01T01:00:00Z]"""
    file_dec: DatasetFile
    """Dec 2024 · source=alpha, region=eu-west · spans [2024-12-01T00:00:00Z, 2024-12-01T01:00:00Z]"""


@pytest.fixture(scope="session")
def dataset_search_context(client: NominalClient) -> Iterator[DatasetSearchContext]:
    tag = uuid4().hex
    dataset = client.create_dataset(f"dataset-search-{tag}")

    file_jan = dataset.add_from_io(
        BytesIO(_HEADER + b"1704067200,20.1,1013.2\n1704070800,21.3,1012.8\n"),
        timestamp_column="timestamp",
        timestamp_type="epoch_seconds",
        file_name="jan_2024",
        tags={"source": "alpha", "region": "us-east"},
    )
    file_jun = dataset.add_from_io(
        BytesIO(_HEADER + b"1717200000,25.4,1008.1\n1717203600,26.0,1007.5\n"),
        timestamp_column="timestamp",
        timestamp_type="epoch_seconds",
        file_name="jun_2024",
        tags={"source": "beta", "region": "eu-west"},
    )
    file_dec = dataset.add_from_io(
        BytesIO(_HEADER + b"1733011200,8.2,1020.3\n1733014800,7.9,1021.0\n"),
        timestamp_column="timestamp",
        timestamp_type="epoch_seconds",
        file_name="dec_2024",
        tags={"source": "alpha", "region": "eu-west"},
    )
    wait_for_files_to_ingest([file_jan, file_jun, file_dec], poll_interval=timedelta(seconds=0.5))

    yield DatasetSearchContext(
        dataset=dataset,
        file_jan=file_jan,
        file_jun=file_jun,
        file_dec=file_dec,
    )

    dataset.archive()


# ---------------------------------------------------------------------------
# Tag filters
# ---------------------------------------------------------------------------


def test_search_dataset_files_no_filter(client: NominalClient, dataset_search_context: DatasetSearchContext) -> None:
    ctx = dataset_search_context
    results = client.search_dataset_files(ctx.dataset)
    ids = {f.id for f in results}
    assert ids == {ctx.file_jan.id, ctx.file_jun.id, ctx.file_dec.id}


def test_search_dataset_files_by_source_alpha(
    client: NominalClient, dataset_search_context: DatasetSearchContext
) -> None:
    ctx = dataset_search_context
    results = client.search_dataset_files(ctx.dataset, file_tags={"source": "alpha"})
    ids = {f.id for f in results}
    assert ids == {ctx.file_jan.id, ctx.file_dec.id}


def test_search_dataset_files_by_source_beta(
    client: NominalClient, dataset_search_context: DatasetSearchContext
) -> None:
    ctx = dataset_search_context
    results = client.search_dataset_files(ctx.dataset, file_tags={"source": "beta"})
    ids = {f.id for f in results}
    assert ids == {ctx.file_jun.id}


def test_search_dataset_files_by_region_eu_west(
    client: NominalClient, dataset_search_context: DatasetSearchContext
) -> None:
    ctx = dataset_search_context
    results = client.search_dataset_files(ctx.dataset, file_tags={"region": "eu-west"})
    ids = {f.id for f in results}
    assert ids == {ctx.file_jun.id, ctx.file_dec.id}


def test_search_dataset_files_by_region_us_east(
    client: NominalClient, dataset_search_context: DatasetSearchContext
) -> None:
    ctx = dataset_search_context
    results = client.search_dataset_files(ctx.dataset, file_tags={"region": "us-east"})
    ids = {f.id for f in results}
    assert ids == {ctx.file_jan.id}


def test_search_dataset_files_by_combined_tags(
    client: NominalClient, dataset_search_context: DatasetSearchContext
) -> None:
    ctx = dataset_search_context
    # source=alpha AND region=eu-west → dec_2024 only
    results = client.search_dataset_files(ctx.dataset, file_tags={"source": "alpha", "region": "eu-west"})
    ids = {f.id for f in results}
    assert ids == {ctx.file_dec.id}


# ---------------------------------------------------------------------------
# Time range filters
# ---------------------------------------------------------------------------


def test_search_dataset_files_by_time_range(
    client: NominalClient, dataset_search_context: DatasetSearchContext
) -> None:
    ctx = dataset_search_context
    # Mar–Sep 2024 window fully contains jun_2024 and nothing else
    results = client.search_dataset_files(ctx.dataset, start="2024-03-01", end="2024-09-01")
    ids = {f.id for f in results}
    assert ids == {ctx.file_jun.id}


def test_search_dataset_files_combined_tag_and_time(
    client: NominalClient, dataset_search_context: DatasetSearchContext
) -> None:
    ctx = dataset_search_context
    # source=alpha AND file starts on or after Jun 2024 → dec_2024 only
    results = client.search_dataset_files(ctx.dataset, start="2024-06-01", file_tags={"source": "alpha"})
    ids = {f.id for f in results}
    assert ids == {ctx.file_dec.id}


# ---------------------------------------------------------------------------
# Boundary / overlap semantics
#
# jan_2024 spans [2024-01-01T00:00:00Z, 2024-01-01T01:00:00Z]
# dec_2024 spans [2024-12-01T00:00:00Z, 2024-12-01T01:00:00Z]
#
# `start` filters on the file's OWN start time  (file.start >= search.start).
# `end`   filters on the file's OWN end time    (file.end   <= search.end).
#
# This means the boundaries are INCLUSIVE for exact matches, but a file whose
# start is BEFORE the search `start` is excluded even if it still overlaps the
# search window — i.e., there is NO overlap logic.
# ---------------------------------------------------------------------------


def test_search_dataset_files_start_exact_boundary_is_inclusive(
    client: NominalClient, dataset_search_context: DatasetSearchContext
) -> None:
    ctx = dataset_search_context
    # search start == jan_2024's own start → jan_2024 is included (inclusive lower bound)
    results = client.search_dataset_files(ctx.dataset, start="2024-01-01T00:00:00Z")
    ids = {f.id for f in results}
    assert ctx.file_jan.id in ids


def test_search_dataset_files_end_exact_boundary_is_inclusive(
    client: NominalClient, dataset_search_context: DatasetSearchContext
) -> None:
    ctx = dataset_search_context
    # search end == dec_2024's own end → dec_2024 is included (inclusive upper bound)
    results = client.search_dataset_files(ctx.dataset, end="2024-12-01T01:00:00Z")
    ids = {f.id for f in results}
    assert ctx.file_dec.id in ids


def test_search_dataset_files_start_no_overlap_semantics(
    client: NominalClient, dataset_search_context: DatasetSearchContext
) -> None:
    ctx = dataset_search_context
    # jan_2024 spans [00:00, 01:00] on Jan 1. Search start is the midpoint (00:30).
    # Because `start` compares against the file's OWN start time, jan_2024 (which starts
    # at 00:00, BEFORE 00:30) is excluded even though it overlaps the search window.
    # This directly answers: "if search start=6 and file spans [4, 8], is it included?" → NO.
    results = client.search_dataset_files(ctx.dataset, start="2024-01-01T00:30:00Z")
    ids = {f.id for f in results}
    assert ctx.file_jan.id not in ids
    assert ctx.file_jun.id in ids
    assert ctx.file_dec.id in ids


def test_search_dataset_files_end_no_overlap_semantics(
    client: NominalClient, dataset_search_context: DatasetSearchContext
) -> None:
    ctx = dataset_search_context
    # dec_2024 spans [00:00, 01:00] on Dec 1. Search end is the midpoint (00:30).
    # Because `end` compares against the file's OWN end time, dec_2024 (which ends
    # at 01:00, AFTER 00:30) is excluded even though it overlaps the search window.
    results = client.search_dataset_files(ctx.dataset, end="2024-12-01T00:30:00Z")
    ids = {f.id for f in results}
    assert ctx.file_dec.id not in ids
    assert ctx.file_jan.id in ids
    assert ctx.file_jun.id in ids
