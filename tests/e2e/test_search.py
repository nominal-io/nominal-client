"""End-to-end tests for all client search methods.

One session-scoped fixture (search_context) creates all the entities we need
up-front, then each test asserts against that shared state.  Everything
created here is archived in fixture teardown so it doesn't accumulate in
the test environment.

# TODO(drake): Add workbook and workbook-template search tests once there is a
# programmatic way to create workbooks without a pre-existing template.
# TODO(drake): Add checklist search tests once there is a programmatic way to
# create checklists.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from io import BytesIO
from typing import Iterator
from uuid import uuid4

import pytest

from nominal.core import EventType, NominalClient
from nominal.core.asset import Asset
from nominal.core.dataset import Dataset
from nominal.core.dataset_file import DatasetFile, wait_for_files_to_ingest
from nominal.core.event import Event
from nominal.core.run import Run
from nominal.core.video import Video
from tests.e2e import _create_random_start_end

_DATASET_HEADER = b"timestamp,temperature,pressure\n"


@dataclass
class SearchContext:
    """All entities created once for the entire search test suite."""

    tag: str
    """Unique 32-character hex string embedded in all entity names for isolation."""

    # Runs
    run: Run
    """Plain run with search-test label and property."""

    # Assets
    asset: Asset
    """Asset with search-test label and property; all test events are attached here."""

    # Events (all attached to `asset` for isolation)
    event_info: Event
    event_error: Event
    event_flag: Event

    # Videos
    video: Video
    """Fully ingested video."""

    # Dataset + files
    dataset: Dataset
    file_jan: DatasetFile
    """Jan 2024 · source=alpha, region=us-east · spans [2024-01-01T00:00:00Z, 2024-01-01T01:00:00Z]"""
    file_jun: DatasetFile
    """Jun 2024 · source=beta,  region=eu-west · spans [2024-06-01T00:00:00Z, 2024-06-01T01:00:00Z]"""
    file_dec: DatasetFile
    """Dec 2024 · source=alpha, region=eu-west · spans [2024-12-01T00:00:00Z, 2024-12-01T01:00:00Z]"""


@pytest.fixture(scope="session")
def search_context(client: NominalClient, mp4_data: bytes) -> Iterator[SearchContext]:
    tag = uuid4().hex  # 32-char hex; unique per test session

    start, end = _create_random_start_end()

    # --- Assets ---
    asset = client.create_asset(
        f"asset-{tag}",
        labels=["search-test"],
        properties={"search-tag": tag},
    )

    # --- Runs ---
    run = client.create_run(
        f"run-{tag}",
        start,
        end,
        labels=["search-test"],
        properties={"search-tag": tag},
    )

    # --- Events (attached to `asset` for search isolation) ---
    event_info = client.create_event(f"event-info-{tag}", EventType.INFO, start, assets=[asset])
    event_error = client.create_event(f"event-error-{tag}", EventType.ERROR, start, assets=[asset])
    event_flag = client.create_event(f"event-flag-{tag}", EventType.FLAG, start, assets=[asset])

    # --- Video ---
    video = client.create_video(f"video-{tag}")
    video_file = video.add_from_io(BytesIO(mp4_data), f"video-{tag}.mp4", start=start)
    video_file.poll_until_ingestion_completed(interval=timedelta(seconds=0.5))

    # --- Dataset + files ---
    dataset = client.create_dataset(f"dataset-{tag}")
    file_jan = dataset.add_from_io(
        BytesIO(_DATASET_HEADER + b"1704067200,20.1,1013.2\n1704070800,21.3,1012.8\n"),
        timestamp_column="timestamp",
        timestamp_type="epoch_seconds",
        file_name="jan_2024",
        tags={"source": "alpha", "region": "us-east"},
    )
    file_jun = dataset.add_from_io(
        BytesIO(_DATASET_HEADER + b"1717200000,25.4,1008.1\n1717203600,26.0,1007.5\n"),
        timestamp_column="timestamp",
        timestamp_type="epoch_seconds",
        file_name="jun_2024",
        tags={"source": "beta", "region": "eu-west"},
    )
    file_dec = dataset.add_from_io(
        BytesIO(_DATASET_HEADER + b"1733011200,8.2,1020.3\n1733014800,7.9,1021.0\n"),
        timestamp_column="timestamp",
        timestamp_type="epoch_seconds",
        file_name="dec_2024",
        tags={"source": "alpha", "region": "eu-west"},
    )
    wait_for_files_to_ingest([file_jan, file_jun, file_dec], poll_interval=timedelta(seconds=0.5))

    ctx = SearchContext(
        tag=tag,
        run=run,
        asset=asset,
        event_info=event_info,
        event_error=event_error,
        event_flag=event_flag,
        video=video,
        dataset=dataset,
        file_jan=file_jan,
        file_jun=file_jun,
        file_dec=file_dec,
    )
    yield ctx

    # Teardown: archive all live entities so they don't accumulate in the environment.
    run.archive()
    asset.archive()
    event_info.archive()
    event_error.archive()
    event_flag.archive()
    video.archive()
    dataset.archive()


# ---------------------------------------------------------------------------
# Run search
# ---------------------------------------------------------------------------


def test_search_runs_by_name_substring(client: NominalClient, search_context: SearchContext) -> None:
    """Searching runs by name_substring returns only runs whose name contains the session tag."""
    ctx = search_context
    results = client.search_runs(name_substring=ctx.tag)
    rids = {r.rid for r in results}
    assert rids == {ctx.run.rid}


def test_search_runs_by_labels(client: NominalClient, search_context: SearchContext) -> None:
    """Filtering by a label narrows results to only the run created with that label."""
    ctx = search_context
    results = client.search_runs(labels=["search-test"], name_substring=ctx.tag)
    rids = {r.rid for r in results}
    assert rids == {ctx.run.rid}


def test_search_runs_by_properties(client: NominalClient, search_context: SearchContext) -> None:
    """Filtering by a key-value property returns only the run that carries that property."""
    ctx = search_context
    results = client.search_runs(properties={"search-tag": ctx.tag})
    rids = {r.rid for r in results}
    assert rids == {ctx.run.rid}



# ---------------------------------------------------------------------------
# Asset search
# ---------------------------------------------------------------------------


def test_search_assets_by_name(client: NominalClient, search_context: SearchContext) -> None:
    """Searching assets by name substring returns only the asset whose name contains the session tag."""
    ctx = search_context
    results = client.search_assets(search_text=ctx.tag)
    rids = {a.rid for a in results}
    assert rids == {ctx.asset.rid}


def test_search_assets_by_labels(client: NominalClient, search_context: SearchContext) -> None:
    """Filtering by a label narrows results to only the asset created with that label."""
    ctx = search_context
    results = client.search_assets(labels=["search-test"], search_text=ctx.tag)
    rids = {a.rid for a in results}
    assert rids == {ctx.asset.rid}


def test_search_assets_by_properties(client: NominalClient, search_context: SearchContext) -> None:
    """Filtering by a key-value property returns only the asset that carries that property."""
    ctx = search_context
    results = client.search_assets(properties={"search-tag": ctx.tag})
    rids = {a.rid for a in results}
    assert rids == {ctx.asset.rid}



# ---------------------------------------------------------------------------
# Event search
# ---------------------------------------------------------------------------


def test_search_events_by_asset(client: NominalClient, search_context: SearchContext) -> None:
    """Searching events scoped to an asset returns all events attached to that asset."""
    ctx = search_context
    results = client.search_events(assets=[ctx.asset])
    rids = {e.rid for e in results}
    assert rids == {ctx.event_info.rid, ctx.event_error.rid, ctx.event_flag.rid}


def test_search_events_by_event_type(client: NominalClient, search_context: SearchContext) -> None:
    """Filtering by event_type returns only events whose type matches."""
    ctx = search_context
    results = client.search_events(event_type=EventType.INFO, assets=[ctx.asset])
    rids = {e.rid for e in results}
    assert rids == {ctx.event_info.rid}



# ---------------------------------------------------------------------------
# Video search
# ---------------------------------------------------------------------------


def test_search_videos_by_name(client: NominalClient, search_context: SearchContext) -> None:
    """Searching videos by name substring returns only the video whose name contains the session tag."""
    ctx = search_context
    results = client.search_videos(search_text=ctx.tag)
    rids = {v.rid for v in results}
    assert rids == {ctx.video.rid}


# ---------------------------------------------------------------------------
# Dataset file search
# ---------------------------------------------------------------------------


def test_search_dataset_files_no_filter(client: NominalClient, search_context: SearchContext) -> None:
    """Listing dataset files with no filter arguments returns all files in the dataset."""
    ctx = search_context
    results = client.search_dataset_files(ctx.dataset)
    ids = {f.id for f in results}
    assert ids == {ctx.file_jan.id, ctx.file_jun.id, ctx.file_dec.id}


def test_search_dataset_files_by_source_alpha(client: NominalClient, search_context: SearchContext) -> None:
    """Filtering by source=alpha returns only the Jan and Dec files."""
    ctx = search_context
    results = client.search_dataset_files(ctx.dataset, file_tags={"source": "alpha"})
    ids = {f.id for f in results}
    assert ids == {ctx.file_jan.id, ctx.file_dec.id}


def test_search_dataset_files_by_source_beta(client: NominalClient, search_context: SearchContext) -> None:
    """Filtering by source=beta returns only the Jun file."""
    ctx = search_context
    results = client.search_dataset_files(ctx.dataset, file_tags={"source": "beta"})
    ids = {f.id for f in results}
    assert ids == {ctx.file_jun.id}


def test_search_dataset_files_by_region_eu_west(client: NominalClient, search_context: SearchContext) -> None:
    """Filtering by region=eu-west returns the Jun and Dec files."""
    ctx = search_context
    results = client.search_dataset_files(ctx.dataset, file_tags={"region": "eu-west"})
    ids = {f.id for f in results}
    assert ids == {ctx.file_jun.id, ctx.file_dec.id}


def test_search_dataset_files_by_region_us_east(client: NominalClient, search_context: SearchContext) -> None:
    """Filtering by region=us-east returns only the Jan file."""
    ctx = search_context
    results = client.search_dataset_files(ctx.dataset, file_tags={"region": "us-east"})
    ids = {f.id for f in results}
    assert ids == {ctx.file_jan.id}


def test_search_dataset_files_by_combined_tags(client: NominalClient, search_context: SearchContext) -> None:
    """Combining two tag filters (source=alpha AND region=eu-west) returns only the Dec file."""
    ctx = search_context
    results = client.search_dataset_files(ctx.dataset, file_tags={"source": "alpha", "region": "eu-west"})
    ids = {f.id for f in results}
    assert ids == {ctx.file_dec.id}


def test_search_dataset_files_by_time_range(client: NominalClient, search_context: SearchContext) -> None:
    """A time range that fully contains only Jun 2024 returns only that file."""
    ctx = search_context
    results = client.search_dataset_files(ctx.dataset, start="2024-03-01", end="2024-09-01")
    ids = {f.id for f in results}
    assert ids == {ctx.file_jun.id}


def test_search_dataset_files_combined_tag_and_time(client: NominalClient, search_context: SearchContext) -> None:
    """Combining a tag filter and a start time returns only files that match both criteria."""
    ctx = search_context
    results = client.search_dataset_files(ctx.dataset, start="2024-06-01", file_tags={"source": "alpha"})
    ids = {f.id for f in results}
    assert ids == {ctx.file_dec.id}


# ---------------------------------------------------------------------------
# Dataset file search — boundary / overlap semantics
#
# file_jan spans [2024-01-01T00:00:00Z, 2024-01-01T01:00:00Z]
# file_dec spans [2024-12-01T00:00:00Z, 2024-12-01T01:00:00Z]
#
# The server uses OVERLAP semantics:
#   `start` → file included if file.end   >= search.start  (not entirely before the window)
#   `end`   → file included if file.start <= search.end    (not entirely after the window)
#
# A file that straddles the boundary IS included.
# A file that ends before `start`, or starts after `end`, is excluded.
#
# This answers: "if search start=6 and file spans [4, 8], is it included?" → YES.
# ---------------------------------------------------------------------------


def test_search_dataset_files_start_exact_boundary_is_inclusive(
    client: NominalClient, search_context: SearchContext
) -> None:
    """A search start equal to a file's own start timestamp includes that file (inclusive lower bound)."""
    ctx = search_context
    # search start == file_jan's own start → file_jan is included (inclusive lower bound).
    # All three files end on or after Jan 1 00:00, so all three are returned.
    results = client.search_dataset_files(ctx.dataset, start="2024-01-01T00:00:00Z")
    ids = {f.id for f in results}
    assert ids == {ctx.file_jan.id, ctx.file_jun.id, ctx.file_dec.id}


def test_search_dataset_files_end_exact_boundary_is_inclusive(
    client: NominalClient, search_context: SearchContext
) -> None:
    """A search end equal to a file's own end timestamp includes that file (inclusive upper bound)."""
    ctx = search_context
    # search end == file_dec's own end → file_dec is included (inclusive upper bound).
    # All three files start on or before Dec 1 01:00, so all three are returned.
    results = client.search_dataset_files(ctx.dataset, end="2024-12-01T01:00:00Z")
    ids = {f.id for f in results}
    assert ids == {ctx.file_jan.id, ctx.file_jun.id, ctx.file_dec.id}


def test_search_dataset_files_start_uses_overlap_semantics(
    client: NominalClient, search_context: SearchContext
) -> None:
    """A file whose range starts before the search window but overlaps it is still included."""
    ctx = search_context
    # file_jan spans [00:00, 01:00] on Jan 1. Search start is the midpoint (00:30).
    # file_jan starts BEFORE the search start but its range still overlaps → IS included.
    # This directly answers: "if search start=6 and file spans [4, 8], is it included?" → YES.
    results = client.search_dataset_files(ctx.dataset, start="2024-01-01T00:30:00Z")
    ids = {f.id for f in results}
    assert ids == {ctx.file_jan.id, ctx.file_jun.id, ctx.file_dec.id}


def test_search_dataset_files_end_uses_overlap_semantics(
    client: NominalClient, search_context: SearchContext
) -> None:
    """A file whose range ends after the search window but overlaps it is still included."""
    ctx = search_context
    # file_dec spans [00:00, 01:00] on Dec 1. Search end is the midpoint (00:30).
    # file_dec ends AFTER the search end but its range still overlaps → IS included.
    results = client.search_dataset_files(ctx.dataset, end="2024-12-01T00:30:00Z")
    ids = {f.id for f in results}
    assert ids == {ctx.file_jan.id, ctx.file_jun.id, ctx.file_dec.id}
