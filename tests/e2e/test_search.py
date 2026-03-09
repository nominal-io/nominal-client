"""End-to-end tests for public search methods.

One session-scoped fixture (search_context) creates the common entities used by
the baseline search tests. A second session-scoped fixture
(`archive_search_context`) adds active/archived resource pairs so archive-status
filters can be exercised against shared state instead of per-test setup.

Checklist archive-status coverage is intentionally omitted because the SDK does
not yet expose a public checklist creation API.
"""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Callable, Iterator, Sequence, cast
from uuid import uuid4

import pytest

from nominal.core import ArchiveStatusFilter, EventType, NominalClient
from nominal.core._utils.api_tools import HasRid
from nominal.core.asset import Asset
from nominal.core.dataset import Dataset
from nominal.core.dataset_file import DatasetFile, wait_for_files_to_ingest
from nominal.core.event import Event
from nominal.core.run import Run
from nominal.core.secret import Secret
from nominal.core.video import Video
from nominal.core.workbook import Workbook
from nominal.core.workbook_template import WorkbookTemplate
from tests.e2e import POLL_INTERVAL, _create_random_start_end

_DATASET_HEADER = b"timestamp,temperature,pressure\n"


@dataclass
class SearchContext:
    """Common entities created once for the entire search test suite."""

    tag: str
    """Unique 32-character hex string embedded in all entity names for isolation."""

    run: Run
    """Plain run with search-test label and property."""

    asset: Asset
    """Asset with search-test label and property; all baseline test events are attached here."""

    event_info: Event
    event_error: Event
    event_flag: Event

    video: Video
    """Fully ingested video."""

    dataset: Dataset
    file_jan: DatasetFile
    """Jan 2024 · source=alpha, region=us-east · spans [2024-01-01T00:00:00Z, 2024-01-01T01:00:00Z]"""
    file_jun: DatasetFile
    """Jun 2024 · source=beta,  region=eu-west · spans [2024-06-01T00:00:00Z, 2024-06-01T01:00:00Z]"""
    file_dec: DatasetFile
    """Dec 2024 · source=alpha, region=eu-west · spans [2024-12-01T00:00:00Z, 2024-12-01T01:00:00Z]"""


@dataclass
class ArchiveSearchContext:
    """Additional resources used to exercise archive-status filters."""

    archived_run: Run
    archived_asset: Asset
    archived_dataset: Dataset
    active_secret: Secret
    archived_secret: Secret
    archived_video: Video
    event_asset: Asset
    event_tag: str
    active_archive_event: Event
    archived_archive_event: Event
    template_tag: str
    active_workbook_template: WorkbookTemplate
    archived_workbook_template: WorkbookTemplate
    run_workbook_tag: str
    active_run_workbook: Workbook
    archived_run_workbook: Workbook
    draft_run_workbook: Workbook
    asset_workbook_tag: str
    active_asset_workbook: Workbook
    archived_asset_workbook: Workbook
    draft_asset_workbook: Workbook


def _rids(items: Sequence[object]) -> set[str]:
    return {cast(HasRid, item).rid for item in items}


def _assert_archive_status_behavior(
    search_fn: Callable[[ArchiveStatusFilter], Sequence[object]],
    *,
    active_rids: set[str],
    archived_rids: set[str],
) -> None:
    assert _rids(search_fn(ArchiveStatusFilter.NOT_ARCHIVED)) == active_rids
    assert _rids(search_fn(ArchiveStatusFilter.ARCHIVED)) == archived_rids
    assert _rids(search_fn(ArchiveStatusFilter.ANY)) == active_rids | archived_rids


def _assert_include_archived_behavior(
    search_fn: Callable[[bool], Sequence[object]],
    *,
    active_rids: set[str],
    archived_rids: set[str],
) -> None:
    with pytest.warns(UserWarning, match="include_archived"):
        assert _rids(search_fn(False)) == active_rids
    with pytest.warns(UserWarning, match="include_archived"):
        assert _rids(search_fn(True)) == active_rids | archived_rids


def _assert_include_drafts_behavior(
    search_fn: Callable[[bool], Sequence[object]],
    *,
    non_draft_rids: set[str],
    draft_rids: set[str],
) -> None:
    assert _rids(search_fn(False)) == non_draft_rids
    assert _rids(search_fn(True)) == non_draft_rids | draft_rids


@pytest.fixture(scope="session")
def search_context(client: NominalClient, mp4_data: bytes) -> Iterator[SearchContext]:
    tag = uuid4().hex  # 32-char hex; unique per test session

    start, end = _create_random_start_end()

    asset = client.create_asset(
        f"asset-{tag}",
        labels=["search-test"],
        properties={"search-tag": tag},
    )

    run = client.create_run(
        f"run-{tag}",
        start,
        end,
        labels=["search-test"],
        properties={"search-tag": tag},
    )

    event_info = client.create_event(f"event-info-{tag}", EventType.INFO, start, assets=[asset])
    event_error = client.create_event(f"event-error-{tag}", EventType.ERROR, start, assets=[asset])
    event_flag = client.create_event(f"event-flag-{tag}", EventType.FLAG, start, assets=[asset])

    video = client.create_video(f"video-{tag}")
    video_file = video.add_from_io(BytesIO(mp4_data), f"video-{tag}.mp4", start=start)
    video_file.poll_until_ingestion_completed(interval=POLL_INTERVAL)

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
    wait_for_files_to_ingest([file_jan, file_jun, file_dec], poll_interval=POLL_INTERVAL)

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

    run.archive()
    asset.archive()
    event_info.archive()
    event_error.archive()
    event_flag.archive()
    video.archive()
    dataset.archive()


@pytest.fixture(scope="session")
def archive_search_context(  # noqa: PLR0915
    client: NominalClient,
    search_context: SearchContext,
    mp4_data: bytes,
) -> Iterator[ArchiveSearchContext]:
    start, end = _create_random_start_end()

    archived_run = client.create_run(
        f"run-archived-{search_context.tag}",
        start,
        end,
        labels=["search-test"],
        properties={"search-tag": search_context.tag},
    )
    archived_run.archive()

    archived_asset = client.create_asset(
        f"asset-archived-{search_context.tag}",
        labels=["search-test"],
        properties={"search-tag": search_context.tag},
    )
    archived_asset.archive()

    archived_dataset = client.create_dataset(f"dataset-archived-{search_context.tag}")
    archived_dataset.archive()

    active_secret = client.create_secret(f"secret-active-{search_context.tag}", "active secret value")
    archived_secret = client.create_secret(f"secret-archived-{search_context.tag}", "archived secret value")
    archived_secret.archive()

    archived_video = client.create_video(f"video-archived-{search_context.tag}")
    archived_video_file = archived_video.add_from_io(
        BytesIO(mp4_data),
        f"video-archived-{search_context.tag}.mp4",
        start=start,
    )
    archived_video_file.poll_until_ingestion_completed(interval=POLL_INTERVAL)
    archived_video.archive()

    event_asset = client.create_asset(f"event-asset-{uuid4().hex}")
    event_tag = f"archive-event-{search_context.tag}"
    active_archive_event = client.create_event(
        f"{event_tag}-active",
        EventType.INFO,
        start,
        assets=[event_asset],
    )
    archived_archive_event = client.create_event(
        f"{event_tag}-archived",
        EventType.INFO,
        start,
        assets=[event_asset],
    )
    archived_archive_event.archive()

    template_tag = f"archive-template-{search_context.tag}"
    active_workbook_template = client.create_workbook_template(
        f"{template_tag}-active",
        labels=[template_tag],
    )
    archived_workbook_template = client.create_workbook_template(
        f"{template_tag}-archived",
        labels=[template_tag],
    )
    archived_workbook_template.archive()

    run_workbook_tag = f"archive-run-workbook-{search_context.tag}"
    active_run_workbook = active_workbook_template.create_workbook(
        title=f"{run_workbook_tag}-active",
        run=search_context.run,
    )
    archived_run_workbook = active_workbook_template.create_workbook(
        title=f"{run_workbook_tag}-archived",
        run=search_context.run,
    )
    archived_run_workbook.archive()
    draft_run_workbook = active_workbook_template.create_workbook(
        title=f"{run_workbook_tag}-draft",
        run=search_context.run,
        is_draft=True,
    )

    asset_workbook_tag = f"archive-asset-workbook-{search_context.tag}"
    active_asset_workbook = active_workbook_template.create_workbook(
        title=f"{asset_workbook_tag}-active",
        asset=search_context.asset,
    )

    archived_asset_workbook = active_workbook_template.create_workbook(
        title=f"{asset_workbook_tag}-archived",
        asset=search_context.asset,
    )
    archived_asset_workbook.archive()

    draft_asset_workbook = active_workbook_template.create_workbook(
        title=f"{asset_workbook_tag}-draft",
        asset=search_context.asset,
        is_draft=True,
    )

    ctx = ArchiveSearchContext(
        archived_run=archived_run,
        archived_asset=archived_asset,
        archived_dataset=archived_dataset,
        active_secret=active_secret,
        archived_secret=archived_secret,
        archived_video=archived_video,
        event_asset=event_asset,
        event_tag=event_tag,
        active_archive_event=active_archive_event,
        archived_archive_event=archived_archive_event,
        template_tag=template_tag,
        active_workbook_template=active_workbook_template,
        archived_workbook_template=archived_workbook_template,
        run_workbook_tag=run_workbook_tag,
        active_run_workbook=active_run_workbook,
        archived_run_workbook=archived_run_workbook,
        draft_run_workbook=draft_run_workbook,
        asset_workbook_tag=asset_workbook_tag,
        active_asset_workbook=active_asset_workbook,
        archived_asset_workbook=archived_asset_workbook,
        draft_asset_workbook=draft_asset_workbook,
    )
    yield ctx

    active_run_workbook.archive()
    archived_run_workbook.archive()
    draft_run_workbook.archive()
    active_asset_workbook.archive()
    archived_asset_workbook.archive()
    draft_asset_workbook.archive()
    active_workbook_template.archive()
    archived_workbook_template.archive()
    active_archive_event.archive()
    archived_archive_event.archive()
    event_asset.archive()
    active_secret.archive()
    archived_secret.archive()
    archived_video.archive()
    archived_dataset.archive()
    archived_asset.archive()
    archived_run.archive()


# ---------------------------------------------------------------------------
# Run search
# ---------------------------------------------------------------------------


def test_search_runs_by_name_substring(client: NominalClient, search_context: SearchContext) -> None:
    """Searching runs by name_substring returns only runs whose name contains the session tag."""
    results = client.search_runs(name_substring=search_context.tag)
    rids = {r.rid for r in results}
    assert rids == {search_context.run.rid}


def test_search_runs_by_labels(client: NominalClient, search_context: SearchContext) -> None:
    """Filtering by a label narrows results to only the run created with that label."""
    results = client.search_runs(labels=["search-test"], name_substring=search_context.tag)
    rids = {r.rid for r in results}
    assert rids == {search_context.run.rid}


def test_search_runs_by_properties(client: NominalClient, search_context: SearchContext) -> None:
    """Filtering by a key-value property returns only the run that carries that property."""
    results = client.search_runs(properties={"search-tag": search_context.tag})
    rids = {r.rid for r in results}
    assert rids == {search_context.run.rid}


def test_search_runs_archive_status(
    client: NominalClient,
    search_context: SearchContext,
    archive_search_context: ArchiveSearchContext,
) -> None:
    """Run search honors archive_status filtering."""
    _assert_archive_status_behavior(
        lambda archive_status: client.search_runs(
            name_substring=search_context.tag,
            archive_status=archive_status,
        ),
        active_rids={search_context.run.rid},
        archived_rids={archive_search_context.archived_run.rid},
    )


# ---------------------------------------------------------------------------
# Asset search
# ---------------------------------------------------------------------------


def test_search_assets_by_name(client: NominalClient, search_context: SearchContext) -> None:
    """Searching assets by name substring returns only the asset whose name contains the session tag."""
    results = client.search_assets(search_text=search_context.tag)
    rids = {a.rid for a in results}
    assert rids == {search_context.asset.rid}


def test_search_assets_by_labels(client: NominalClient, search_context: SearchContext) -> None:
    """Filtering by a label narrows results to only the asset created with that label."""
    results = client.search_assets(labels=["search-test"], search_text=search_context.tag)
    rids = {a.rid for a in results}
    assert rids == {search_context.asset.rid}


def test_search_assets_by_properties(client: NominalClient, search_context: SearchContext) -> None:
    """Filtering by a key-value property returns only the asset that carries that property."""
    results = client.search_assets(properties={"search-tag": search_context.tag})
    rids = {a.rid for a in results}
    assert rids == {search_context.asset.rid}


def test_search_assets_archive_status(
    client: NominalClient,
    search_context: SearchContext,
    archive_search_context: ArchiveSearchContext,
) -> None:
    """Asset search honors archive_status filtering."""
    _assert_archive_status_behavior(
        lambda archive_status: client.search_assets(
            search_text=search_context.tag,
            archive_status=archive_status,
        ),
        active_rids={search_context.asset.rid},
        archived_rids={archive_search_context.archived_asset.rid},
    )


# ---------------------------------------------------------------------------
# Event search
# ---------------------------------------------------------------------------


def test_search_events_by_asset(client: NominalClient, search_context: SearchContext) -> None:
    """Searching events scoped to an asset returns all events attached to that asset."""
    results = client.search_events(assets=[search_context.asset])
    rids = {e.rid for e in results}
    assert rids == {search_context.event_info.rid, search_context.event_error.rid, search_context.event_flag.rid}


def test_search_events_by_event_type(client: NominalClient, search_context: SearchContext) -> None:
    """Filtering by event_type returns only events whose type matches."""
    results = client.search_events(event_type=EventType.INFO, assets=[search_context.asset])
    rids = {e.rid for e in results}
    assert rids == {search_context.event_info.rid}


def test_search_events_archive_status(
    client: NominalClient,
    archive_search_context: ArchiveSearchContext,
) -> None:
    """Event search honors archive_status filtering."""
    _assert_archive_status_behavior(
        lambda archive_status: client.search_events(
            assets=[archive_search_context.event_asset],
            archive_status=archive_status,
        ),
        active_rids={archive_search_context.active_archive_event.rid},
        archived_rids={archive_search_context.archived_archive_event.rid},
    )


def test_asset_search_events_archive_status(
    archive_search_context: ArchiveSearchContext,
) -> None:
    """Asset.search_events honors archive_status filtering."""
    _assert_archive_status_behavior(
        lambda archive_status: archive_search_context.event_asset.search_events(
            archive_status=archive_status,
        ),
        active_rids={archive_search_context.active_archive_event.rid},
        archived_rids={archive_search_context.archived_archive_event.rid},
    )


# ---------------------------------------------------------------------------
# Video search
# ---------------------------------------------------------------------------


def test_search_videos_by_name(client: NominalClient, search_context: SearchContext) -> None:
    """Searching videos by name substring returns only the video whose name contains the session tag."""
    results = client.search_videos(search_text=search_context.tag)
    rids = {v.rid for v in results}
    assert rids == {search_context.video.rid}


def test_search_videos_archive_status(
    client: NominalClient,
    search_context: SearchContext,
    archive_search_context: ArchiveSearchContext,
) -> None:
    """Video search honors archive_status filtering."""
    _assert_archive_status_behavior(
        lambda archive_status: client.search_videos(
            search_text=search_context.tag,
            archive_status=archive_status,
        ),
        active_rids={search_context.video.rid},
        archived_rids={archive_search_context.archived_video.rid},
    )


# ---------------------------------------------------------------------------
# Dataset search
# ---------------------------------------------------------------------------


def test_search_datasets_archive_status(
    client: NominalClient,
    search_context: SearchContext,
    archive_search_context: ArchiveSearchContext,
) -> None:
    """Dataset search honors archive_status filtering."""
    _assert_archive_status_behavior(
        lambda archive_status: client.search_datasets(
            search_text=search_context.tag,
            archive_status=archive_status,
        ),
        active_rids={search_context.dataset.rid},
        archived_rids={archive_search_context.archived_dataset.rid},
    )


# ---------------------------------------------------------------------------
# Secret search
# ---------------------------------------------------------------------------


def test_search_secrets_archive_status(
    client: NominalClient,
    search_context: SearchContext,
    archive_search_context: ArchiveSearchContext,
) -> None:
    """Secret search honors archive_status filtering."""
    _assert_archive_status_behavior(
        lambda archive_status: client.search_secrets(
            search_text=search_context.tag,
            archive_status=archive_status,
        ),
        active_rids={archive_search_context.active_secret.rid},
        archived_rids={archive_search_context.archived_secret.rid},
    )


# ---------------------------------------------------------------------------
# Workbook template search
# ---------------------------------------------------------------------------


def test_search_workbook_templates_archive_status(
    client: NominalClient,
    archive_search_context: ArchiveSearchContext,
) -> None:
    """Workbook template search honors archive_status filtering."""
    _assert_archive_status_behavior(
        lambda archive_status: client.search_workbook_templates(
            labels=[archive_search_context.template_tag],
            archive_status=archive_status,
        ),
        active_rids={archive_search_context.active_workbook_template.rid},
        archived_rids={archive_search_context.archived_workbook_template.rid},
    )


# ---------------------------------------------------------------------------
# Workbook search
# ---------------------------------------------------------------------------


def test_client_search_workbooks_archive_filters(
    client: NominalClient,
    search_context: SearchContext,
    archive_search_context: ArchiveSearchContext,
) -> None:
    """NominalClient.search_workbooks supports archive_status and include_archived filtering."""
    _assert_archive_status_behavior(
        lambda archive_status: client.search_workbooks(
            run=search_context.run,
            search_text=archive_search_context.run_workbook_tag,
            archive_status=archive_status,
        ),
        active_rids={archive_search_context.active_run_workbook.rid},
        archived_rids={archive_search_context.archived_run_workbook.rid},
    )
    _assert_include_archived_behavior(
        lambda include_archived: client.search_workbooks(
            run=search_context.run,
            search_text=archive_search_context.run_workbook_tag,
            include_archived=include_archived,
        ),
        active_rids={archive_search_context.active_run_workbook.rid},
        archived_rids={archive_search_context.archived_run_workbook.rid},
    )


def test_client_search_workbooks_include_drafts(
    client: NominalClient,
    search_context: SearchContext,
    archive_search_context: ArchiveSearchContext,
) -> None:
    """NominalClient.search_workbooks supports include_drafts filtering."""
    _assert_include_drafts_behavior(
        lambda include_drafts: client.search_workbooks(
            run=search_context.run,
            search_text=archive_search_context.run_workbook_tag,
            include_drafts=include_drafts,
        ),
        non_draft_rids={archive_search_context.active_run_workbook.rid},
        draft_rids={archive_search_context.draft_run_workbook.rid},
    )


def test_asset_search_workbooks_archive_filters(
    search_context: SearchContext,
    archive_search_context: ArchiveSearchContext,
) -> None:
    """Asset.search_workbooks supports archive_status and include_archived filtering."""
    _assert_archive_status_behavior(
        lambda archive_status: search_context.asset.search_workbooks(
            search_text=archive_search_context.asset_workbook_tag,
            archive_status=archive_status,
        ),
        active_rids={archive_search_context.active_asset_workbook.rid},
        archived_rids={archive_search_context.archived_asset_workbook.rid},
    )
    _assert_include_archived_behavior(
        lambda include_archived: search_context.asset.search_workbooks(
            search_text=archive_search_context.asset_workbook_tag,
            include_archived=include_archived,
        ),
        active_rids={archive_search_context.active_asset_workbook.rid},
        archived_rids={archive_search_context.archived_asset_workbook.rid},
    )


def test_asset_search_workbooks_include_drafts(
    search_context: SearchContext,
    archive_search_context: ArchiveSearchContext,
) -> None:
    """Asset.search_workbooks supports include_drafts filtering."""
    _assert_include_drafts_behavior(
        lambda include_drafts: search_context.asset.search_workbooks(
            search_text=archive_search_context.asset_workbook_tag,
            include_drafts=include_drafts,
        ),
        non_draft_rids={archive_search_context.active_asset_workbook.rid},
        draft_rids={archive_search_context.draft_asset_workbook.rid},
    )


def test_run_search_workbooks_archive_filters(
    search_context: SearchContext,
    archive_search_context: ArchiveSearchContext,
) -> None:
    """Run.search_workbooks supports archive_status and include_archived filtering."""
    _assert_archive_status_behavior(
        lambda archive_status: search_context.run.search_workbooks(
            search_text=archive_search_context.run_workbook_tag,
            archive_status=archive_status,
        ),
        active_rids={archive_search_context.active_run_workbook.rid},
        archived_rids={archive_search_context.archived_run_workbook.rid},
    )
    _assert_include_archived_behavior(
        lambda include_archived: search_context.run.search_workbooks(
            search_text=archive_search_context.run_workbook_tag,
            include_archived=include_archived,
        ),
        active_rids={archive_search_context.active_run_workbook.rid},
        archived_rids={archive_search_context.archived_run_workbook.rid},
    )


def test_run_search_workbooks_include_drafts(
    search_context: SearchContext,
    archive_search_context: ArchiveSearchContext,
) -> None:
    """Run.search_workbooks supports include_drafts filtering."""
    _assert_include_drafts_behavior(
        lambda include_drafts: search_context.run.search_workbooks(
            search_text=archive_search_context.run_workbook_tag,
            include_drafts=include_drafts,
        ),
        non_draft_rids={archive_search_context.active_run_workbook.rid},
        draft_rids={archive_search_context.draft_run_workbook.rid},
    )


# ---------------------------------------------------------------------------
# Dataset file search
# ---------------------------------------------------------------------------


def test_search_dataset_files_no_filter(client: NominalClient, search_context: SearchContext) -> None:
    """Listing dataset files with no filter arguments returns all files in the dataset."""
    results = client.search_dataset_files(search_context.dataset)
    ids = {f.id for f in results}
    assert ids == {search_context.file_jan.id, search_context.file_jun.id, search_context.file_dec.id}


def test_search_dataset_files_by_tags(client: NominalClient, search_context: SearchContext) -> None:
    """Multiple tag filters are AND-ed: source=alpha AND region=eu-west returns only the Dec file."""
    results = client.search_dataset_files(search_context.dataset, file_tags={"source": "alpha", "region": "eu-west"})
    ids = {f.id for f in results}
    assert ids == {search_context.file_dec.id}


def test_search_dataset_files_by_time_range(client: NominalClient, search_context: SearchContext) -> None:
    """A time range that fully contains only Jun 2024 returns only that file."""
    results = client.search_dataset_files(search_context.dataset, start="2024-03-01", end="2024-09-01")
    ids = {f.id for f in results}
    assert ids == {search_context.file_jun.id}


def test_search_dataset_files_combined_tag_and_time(client: NominalClient, search_context: SearchContext) -> None:
    """A tag filter and a start time are AND-ed: source=alpha after Jun 2024 returns only the Dec file."""
    results = client.search_dataset_files(search_context.dataset, start="2024-06-01", file_tags={"source": "alpha"})
    ids = {f.id for f in results}
    assert ids == {search_context.file_dec.id}


def test_search_dataset_files_start_uses_overlap_semantics(
    client: NominalClient, search_context: SearchContext
) -> None:
    """A file whose range starts before the search window but overlaps it is still included."""
    results = client.search_dataset_files(search_context.dataset, start="2024-01-01T00:30:00Z")
    ids = {f.id for f in results}
    assert ids == {search_context.file_jan.id, search_context.file_jun.id, search_context.file_dec.id}


def test_search_dataset_files_end_uses_overlap_semantics(client: NominalClient, search_context: SearchContext) -> None:
    """A file whose range ends after the search window but overlaps it is still included."""
    results = client.search_dataset_files(search_context.dataset, end="2024-12-01T00:30:00Z")
    ids = {f.id for f in results}
    assert ids == {search_context.file_jan.id, search_context.file_jun.id, search_context.file_dec.id}


def test_search_dataset_files_no_match(client: NominalClient, search_context: SearchContext) -> None:
    """A search window entirely beyond all file ranges returns an empty result."""
    results = client.search_dataset_files(search_context.dataset, start="2030-01-01")
    assert list(results) == []
