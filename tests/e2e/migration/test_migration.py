r"""End-to-end tests for the migration library via MigrationRunner.

All tests go through MigrationRunner (the top-level entry point), mirroring
real-world usage. Direct use of AssetMigrator/DatasetMigrator belongs in unit tests.

Tests cover:
  - Full migration of an asset with datasets, events, a run, a checklist, and a video
  - Dataset file upload and channel verification
  - Idempotency: running the same runner twice produces no duplicates
  - Resumption from a partial state: missing resources are created, existing ones reused
  - Resumption from a complete state: a fully-recorded state causes the runner to do nothing

Run with:
    uv run pytest tests/e2e/migration/ \
        --source-profile=<prod> --profile=<staging> -v
"""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Callable
from uuid import uuid4

from nominal.core import NominalClient
from nominal.core._event_types import EventType
from nominal.core.asset import Asset
from nominal.core.checklist import Checklist
from nominal.core.dataset import Dataset
from nominal.core.event import Event
from nominal.core.run import Run
from nominal.core.video import Video
from nominal.experimental.checklist_utils.checklist_utils import _create_checklist_with_content
from nominal.experimental.migration.config.migration_data_config import MigrationDatasetConfig
from nominal.experimental.migration.config.migration_resources import AssetResources, MigrationResources
from nominal.experimental.migration.migration_runner import MigrationRunner
from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.resource_type import ResourceType
from tests.e2e import POLL_INTERVAL, _create_random_start_end

ArchiveFn = Callable[[object], None]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_resources(*assets: Asset) -> MigrationResources:
    """Wrap one or more source assets into a MigrationResources with no standalone templates."""
    return MigrationResources(
        source_assets={a.rid: AssetResources(asset=a, source_workbook_templates=[]) for a in assets},
        source_standalone_templates=[],
    )


def _no_files_config() -> MigrationDatasetConfig:
    return MigrationDatasetConfig(preserve_dataset_uuid=False, include_dataset_files=False)


def _with_files_config() -> MigrationDatasetConfig:
    return MigrationDatasetConfig(preserve_dataset_uuid=False, include_dataset_files=True)


def _make_runner(
    resources: MigrationResources,
    config: MigrationDatasetConfig,
    dest_client: NominalClient,
    state_path: Path,
) -> MigrationRunner:
    return MigrationRunner(
        migration_resources=resources,
        dataset_config=config,
        destination_client=dest_client,
        migration_state_path=state_path,
    )


def _dest_asset(runner: MigrationRunner, source_asset: Asset, dest_client: NominalClient) -> Asset:
    rid = runner.migration_state.get_mapped_rid(ResourceType.ASSET, source_asset.rid)
    assert rid is not None, f"No dest asset RID in migration state for source {source_asset.rid}"
    return dest_client.get_asset(rid)


def _setup_source_resources(source_client: NominalClient, source_archive: ArchiveFn, mp4_data: bytes):
    """Create a source asset with one of every resource type, each populated with labels,
    properties, and descriptions to exercise full metadata migration fidelity.
    """
    start, end = _create_random_start_end()
    tag = uuid4()

    source_asset = source_client.create_asset(
        f"migration-e2e-asset-{tag}",
        description=f"asset-description-{tag}",
        properties={"asset-prop": "asset-val"},
        labels=["migration-e2e"],
    )
    source_archive(source_asset)

    source_ds = source_client.create_dataset(
        f"migration-e2e-ds-{tag}",
        description=f"ds-description-{tag}",
        properties={"ds-prop": "ds-val"},
        labels=["migration-e2e"],
    )
    source_archive(source_ds)
    source_asset.add_dataset("primary", source_ds)

    event_a = source_client.create_event(
        f"migration-e2e-event-a-{tag}",
        EventType.FLAG,
        start,
        assets=[source_asset],
        description=f"event-a-description-{tag}",
        properties={"event-prop": "flag"},
        labels=["migration-e2e"],
    )
    source_archive(event_a)
    event_b = source_client.create_event(
        f"migration-e2e-event-b-{tag}",
        EventType.INFO,
        start,
        assets=[source_asset],
        description=f"event-b-description-{tag}",
        properties={"event-prop": "info"},
        labels=["migration-e2e"],
    )
    source_archive(event_b)

    source_run = source_client.create_run(
        f"migration-e2e-run-{tag}",
        start,
        end,
        assets=[source_asset],
        description=f"run-description-{tag}",
        properties={"run-prop": "run-val"},
        labels=["migration-e2e"],
    )
    source_archive(source_run)

    source_checklist = _create_checklist_with_content(
        source_client, title=f"migration-e2e-checklist-{tag}", is_published=True
    )
    source_archive(source_checklist)
    source_data_review = source_checklist.execute(source_run)
    source_archive(source_data_review)

    source_video = source_client.create_video(
        f"migration-e2e-video-{tag}",
        description=f"video-description-{tag}",
        properties={"video-prop": "video-val"},
        labels=["migration-e2e"],
    )
    source_archive(source_video)
    source_video.add_from_io(BytesIO(mp4_data), "test.mp4", start=start).poll_until_ingestion_completed(
        interval=POLL_INTERVAL
    )
    source_asset.add_video("camera", source_video)

    return source_asset, source_ds, event_a, event_b, source_run, source_checklist, source_data_review, source_video


# ---------------------------------------------------------------------------
# Per-resource assertion helpers
# ---------------------------------------------------------------------------


def _assert_asset_fields(source: Asset, dest: Asset) -> None:
    assert dest.name == source.name
    assert dest.description == source.description
    assert set(dest.labels) == set(source.labels)
    assert dest.properties == source.properties


def _assert_dataset_fields(source: Dataset, dest: Dataset, scope_name: str, dest_asset: Asset) -> None:
    assert dest.name == source.name
    assert dest.description == source.description
    assert set(dest.labels) == set(source.labels)
    assert dest.properties == source.properties
    # Linkage: dataset is accessible under the expected scope on the destination asset.
    dest_datasets = dict(dest_asset.list_datasets())
    assert scope_name in dest_datasets
    assert dest_datasets[scope_name].rid == dest.rid


def _assert_event_fields(source: Event, dest: Event, dest_asset: Asset) -> None:
    assert dest.name == source.name
    assert dest.type == source.type
    assert dest.description == source.description
    assert set(dest.labels) == set(source.labels)
    assert dest.properties == source.properties
    # Linkage: event is associated with the destination asset.
    assert dest.rid in {e.rid for e in dest_asset.search_events()}


def _assert_run_fields(source: Run, dest: Run, dest_asset: Asset) -> None:
    assert dest.name == source.name
    assert dest.description == source.description
    assert set(dest.labels) == set(source.labels)
    assert dest.properties == source.properties
    assert dest.start == source.start
    assert dest.end == source.end
    # Linkage: bidirectional — run appears on the destination asset and the run references it.
    assert dest.rid in {r.rid for r in dest_asset.list_runs()}
    assert dest_asset.rid in dest.assets


def _assert_checklist_fields(source: Checklist, dest: Checklist) -> None:
    assert dest.name == source.name
    assert dest.description == source.description
    assert set(dest.labels) == set(source.labels)
    assert dest.properties == source.properties


def _assert_video_fields(source: Video, dest: Video, scope_name: str, dest_asset: Asset) -> None:
    assert dest.name == source.name
    assert dest.description == source.description
    assert set(dest.labels) == set(source.labels)
    assert dest.properties == source.properties
    # Linkage: video is accessible under the expected scope on the destination asset.
    dest_videos = dict(dest_asset.list_videos())
    assert scope_name in dest_videos
    assert dest_videos[scope_name].rid == dest.rid


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_migrate_asset(
    source_client: NominalClient,
    dest_client: NominalClient,
    source_archive: ArchiveFn,
    dest_archive: ArchiveFn,
    mp4_data: bytes,
    tmp_path: Path,
):
    """Full migration of an asset covering all resource types: dataset, events, run, checklist, and video.

    Verifies:
    - All child resources exist on the destination with RID mappings in state
    - Metadata (name, description, labels, properties) is preserved for each resource
    - Resources are correctly linked to the migrated destination asset
    """
    # --- source setup ---
    source_asset, source_ds, event_a, event_b, source_run, source_checklist, source_data_review, source_video = (
        _setup_source_resources(source_client, source_archive, mp4_data)
    )

    # --- migrate ---
    runner = _make_runner(_make_resources(source_asset), _no_files_config(), dest_client, tmp_path / "state.json")
    runner.run_migration()
    state = runner.migration_state

    dest_asset = _dest_asset(runner, source_asset, dest_client)
    dest_archive(dest_asset)

    # --- asset ---
    _assert_asset_fields(source_asset, dest_asset)

    # --- dataset ---
    dest_ds_rid = state.get_mapped_rid(ResourceType.DATASET, source_ds.rid)
    assert dest_ds_rid is not None
    dest_ds = dest_client.get_dataset(dest_ds_rid)
    dest_archive(dest_ds)
    _assert_dataset_fields(source_ds, dest_ds, "primary", dest_asset)

    # --- events ---
    dest_event_a_rid = state.get_mapped_rid(ResourceType.EVENT, event_a.rid)
    dest_event_b_rid = state.get_mapped_rid(ResourceType.EVENT, event_b.rid)
    assert dest_event_a_rid is not None
    assert dest_event_b_rid is not None
    dest_event_a = dest_client.get_event(dest_event_a_rid)
    dest_event_b = dest_client.get_event(dest_event_b_rid)
    _assert_event_fields(event_a, dest_event_a, dest_asset)
    _assert_event_fields(event_b, dest_event_b, dest_asset)

    # --- run ---
    dest_run_rid = state.get_mapped_rid(ResourceType.RUN, source_run.rid)
    assert dest_run_rid is not None
    dest_run = dest_client.get_run(dest_run_rid)
    dest_archive(dest_run)
    _assert_run_fields(source_run, dest_run, dest_asset)

    # --- checklist + data review ---
    dest_checklist_rid = state.get_mapped_rid(ResourceType.CHECKLIST, source_checklist.rid)
    assert dest_checklist_rid is not None
    dest_checklist = dest_client.get_checklist(dest_checklist_rid)
    dest_archive(dest_checklist)
    _assert_checklist_fields(source_checklist, dest_checklist)
    assert state.get_mapped_rid(ResourceType.DATA_REVIEW, source_data_review.rid) is not None

    # --- video ---
    dest_video_rid = state.get_mapped_rid(ResourceType.VIDEO, source_video.rid)
    assert dest_video_rid is not None
    dest_video = dest_client.get_video(dest_video_rid)
    dest_archive(dest_video)
    _assert_video_fields(source_video, dest_video, "camera", dest_asset)


def test_migrate_asset_with_dataset_files(
    source_client: NominalClient,
    dest_client: NominalClient,
    source_archive: ArchiveFn,
    dest_archive: ArchiveFn,
    csv_data: bytes,
    tmp_path: Path,
):
    """Dataset files are copied and ingested on the destination when include_dataset_files=True."""
    source_asset = source_client.create_asset(f"migration-e2e-files-asset-{uuid4()}")
    source_archive(source_asset)
    source_ds = source_client.create_dataset(f"migration-e2e-files-ds-{uuid4()}")
    source_archive(source_ds)
    source_ds.add_from_io(BytesIO(csv_data), "timestamp", "iso_8601").poll_until_ingestion_completed(
        interval=POLL_INTERVAL
    )
    source_asset.add_dataset("primary", source_ds)

    runner = _make_runner(_make_resources(source_asset), _with_files_config(), dest_client, tmp_path / "state.json")
    runner.run_migration()

    dest_asset = _dest_asset(runner, source_asset, dest_client)
    dest_archive(dest_asset)
    dest_ds_rid = runner.migration_state.get_mapped_rid(ResourceType.DATASET, source_ds.rid)
    assert dest_ds_rid is not None
    dest_ds = dest_client.get_dataset(dest_ds_rid)
    dest_archive(dest_ds)

    dest_files = list(dest_ds.list_files())
    assert len(dest_files) == 1
    dest_files[0].poll_until_ingestion_completed(interval=POLL_INTERVAL)

    source_channels = {ch.name for ch in source_ds.search_channels()}
    dest_channels = {ch.name for ch in dest_ds.search_channels()}
    assert source_channels.issubset(dest_channels)


def test_migration_idempotency(
    source_client: NominalClient,
    dest_client: NominalClient,
    source_archive: ArchiveFn,
    dest_archive: ArchiveFn,
    tmp_path: Path,
):
    """Running run_migration() twice on the same runner creates no duplicate resources."""
    source_asset = source_client.create_asset(f"migration-e2e-idempotent-asset-{uuid4()}")
    source_archive(source_asset)
    source_ds = source_client.create_dataset(f"migration-e2e-idempotent-ds-{uuid4()}")
    source_archive(source_ds)
    source_asset.add_dataset("primary", source_ds)

    runner = _make_runner(_make_resources(source_asset), _no_files_config(), dest_client, tmp_path / "state.json")

    runner.run_migration()
    dest_asset_rid_1 = runner.migration_state.get_mapped_rid(ResourceType.ASSET, source_asset.rid)
    dest_ds_rid_1 = runner.migration_state.get_mapped_rid(ResourceType.DATASET, source_ds.rid)
    dest_archive(dest_client.get_asset(dest_asset_rid_1))
    dest_archive(dest_client.get_dataset(dest_ds_rid_1))

    runner.run_migration()
    assert runner.migration_state.get_mapped_rid(ResourceType.ASSET, source_asset.rid) == dest_asset_rid_1
    assert runner.migration_state.get_mapped_rid(ResourceType.DATASET, source_ds.rid) == dest_ds_rid_1


def test_resume_partial_migration(
    source_client: NominalClient,
    dest_client: NominalClient,
    source_archive: ArchiveFn,
    dest_archive: ArchiveFn,
    tmp_path: Path,
):
    """Resuming from a partial state completes the migration without duplicating already-migrated resources.

    Simulates a crash mid-migration: ds1 was already created on the destination and recorded
    in the state file, but ds2 and the asset itself were not. The runner must create the asset
    and ds2 fresh, reuse the pre-existing ds1 (no duplicate), and link both to the new asset.
    """
    source_asset = source_client.create_asset(f"migration-e2e-partial-asset-{uuid4()}")
    source_archive(source_asset)
    source_ds1 = source_client.create_dataset(f"migration-e2e-partial-ds1-{uuid4()}")
    source_archive(source_ds1)
    source_ds2 = source_client.create_dataset(f"migration-e2e-partial-ds2-{uuid4()}")
    source_archive(source_ds2)
    source_asset.add_dataset("primary", source_ds1)
    source_asset.add_dataset("secondary", source_ds2)

    # Simulate a previous run: ds1 already exists on dest, state file was written, then crash.
    pre_dest_ds1 = dest_client.create_dataset(f"migration-e2e-partial-ds1-pre-{uuid4()}")
    dest_archive(pre_dest_ds1)
    partial_state = MigrationState(rid_mapping={ResourceType.DATASET.value: {source_ds1.rid: pre_dest_ds1.rid}})
    state_file = tmp_path / "state.json"
    state_file.write_text(partial_state.to_json(), encoding="utf-8")

    runner = _make_runner(_make_resources(source_asset), _no_files_config(), dest_client, state_file)
    runner.run_migration()

    dest_asset = _dest_asset(runner, source_asset, dest_client)
    dest_archive(dest_asset)
    dest_ds2_rid = runner.migration_state.get_mapped_rid(ResourceType.DATASET, source_ds2.rid)
    assert dest_ds2_rid is not None
    dest_archive(dest_client.get_dataset(dest_ds2_rid))

    # ds1 was reused — no duplicate created.
    assert runner.migration_state.get_mapped_rid(ResourceType.DATASET, source_ds1.rid) == pre_dest_ds1.rid

    # Both scopes are linked on the destination asset.
    dest_datasets = dict(dest_asset.list_datasets())
    assert "primary" in dest_datasets
    assert "secondary" in dest_datasets
    assert dest_datasets["primary"].rid == pre_dest_ds1.rid


def test_resume_complete_migration(
    source_client: NominalClient,
    dest_client: NominalClient,
    source_archive: ArchiveFn,
    dest_archive: ArchiveFn,
    tmp_path: Path,
):
    """Resuming from a complete state file creates nothing new.

    Runs the full migration with one runner, then creates a second runner pointed at the
    same state file. The second run must return identical RIDs for every resource.
    """
    source_asset = source_client.create_asset(f"migration-e2e-complete-asset-{uuid4()}")
    source_archive(source_asset)
    source_ds1 = source_client.create_dataset(f"migration-e2e-complete-ds1-{uuid4()}")
    source_archive(source_ds1)
    source_ds2 = source_client.create_dataset(f"migration-e2e-complete-ds2-{uuid4()}")
    source_archive(source_ds2)
    source_asset.add_dataset("primary", source_ds1)
    source_asset.add_dataset("secondary", source_ds2)

    resources = _make_resources(source_asset)
    config = _no_files_config()
    state_file = tmp_path / "state.json"

    # First run: full migration.
    first_runner = _make_runner(resources, config, dest_client, state_file)
    first_runner.run_migration()
    dest_asset_rid = first_runner.migration_state.get_mapped_rid(ResourceType.ASSET, source_asset.rid)
    dest_ds1_rid = first_runner.migration_state.get_mapped_rid(ResourceType.DATASET, source_ds1.rid)
    dest_ds2_rid = first_runner.migration_state.get_mapped_rid(ResourceType.DATASET, source_ds2.rid)
    assert dest_asset_rid and dest_ds1_rid and dest_ds2_rid
    dest_archive(dest_client.get_asset(dest_asset_rid))
    dest_archive(dest_client.get_dataset(dest_ds1_rid))
    dest_archive(dest_client.get_dataset(dest_ds2_rid))

    # Second run: loads the complete state file, should create nothing.
    # MigrationRunner increments the output path to state_v2.json but reuses the loaded state.
    second_runner = _make_runner(resources, config, dest_client, state_file)
    second_runner.run_migration()

    assert second_runner.migration_state.get_mapped_rid(ResourceType.ASSET, source_asset.rid) == dest_asset_rid
    assert second_runner.migration_state.get_mapped_rid(ResourceType.DATASET, source_ds1.rid) == dest_ds1_rid
    assert second_runner.migration_state.get_mapped_rid(ResourceType.DATASET, source_ds2.rid) == dest_ds2_rid

    dest_asset = dest_client.get_asset(dest_asset_rid)
    dest_datasets = dict(dest_asset.list_datasets())
    assert "primary" in dest_datasets
    assert "secondary" in dest_datasets
    assert dest_datasets["primary"].rid == dest_ds1_rid
    assert dest_datasets["secondary"].rid == dest_ds2_rid
