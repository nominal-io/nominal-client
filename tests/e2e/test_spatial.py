"""End-to-end tests for spatial assets and the point-cloud ingest pipeline.

Run against a local scout:

    uv run pytest tests/e2e/test_spatial.py --profile <profile> -v
    uv run pytest tests/e2e/test_spatial.py --auth-token <token> -v   # defaults to api.nominal.test

On macOS a local scout also needs the mkcert root passed explicitly, since the gRPC
transport cannot read the Keychain:

    --trust-store-path "$(mkcert -CAROOT)/rootCA.pem"

The point-cloud ingest tests need scout built from a branch that carries
`PointCloudOpts.daggerImportConfig` AND a running Dagger microservice.

Dagger being unreachable is NOT detected up front: scout accepts the ingest and the job
parks at IN_PROGRESS rather than failing, so `_ingest`'s skip guard never fires and each
polling test burns the full INGEST_TIMEOUT before failing. The guard only catches scout
refusing outright ("Point-cloud ingestion is disabled because Dagger is not configured"),
which is what a scout with no Dagger *configured* answers. Dagger also needs its
per-tenant bucket (``dag-tenant-<org-uuid>``) to exist in the object store, or every
import fails with NoSuchBucket and the job never leaves IN_PROGRESS.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator
from uuid import UUID, uuid4

import pytest

from nominal.core import NominalClient
from nominal.core.ingestion_job import IngestionJobStatus
from nominal.core.spatial_asset import PointCloudMetadata, ScanPattern, SpatialAsset

DAGGER_UNAVAILABLE = "Dagger is not configured"

TERMINAL_STATUSES = frozenset({IngestionJobStatus.COMPLETED, IngestionJobStatus.FAILED, IngestionJobStatus.CANCELLED})

# Dagger's import is a real out-of-process indexing job, so allow considerably more
# headroom than the tabular ingest tests, which only wait on the refinery.
INGEST_TIMEOUT = timedelta(minutes=5)
INGEST_POLL_INTERVAL = timedelta(seconds=2)


@pytest.fixture(scope="session")
def point_cloud_csv(tmp_path_factory) -> Path:
    """A small point cloud exercising every inferred column type.

    `count` is int, `stress` is real (its first row is integer-valued, so this also
    covers sample-based promotion), and `label` is a string.
    """
    rows = ["x,y,z,count,stress,label"]
    for i in range(200):
        stress = 1 if i == 0 else round(0.5 + i / 1000, 4)
        rows.append(f"{i * 0.1},{i * 0.2},{i * 0.3},{i},{stress},pt{i % 4}")
    path = tmp_path_factory.mktemp("spatial") / "cloud.csv"
    path.write_text("\n".join(rows) + "\n")
    return path


@pytest.fixture
def spatial_asset(client: NominalClient) -> Iterator[SpatialAsset]:
    asset = client.create_spatial_asset(
        f"e2e-spatial-{uuid4().hex[:8]}",
        metadata=PointCloudMetadata(sensor_model="Ouster OS1-128", scan_pattern=ScanPattern.ROTATING),
    )
    yield asset
    asset.archive()


def _poll_ingest_job(client: NominalClient, rid: str) -> IngestionJobStatus:
    """Block until the ingest job reaches a terminal status, or fail on timeout."""
    deadline = time.monotonic() + INGEST_TIMEOUT.total_seconds()
    job = client.get_ingestion_job(rid)
    while job.status not in TERMINAL_STATUSES:
        if time.monotonic() > deadline:
            pytest.fail(f"ingest job {rid} still {job.status} after {INGEST_TIMEOUT}")
        time.sleep(INGEST_POLL_INTERVAL.total_seconds())
        job = job.refresh()
    return job.status


def _ingest(asset: SpatialAsset, csv_path: Path, **kwargs) -> str:
    """Submit a point-cloud ingest, skipping the test if Dagger is not configured."""
    try:
        job_rid = asset.ingest_point_cloud_csv(csv_path, **kwargs)
    except Exception as e:
        if DAGGER_UNAVAILABLE in str(e):
            pytest.skip("scout has no Dagger configured; point-cloud ingest unavailable")
        raise
    assert job_rid is not None, "scout returned no ingest job rid"
    return job_rid


# --- spatial asset metadata (no Dagger required) ------------------------------


def test_create_spatial_asset_round_trips(client: NominalClient, spatial_asset: SpatialAsset) -> None:
    """A created asset is readable by rid with its metadata intact."""
    fetched = client.get_spatial_asset(spatial_asset.rid)
    assert fetched.rid == spatial_asset.rid
    assert fetched.name == spatial_asset.name
    assert fetched.metadata == PointCloudMetadata(sensor_model="Ouster OS1-128", scan_pattern=ScanPattern.ROTATING)
    # The client reserves the model uuid; scout must persist it verbatim.
    assert UUID(fetched.dagger_uuid) == UUID(spatial_asset.dagger_uuid)
    # Nothing has been ingested yet, so there is no source to point at.
    assert fetched.start_timestamp is None


def test_create_spatial_asset_persists_time_bounds(client: NominalClient) -> None:
    """start/end timestamps survive a create -> fetch round trip."""
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 2, tzinfo=timezone.utc)
    asset = client.create_spatial_asset(
        f"e2e-spatial-ts-{uuid4().hex[:8]}",
        metadata=PointCloudMetadata(),
        start_timestamp=start,
        end_timestamp=end,
    )
    try:
        fetched = client.get_spatial_asset(asset.rid)
        assert fetched.start_timestamp == int(start.timestamp()) * 1_000_000_000
        assert fetched.end_timestamp == int(end.timestamp()) * 1_000_000_000
    finally:
        asset.archive()


def test_update_spatial_asset_metadata(client: NominalClient, spatial_asset: SpatialAsset) -> None:
    """update() replaces only the fields passed and refreshes in place."""
    spatial_asset.update(
        name="renamed-scan",
        description="updated",
        labels=["lidar", "e2e"],
        properties={"site": "downtown"},
    )
    assert spatial_asset.name == "renamed-scan"

    fetched = client.get_spatial_asset(spatial_asset.rid)
    assert fetched.name == "renamed-scan"
    assert fetched.description == "updated"
    assert set(fetched.labels) == {"lidar", "e2e"}
    assert fetched.properties["site"] == "downtown"
    # Untouched fields survive the partial update.
    assert fetched.metadata.sensor_model == "Ouster OS1-128"


def test_archive_and_unarchive(client: NominalClient) -> None:
    asset = client.create_spatial_asset(f"e2e-spatial-arch-{uuid4().hex[:8]}", metadata=PointCloudMetadata())
    try:
        asset.archive()
        assert client.get_spatial_asset(asset.rid).is_archived is True
        asset.unarchive()
        assert client.get_spatial_asset(asset.rid).is_archived is False
    finally:
        asset.archive()


# --- point cloud ingest (requires Dagger) -------------------------------------


def test_point_cloud_ingest_completes(
    client: NominalClient, spatial_asset: SpatialAsset, point_cloud_csv: Path
) -> None:
    """The full pipeline: scout accepts our daggerImportConfig and the import completes.

    This is the load-bearing test for the migration. Scout deserializes
    `daggerImportConfig` with FAIL_ON_UNKNOWN_PROPERTIES, so any drift in the v2 wire
    shape (a stray field, a lowercased enum, geometry_type left at the top level)
    fails the request outright rather than degrading quietly.
    """
    job_rid = _ingest(spatial_asset, point_cloud_csv)
    status = _poll_ingest_job(client, job_rid)
    assert status == IngestionJobStatus.COMPLETED, f"ingest job {job_rid} ended {status}"


def test_point_cloud_ingest_records_source_handle(
    client: NominalClient, spatial_asset: SpatialAsset, point_cloud_csv: Path
) -> None:
    """Provenance is written back to the asset after the upload."""
    assert spatial_asset.source_handle is None, "nothing uploaded yet"
    _ingest(spatial_asset, point_cloud_csv)
    # Set during ingest, since the object location is not knowable at create time.
    source_handle = client.get_spatial_asset(spatial_asset.rid).source_handle
    assert source_handle is not None
    assert source_handle.endswith(".csv")


def test_point_cloud_ingest_accepts_column_type_overrides(
    client: NominalClient, spatial_asset: SpatialAsset, point_cloud_csv: Path
) -> None:
    """An explicit column_types override produces a config scout still accepts."""
    job_rid = _ingest(spatial_asset, point_cloud_csv, column_types={"count": "real"})
    status = _poll_ingest_job(client, job_rid)
    assert status == IngestionJobStatus.COMPLETED, f"ingest job {job_rid} ended {status}"


def test_ingest_into_archived_asset_is_accepted(
    client: NominalClient, spatial_asset: SpatialAsset, point_cloud_csv: Path
) -> None:
    """Archiving does not make an asset read-only: scout still accepts an ingest into it.

    Archive is a search-visibility flag, not a write lock -- the same reason it is reversible
    via `unarchive`. Pinned because the opposite is the intuitive guess, and an ingest that
    lands on a hidden asset is easy to mistake for a silent no-op.
    """
    spatial_asset.archive()
    assert _ingest(spatial_asset, point_cloud_csv) is not None
    # The upload really landed, rather than being dropped on the floor.
    assert client.get_spatial_asset(spatial_asset.rid).source_handle is not None


def test_ingest_rejects_csv_without_geometry(spatial_asset: SpatialAsset, tmp_path: Path) -> None:
    """Client-side validation fires before anything is uploaded."""
    bad = tmp_path / "no_geometry.csv"
    bad.write_text("a,b,c\n1,2,3\n")
    with pytest.raises(ValueError, match="missing required point-cloud columns"):
        spatial_asset.ingest_point_cloud_csv(bad)


# --- spatial assets as run / asset data scopes --------------------------------


def test_spatial_asset_as_run_data_scope(client: NominalClient, spatial_asset: SpatialAsset) -> None:
    """add_spatial / get_spatial / list_spatials round-trip on a Run."""
    start = datetime.now(timezone.utc)
    run = client.create_run(f"e2e-spatial-run-{uuid4().hex[:8]}", start=start, end=start + timedelta(hours=1))
    try:
        run.add_spatial("cloud", spatial_asset)
        assert run.get_spatial("cloud").rid == spatial_asset.rid
        assert [(name, a.rid) for name, a in run.list_spatials()] == [("cloud", spatial_asset.rid)]
    finally:
        run.archive()


def test_run_get_spatial_unknown_ref_name_raises(client: NominalClient, spatial_asset: SpatialAsset) -> None:
    start = datetime.now(timezone.utc)
    run = client.create_run(f"e2e-spatial-run-{uuid4().hex[:8]}", start=start, end=start + timedelta(hours=1))
    try:
        run.add_spatial("cloud", spatial_asset)
        with pytest.raises(ValueError, match="No spatial asset with ref name"):
            run.get_spatial("nope")
    finally:
        run.archive()


def test_spatial_asset_as_asset_data_scope(client: NominalClient, spatial_asset: SpatialAsset) -> None:
    """add_spatial / get_spatial / list_spatials round-trip on an Asset."""
    asset = client.create_asset(f"e2e-spatial-asset-{uuid4().hex[:8]}")
    try:
        asset.add_spatial("cloud", spatial_asset)
        assert asset.get_spatial("cloud").rid == spatial_asset.rid
        assert [(name, a.rid) for name, a in asset.list_spatials()] == [("cloud", spatial_asset.rid)]
        # Spatial scopes must also show up in the combined data-scope listing.
        assert spatial_asset.rid in [scope.rid for _, scope in asset.list_data_scopes()]
    finally:
        asset.archive()
