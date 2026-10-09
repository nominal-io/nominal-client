"""End-to-end tests for spatials and the point-cloud ingest pipeline.

Run against a local backend:

    uv run pytest tests/e2e/test_spatial.py --profile <profile> -v
    uv run pytest tests/e2e/test_spatial.py --auth-token <token> -v   # defaults to api.nominal.test

On macOS a local backend also needs the mkcert root passed explicitly, since the gRPC
transport cannot read the Keychain:

    --trust-store-path "$(mkcert -CAROOT)/rootCA.pem"

The point-cloud ingest tests need a backend that supports
`PointCloudOpts.daggerImportConfig` AND a running point-cloud indexing service.

An unreachable indexing service is NOT detected up front: the backend accepts the ingest
and the job parks at IN_PROGRESS rather than failing, so `_ingest`'s skip guard never
fires and each polling test burns the full INGEST_TIMEOUT before failing. The guard only
catches the backend refusing outright ("Point-cloud ingestion is disabled because Dagger
is not configured"), which is what a backend with no indexing service *configured*
answers. The indexing service also needs its per-tenant bucket (``dag-tenant-<org-uuid>``)
to exist in the object store, or every import fails with NoSuchBucket and the job never
leaves IN_PROGRESS.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator
from uuid import UUID, uuid4

import pytest

from nominal.core import NominalClient
from nominal.core.ingestion_job import IngestionJob, IngestionJobStatus
from nominal.experimental.spatial import (
    PointCloudMetadata,
    ScanPattern,
    Spatial,
    add_spatial_to_asset,
    add_spatial_to_run,
    create_point_cloud_spatial,
    get_spatial,
    get_spatial_from_asset,
    get_spatial_from_run,
    list_spatials_in_asset,
    list_spatials_in_run,
)

DAGGER_UNAVAILABLE = "Dagger is not configured"

TERMINAL_STATUSES = frozenset({IngestionJobStatus.COMPLETED, IngestionJobStatus.FAILED, IngestionJobStatus.CANCELLED})

# The point-cloud import is a real out-of-process indexing job, so allow considerably
# more headroom than the tabular ingest tests.
INGEST_TIMEOUT = timedelta(minutes=5)
INGEST_POLL_INTERVAL = timedelta(seconds=2)


@pytest.fixture(scope="session")
def point_cloud_csv(tmp_path_factory) -> Path:
    """A small point cloud exercising every inferred column type.

    `count` and `stress` are numeric, so both infer as real, and `label` is a string.
    `count` is integer-valued, which is what the column_types override test asks for
    as Int.
    """
    rows = ["x,y,z,count,stress,label"]
    for i in range(200):
        stress = 1 if i == 0 else round(0.5 + i / 1000, 4)
        rows.append(f"{i * 0.1},{i * 0.2},{i * 0.3},{i},{stress},pt{i % 4}")
    path = tmp_path_factory.mktemp("spatial") / "cloud.csv"
    path.write_text("\n".join(rows) + "\n")
    return path


@pytest.fixture
def spatial(client: NominalClient) -> Iterator[Spatial]:
    asset = create_point_cloud_spatial(
        client,
        f"e2e-spatial-{uuid4().hex[:8]}",
        metadata=PointCloudMetadata(sensor_model="Ouster OS1-128", scan_pattern=ScanPattern.ROTATING),
    )
    yield asset
    asset.archive()


def _poll_ingest_job(job: IngestionJob) -> IngestionJobStatus:
    """Block until the ingest job reaches a terminal status, or fail on timeout."""
    deadline = time.monotonic() + INGEST_TIMEOUT.total_seconds()
    while job.status not in TERMINAL_STATUSES:
        if time.monotonic() > deadline:
            pytest.fail(f"ingest job {job.rid} still {job.status} after {INGEST_TIMEOUT}")
        time.sleep(INGEST_POLL_INTERVAL.total_seconds())
        job = job.refresh()
    return job.status


def _ingest(spatial: Spatial, csv_path: Path, **kwargs) -> IngestionJob:
    """Submit a point-cloud ingest, skipping the test if the indexing service is unavailable."""
    try:
        return spatial.add_point_cloud_csv(csv_path, **kwargs)
    except Exception as e:
        if DAGGER_UNAVAILABLE in str(e):
            pytest.skip("the backend has no indexing service configured; point-cloud ingest unavailable")
        raise


# --- spatial metadata (no indexing service required) --------------------------


def test_create_spatial_round_trips(client: NominalClient, spatial: Spatial) -> None:
    """A created spatial is readable by rid with its metadata intact."""
    fetched = get_spatial(client, spatial.rid)
    assert fetched.rid == spatial.rid
    assert fetched.name == spatial.name
    assert fetched.metadata == PointCloudMetadata(sensor_model="Ouster OS1-128", scan_pattern=ScanPattern.ROTATING)
    # The client reserves the model uuid; the backend must persist it verbatim.
    assert UUID(fetched.dagger_uuid) == UUID(spatial.dagger_uuid)
    # Nothing has been ingested yet, so there is no source to point at.
    assert fetched.start_timestamp is None


def test_update_round_trips_every_field_and_leaves_the_rest_alone(client: NominalClient, spatial: Spatial) -> None:
    """Everything update() writes survives a fetch, and what it does not name is left untouched."""
    spatial.update(
        name="renamed-scan",
        description="updated",
        labels=["lidar", "e2e"],
        properties={"site": "downtown"},
    )
    assert spatial.name == "renamed-scan"

    fetched = get_spatial(client, spatial.rid)
    assert fetched.name == "renamed-scan"
    assert fetched.description == "updated"
    assert set(fetched.labels) == {"lidar", "e2e"}
    assert fetched.properties["site"] == "downtown"
    # Untouched fields survive the partial update.
    assert fetched.metadata.sensor_model == "Ouster OS1-128"


def test_archive_and_unarchive(client: NominalClient) -> None:
    """Archiving hides a spatial from search and unarchiving brings it back."""
    asset = create_point_cloud_spatial(client, f"e2e-spatial-arch-{uuid4().hex[:8]}", metadata=PointCloudMetadata())
    try:
        asset.archive()
        assert get_spatial(client, asset.rid).is_archived is True
        asset.unarchive()
        assert get_spatial(client, asset.rid).is_archived is False
    finally:
        asset.archive()


# --- point cloud ingest (requires the indexing service) ------------------------


def test_point_cloud_ingest_completes(client: NominalClient, spatial: Spatial, point_cloud_csv: Path) -> None:
    """The full pipeline: the backend accepts our daggerImportConfig and the import completes.

    This is the load-bearing test for the migration. The backend deserializes
    `daggerImportConfig` with FAIL_ON_UNKNOWN_PROPERTIES, so any drift in the v2 wire
    shape (a stray field, a lowercased enum, geometry_type left at the top level)
    fails the request outright rather than degrading quietly.
    """
    job = _ingest(spatial, point_cloud_csv)
    status = _poll_ingest_job(job)
    assert status == IngestionJobStatus.COMPLETED, f"ingest job {job.rid} ended {status}"


def test_point_cloud_ingest_accepts_column_type_overrides(
    client: NominalClient, spatial: Spatial, point_cloud_csv: Path
) -> None:
    """An explicit column_types override reaches the wire as Int and the backend still accepts it."""
    job = _ingest(spatial, point_cloud_csv, column_types={"count": "int"})
    status = _poll_ingest_job(job)
    assert status == IngestionJobStatus.COMPLETED, f"ingest job {job.rid} ended {status}"


def test_ingest_into_archived_spatial_is_accepted(
    client: NominalClient, spatial: Spatial, point_cloud_csv: Path
) -> None:
    """Archiving does not make a spatial read-only: the backend still accepts an ingest into it.

    Archive is a search-visibility flag, not a write lock -- the same reason it is reversible
    via `unarchive`. Pinned because the opposite is the intuitive guess, and an ingest that
    lands on a hidden spatial is easy to mistake for a silent no-op.
    """
    spatial.archive()
    # Polling to completion is what proves the upload really landed, rather than
    # being accepted and then dropped on the floor.
    assert _poll_ingest_job(_ingest(spatial, point_cloud_csv)) == IngestionJobStatus.COMPLETED


# --- spatials as run / asset data scopes --------------------------------------


def test_spatial_as_run_data_scope(client: NominalClient, spatial: Spatial) -> None:
    """add_spatial / get_spatial / list_spatials round-trip on a Run."""
    start = datetime.now(timezone.utc)
    run = client.create_run(f"e2e-spatial-run-{uuid4().hex[:8]}", start=start, end=start + timedelta(hours=1))
    try:
        add_spatial_to_run(run, "cloud", spatial)
        assert get_spatial_from_run(run, "cloud").rid == spatial.rid
        assert [(name, a.rid) for name, a in list_spatials_in_run(run)] == [("cloud", spatial.rid)]
    finally:
        run.archive()


def test_spatial_as_asset_data_scope(client: NominalClient, spatial: Spatial) -> None:
    """add_spatial / get_spatial / list_spatials round-trip on an Asset."""
    asset = client.create_asset(f"e2e-spatial-asset-{uuid4().hex[:8]}")
    try:
        add_spatial_to_asset(asset, "cloud", spatial)
        assert get_spatial_from_asset(asset, "cloud").rid == spatial.rid
        assert [(name, a.rid) for name, a in list_spatials_in_asset(asset)] == [("cloud", spatial.rid)]
        # Core's combined listing deliberately leaves spatials out: `ScopeType` covers only
        # the kinds core knows about, and a spatial is reached through this module instead.
        assert spatial.rid not in [scope.rid for _, scope in asset.list_data_scopes()]
    finally:
        asset.archive()
