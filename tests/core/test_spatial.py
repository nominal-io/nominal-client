from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from nominal_api import api, scout_spatial_api

from nominal.core.client import NominalClient
from nominal.core.spatial import PointCloudMetadata, ScanPattern, Spatial, _PointCloudTimeMetadata

_SPATIAL_RID = "ri.scout.x.spatial.abc"
_JOB_RID = "ri.scout.x.ingest-job.j"
_WORKSPACE_RID = "ri.scout.x.workspace.w"

_STATIC_CSV = "x,y,z,count\n0,0,0,1\n"
_TIMED_CSV = "x,y,z,t_s\n0,0,0,0\n1,1,1,45\n"


@pytest.fixture
def clients() -> MagicMock:
    clients = MagicMock()
    clients.auth_header = "Bearer t"
    clients.resolve_default_workspace_rid.return_value = _WORKSPACE_RID
    clients.ingest.ingest.return_value.ingest_job_rid = _JOB_RID
    # The time-range write reads properties back before replacing the map.
    clients.spatial.get.return_value.properties = {}
    return clients


@pytest.fixture
def client(clients: MagicMock) -> MagicMock:
    """A NominalClient stand-in whose methods run for real against mocked services."""
    nominal_client = MagicMock()
    nominal_client._clients = clients
    return nominal_client


@pytest.fixture
def spatial(clients: MagicMock) -> Spatial:
    return Spatial(
        rid=_SPATIAL_RID,
        name="scan",
        description=None,
        labels=(),
        properties={},
        is_archived=False,
        dagger_uuid="dagger-uuid",
        metadata=PointCloudMetadata(),
        created_at=1_700_000_000_000_000_000,
        start_timestamp=None,
        end_timestamp=None,
        source_handle=None,
        _clients=clients,
    )


def _raw_spatial(
    type_metadata: scout_spatial_api.SpatialTypeMetadata | None = None,
    start_timestamp: api.Timestamp | None = None,
) -> MagicMock:
    """A conjure Spatial bean as the service would return it."""
    raw = MagicMock()
    raw.rid = _SPATIAL_RID
    raw.title = "scan"
    raw.description = None
    raw.labels = []
    raw.properties = {}
    raw.is_archived = False
    raw.dagger_uuid = "dagger-uuid"
    raw.created_at = 1_700_000_000_000_000_000
    raw.created_by = None
    raw.type_metadata = type_metadata or scout_spatial_api.SpatialTypeMetadata(
        point_cloud=scout_spatial_api.PointCloudMetadata()
    )
    raw.start_timestamp = start_timestamp
    raw.end_timestamp = None
    return raw


def _stubbed_upload(s3_path: str = "s3://bucket/scan.csv") -> object:
    """Stub the multipart upload.

    It runs a thread pool over presigned PUTs to object storage, so there is no
    way to drive it from a mock that is not worse than replacing it outright.
    """
    return patch("nominal.core.spatial.upload_multipart_file", return_value=s3_path)


def _csv(tmp_path: Path, text: str = _STATIC_CSV) -> Path:
    path = tmp_path / "scan.csv"
    path.write_text(text)
    return path


# --- reading ------------------------------------------------------------------


def test_get_spatial_returns_typed_metadata_and_nanosecond_bounds(clients: MagicMock, client: MagicMock) -> None:
    """A fetched spatial exposes a typed metadata object and integral-nanosecond bounds, not raw beans."""
    clients.spatial.get.return_value = _raw_spatial(
        type_metadata=scout_spatial_api.SpatialTypeMetadata(
            point_cloud=scout_spatial_api.PointCloudMetadata(
                sensor_model="OS1-128", scan_pattern=scout_spatial_api.ScanPattern.ROTATING
            )
        ),
        start_timestamp=api.Timestamp(seconds=1_700_000_000, nanos=0),
    )

    result = NominalClient.get_spatial(client, _SPATIAL_RID)

    assert result.metadata == PointCloudMetadata(sensor_model="OS1-128", scan_pattern=ScanPattern.ROTATING)
    assert result.start_timestamp == 1_700_000_000_000_000_000
    assert result.end_timestamp is None


# --- creation -----------------------------------------------------------------


def test_create_spatial_reserves_a_model_in_the_default_workspace(clients: MagicMock, client: MagicMock) -> None:
    """Creation names the model that will hold the data and lands in the client's default workspace."""
    clients.spatial.create.return_value = _raw_spatial()

    NominalClient.create_spatial(
        client, "scan", metadata=PointCloudMetadata(sensor_model="OS1-128", scan_pattern=ScanPattern.ROTATING)
    )

    req = clients.spatial.create.call_args.args[1]
    assert uuid.UUID(req.dagger_uuid)
    assert req.workspace == _WORKSPACE_RID
    assert req.type_metadata.point_cloud.sensor_model == "OS1-128"
    assert req.type_metadata.point_cloud.scan_pattern == scout_spatial_api.ScanPattern.ROTATING


def test_each_spatial_reserves_its_own_model(clients: MagicMock, client: MagicMock) -> None:
    """Two spatials sharing a model uuid would index their imports on top of each other."""
    clients.spatial.create.return_value = _raw_spatial()

    uuids = set()
    for _ in range(3):
        NominalClient.create_spatial(client, "scan", metadata=PointCloudMetadata())
        uuids.add(clients.spatial.create.call_args.args[1].dagger_uuid)

    assert len(uuids) == 3


def test_create_spatial_applies_markings(clients: MagicMock, client: MagicMock) -> None:
    """Markings ride along in the create request, so a restricted spatial is never briefly visible."""
    clients.spatial.create.return_value = _raw_spatial()

    NominalClient.create_spatial(
        client, "scan", metadata=PointCloudMetadata(), markings=["ri.scout.x.marking.m1", "ri.scout.x.marking.m2"]
    )

    req = clients.spatial.create.call_args.args[1]
    assert req.marking_rids == ["ri.scout.x.marking.m1", "ri.scout.x.marking.m2"]


def test_update_replaces_only_the_fields_passed(clients: MagicMock, spatial: Spatial) -> None:
    """Everything not named is left untouched rather than cleared, which is how the service reads it."""
    spatial.update(start_timestamp=1_700_000_000_000_000_000)

    request = clients.spatial.update_metadata.call_args.args[1]
    assert request.start_timestamp.seconds == 1_700_000_000
    assert request.end_timestamp is None
    assert request.title is None
    assert request.labels is None
    assert request.properties is None


# --- the measured time range --------------------------------------------------


def test_time_metadata_places_the_measured_extent_on_the_wall_clock() -> None:
    """An extent plus the instant it counts from yield both coordinate systems a workbook needs.

    `relative_*_us` is the extent of the time column, which per-point filtering
    compares against; `*_timestamp_us` is where that extent sits on the wall
    clock, which playhead progress is measured over. A workbook with only one of
    them has nothing to anchor to.
    """
    start = datetime(2026, 3, 4, 9, 30, tzinfo=timezone.utc)
    start_us = int(start.timestamp() * 1_000_000)

    metadata = _PointCloudTimeMetadata.from_extent((0, 45_000_000), start)

    assert metadata.properties["relative_start_us"] == "0"
    assert metadata.properties["relative_end_us"] == "45000000"
    assert metadata.properties["start_timestamp_us"] == str(start_us)
    assert metadata.properties["end_timestamp_us"] == str(start_us + 45_000_000)


def test_time_metadata_shifts_both_bounds_by_a_nonzero_minimum() -> None:
    """A time column that starts late must move the spatial start, not just the end.

    The origin is where the column reads zero, which is not where the data begins.
    Anchoring the start at the origin while the end is offset stretches the window,
    and the playhead then maps onto the wrong part of the scan.
    """
    start = datetime(2026, 3, 4, 9, 30, tzinfo=timezone.utc)
    origin_ns = int(start.timestamp()) * 1_000_000_000

    metadata = _PointCloudTimeMetadata.from_extent((9_000_000, 180_000_000), start)

    assert metadata.start == origin_ns + 9_000_000_000
    assert metadata.end == origin_ns + 180_000_000_000
    assert metadata.properties["start_timestamp_us"] == str(origin_ns // 1_000 + 9_000_000)


# --- point cloud ingest -------------------------------------------------------


def test_ingest_targets_this_spatial_and_records_the_uploaded_object(
    clients: MagicMock, spatial: Spatial, tmp_path: Path
) -> None:
    """The ingest points at this spatial, and the object it uploaded is written back as provenance.

    The target must already exist -- the model uuid this spatial reserved at
    creation is what names the model the import writes into. The source handle is
    only knowable after the upload, so it cannot be set at create time.
    """
    with _stubbed_upload("s3://bucket/scan.csv"):
        job_rid = spatial.ingest_point_cloud_csv(_csv(tmp_path), channel="pc", tags={"run": "1"})

    assert job_rid == _JOB_RID
    opts = clients.ingest.ingest.call_args.args[1].options.point_cloud
    assert opts.source.s3.path == "s3://bucket/scan.csv"
    assert opts.target.existing.spatial_rid == _SPATIAL_RID
    assert opts.target.new is None
    assert opts.dagger_import_config["format"]["geometry_type"] == "Point"
    # source_uri is the backend's to fill in from the presigned URL.
    assert "source_uri" not in opts.dagger_import_config

    request = clients.spatial.update_metadata.call_args.args[1]
    assert request.source_handle.s3 == "s3://bucket/scan.csv"
    # A cloud with no time column gets no invented range.
    assert request.start_timestamp is None
    assert request.properties is None


def test_ingest_merges_onto_server_properties_not_a_stale_snapshot(
    clients: MagicMock, spatial: Spatial, tmp_path: Path
) -> None:
    """Properties are read back before overwriting, because the service replaces the map wholesale.

    A large upload can leave the in-memory snapshot minutes old, and anything added
    to the spatial meanwhile would be erased.
    """
    clients.spatial.get.return_value.properties = {"added": "while uploading"}

    with _stubbed_upload():
        spatial.ingest_point_cloud_csv(
            _csv(tmp_path, _TIMED_CSV), time_column="t_s", start_timestamp=datetime(2026, 3, 4, tzinfo=timezone.utc)
        )

    properties = clients.spatial.update_metadata.call_args.args[1].properties
    assert properties["added"] == "while uploading"
    assert properties["relative_end_us"] == "45000000"


def test_ingest_returns_the_job_even_if_recording_metadata_fails(
    clients: MagicMock, spatial: Spatial, tmp_path: Path
) -> None:
    """The ingest is already running; losing its rid would orphan it.

    A retry would resubmit and duplicate every point, which is not recoverable,
    whereas the metadata can simply be written again.
    """
    clients.spatial.update_metadata.side_effect = ConnectionError("boom")

    with _stubbed_upload():
        job_rid = spatial.ingest_point_cloud_csv(_csv(tmp_path))

    assert job_rid == _JOB_RID


def test_ingest_validates_the_csv_before_uploading(clients: MagicMock, spatial: Spatial, tmp_path: Path) -> None:
    """A malformed CSV must fail before many GB are pushed to object storage."""
    with pytest.raises(ValueError, match="missing required point-cloud columns"):
        spatial.ingest_point_cloud_csv(_csv(tmp_path, "a,b,c\n1,2,3\n"))

    clients.upload.initiate_multipart_upload.assert_not_called()
    clients.ingest.ingest.assert_not_called()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"time_column": "t_s"},
        {"start_timestamp": datetime(2026, 3, 4, tzinfo=timezone.utc)},
    ],
)
def test_ingest_rejects_half_of_the_time_pair(
    clients: MagicMock, spatial: Spatial, tmp_path: Path, kwargs: dict[str, object]
) -> None:
    """Neither half does anything alone, so half a pair is refused instead of silently dropped.

    An extent with nothing to anchor it to, and an anchor with no extent to
    place, are both no-ops -- and reaching that point costs a full extra pass
    over the file plus a potentially multi-GB upload.
    """
    with pytest.raises(ValueError, match="must be given together"):
        spatial.ingest_point_cloud_csv(_csv(tmp_path, _TIMED_CSV), **kwargs)  # type: ignore[arg-type]

    clients.upload.initiate_multipart_upload.assert_not_called()
    clients.ingest.ingest.assert_not_called()
