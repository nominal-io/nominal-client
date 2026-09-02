from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

from nominal_api import scout_spatial_api

from nominal.core.spatial_asset import (
    PointCloudMetadata,
    ScanPattern,
    SpatialAsset,
    _spatial_metadata_from_conjure,
)


def test_point_cloud_metadata_to_conjure_maps_fields_and_scan_pattern() -> None:
    """PointCloudMetadata._to_conjure produces a point_cloud union arm with mapped fields."""
    conjure = PointCloudMetadata(
        sensor_model="Ouster OS1-128",
        coordinate_system="ENU",
        resolution_mm=10.0,
        scan_pattern=ScanPattern.ROTATING,
    )._to_conjure()
    pc = conjure.point_cloud
    assert pc is not None
    assert pc.sensor_model == "Ouster OS1-128"
    assert pc.coordinate_system == "ENU"
    assert pc.resolution_mm == 10.0
    assert pc.scan_pattern == scout_spatial_api.ScanPattern.ROTATING


def test_point_cloud_metadata_to_conjure_omits_unset_scan_pattern() -> None:
    """A None scan_pattern stays None in the conjure metadata."""
    pc = PointCloudMetadata()._to_conjure().point_cloud
    assert pc is not None
    assert pc.scan_pattern is None


def test_spatial_metadata_from_conjure_reads_point_cloud() -> None:
    """_spatial_metadata_from_conjure maps a point_cloud union back to PointCloudMetadata."""
    conjure = scout_spatial_api.SpatialTypeMetadata(
        point_cloud=scout_spatial_api.PointCloudMetadata(
            sensor_model="Ouster OS1-128",
            scan_pattern=scout_spatial_api.ScanPattern.ROTATING,
        )
    )
    md = _spatial_metadata_from_conjure(conjure)
    assert isinstance(md, PointCloudMetadata)
    assert md.sensor_model == "Ouster OS1-128"
    assert md.scan_pattern == ScanPattern.ROTATING


def test_spatial_metadata_from_conjure_returns_empty_on_missing_point_cloud() -> None:
    """A SpatialTypeMetadata with an empty point_cloud arm returns an empty PointCloudMetadata."""
    conjure = scout_spatial_api.SpatialTypeMetadata(point_cloud=scout_spatial_api.PointCloudMetadata())
    md = _spatial_metadata_from_conjure(conjure)
    assert md == PointCloudMetadata()


def test_spatial_asset_from_conjure_builds_typed_metadata() -> None:
    """SpatialAsset._from_conjure populates a typed `metadata` from the bean's type_metadata."""
    raw = MagicMock()
    raw.rid = "ri.scout.x.spatial.abc"
    raw.title = "scan"
    raw.description = "d"
    raw.labels = ["lidar"]
    raw.properties = {"k": "v"}
    raw.is_archived = False
    raw.dagger_uuid = "dagger-uuid"
    raw.created_at = 1_700_000_000_000_000_000
    raw.created_by = "ri.user.1"
    raw.type_metadata = scout_spatial_api.SpatialTypeMetadata(
        point_cloud=scout_spatial_api.PointCloudMetadata(
            sensor_model="OS1-128", scan_pattern=scout_spatial_api.ScanPattern.ROTATING
        )
    )

    asset = SpatialAsset._from_conjure(MagicMock(), raw)

    assert asset.metadata == PointCloudMetadata(sensor_model="OS1-128", scan_pattern=ScanPattern.ROTATING)
    assert not hasattr(asset, "sensor_model")


def test_create_spatial_asset_builds_request_and_returns_asset() -> None:
    """create_spatial_asset posts a CreateSpatialRequest with a generated dagger uuid and metadata."""
    clients = MagicMock()
    clients.auth_header = "Bearer t"
    clients.resolve_default_workspace_rid.return_value = "ri.scout.x.workspace.w"
    created = MagicMock()
    created.rid = "ri.scout.x.spatial.abc"
    created.title = "scan"
    created.description = "d"
    created.labels = []
    created.properties = {}
    created.is_archived = False
    created.dagger_uuid = "dagger-uuid"
    created.created_at = 1_700_000_000_000_000_000
    created.created_by = "ri.user.1"
    created.type_metadata = scout_spatial_api.SpatialTypeMetadata(
        point_cloud=scout_spatial_api.PointCloudMetadata(sensor_model="OS1-128")
    )
    clients.spatial.create.return_value = created
    nominal_client = MagicMock()
    nominal_client._clients = clients

    from nominal.core.client import NominalClient

    asset = NominalClient.create_spatial_asset(
        nominal_client,
        "scan",
        metadata=PointCloudMetadata(sensor_model="OS1-128", scan_pattern=ScanPattern.ROTATING),
        description="d",
        labels=["lidar"],
        properties={"k": "v"},
    )

    clients.spatial.create.assert_called_once()
    req = clients.spatial.create.call_args.args[1]
    assert req.title == "scan"
    # The client reserves the model uuid; scout indexes the Dagger import under it later.
    assert uuid.UUID(req.dagger_uuid)
    assert req.workspace == "ri.scout.x.workspace.w"
    assert req.type_metadata.point_cloud.sensor_model == "OS1-128"
    assert req.type_metadata.point_cloud.scan_pattern == scout_spatial_api.ScanPattern.ROTATING
    # The source location is not known until the CSV is uploaded during ingest.
    assert req.source_handle is None
    assert req.labels == ["lidar"]
    assert req.properties == {"k": "v"}
    assert isinstance(asset, SpatialAsset)
    assert asset.rid == "ri.scout.x.spatial.abc"


def test_create_spatial_asset_generates_a_unique_uuid_per_asset() -> None:
    """Each created asset reserves its own Dagger model, so uuids must not be shared."""
    from nominal.core.spatial_asset import _create_spatial_asset

    spatial_service = MagicMock()
    seen = set()
    for _ in range(3):
        _create_spatial_asset(
            "Bearer t",
            spatial_service,
            "scan",
            metadata=PointCloudMetadata(),
            description=None,
            labels=(),
            properties=None,
            workspace_rid="ri.scout.x.workspace.w",
        )
        seen.add(spatial_service.create.call_args.args[1].dagger_uuid)
    assert len(seen) == 3


def _spatial_asset(clients: MagicMock) -> SpatialAsset:
    return SpatialAsset(
        rid="ri.scout.x.spatial.abc",
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


def test_ingest_point_cloud_csv_targets_this_asset_and_records_provenance() -> None:
    """The asset ingests into itself, then stores the uploaded object as its source handle."""
    clients = MagicMock()
    clients.auth_header = "Bearer t"
    asset = _spatial_asset(clients)

    with patch(
        "nominal.core.spatial_asset._ingest_point_cloud_csv",
        return_value=("s3://bucket/scan.csv", "ri.scout.x.ingest-job.j"),
    ) as ingest:
        job_rid = asset.ingest_point_cloud_csv("scan.csv", column_types={"count": "real"}, tags={"run": "1"})

    assert job_rid == "ri.scout.x.ingest-job.j"
    assert ingest.call_args.args[1] == "ri.scout.x.spatial.abc"
    assert ingest.call_args.kwargs["column_types"] == {"count": "real"}
    assert ingest.call_args.kwargs["tags"] == {"run": "1"}

    # source_handle is only knowable post-upload, so it is written back after ingest.
    clients.spatial.update_metadata.assert_called_once()
    request = clients.spatial.update_metadata.call_args.args[1]
    assert request.source_handle.s3 == "s3://bucket/scan.csv"
    assert clients.spatial.update_metadata.call_args.args[2] == "ri.scout.x.spatial.abc"


def test_create_spatial_asset_passes_time_bounds() -> None:
    """start/end are absolute instants bounding the asset, converted to api.Timestamp."""
    from datetime import datetime, timezone

    from nominal.core.spatial_asset import _create_spatial_asset

    spatial_service = MagicMock()
    _create_spatial_asset(
        "Bearer t",
        spatial_service,
        "scan",
        metadata=PointCloudMetadata(),
        description=None,
        labels=(),
        properties=None,
        workspace_rid="ri.scout.x.workspace.w",
        start_timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_timestamp=1_800_000_000_000_000_000,
    )
    req = spatial_service.create.call_args.args[1]
    assert req.start_timestamp.seconds == int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp())
    assert req.end_timestamp.seconds == 1_800_000_000


def test_create_spatial_asset_omits_time_bounds_when_unset() -> None:
    from nominal.core.spatial_asset import _create_spatial_asset

    spatial_service = MagicMock()
    _create_spatial_asset(
        "Bearer t",
        spatial_service,
        "scan",
        metadata=PointCloudMetadata(),
        description=None,
        labels=(),
        properties=None,
        workspace_rid="ri.scout.x.workspace.w",
    )
    req = spatial_service.create.call_args.args[1]
    assert req.start_timestamp is None
    assert req.end_timestamp is None


def test_update_sets_time_bounds() -> None:
    clients = MagicMock()
    clients.auth_header = "Bearer t"
    asset = _spatial_asset(clients)
    asset.update(start_timestamp=1_700_000_000_000_000_000)
    request = clients.spatial.update_metadata.call_args.args[1]
    assert request.start_timestamp.seconds == 1_700_000_000
    assert request.end_timestamp is None


def test_from_conjure_reads_time_bounds() -> None:
    from nominal_api import api

    raw = MagicMock()
    raw.rid = "ri.scout.x.spatial.abc"
    raw.title = "scan"
    raw.description = None
    raw.labels = []
    raw.properties = {}
    raw.is_archived = False
    raw.dagger_uuid = "dagger-uuid"
    raw.created_at = 1_700_000_000_000_000_000
    raw.created_by = None
    raw.type_metadata = scout_spatial_api.SpatialTypeMetadata(point_cloud=scout_spatial_api.PointCloudMetadata())
    raw.start_timestamp = api.Timestamp(seconds=1_700_000_000, nanos=0)
    raw.end_timestamp = None

    asset = SpatialAsset._from_conjure(MagicMock(), raw)
    assert asset.start_timestamp == 1_700_000_000_000_000_000
    assert asset.end_timestamp is None
