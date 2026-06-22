from __future__ import annotations

from unittest.mock import MagicMock

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
    conjure = scout_spatial_api.SpatialTypeMetadata(
        point_cloud=scout_spatial_api.PointCloudMetadata()
    )
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
