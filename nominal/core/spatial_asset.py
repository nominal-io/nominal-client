from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import TYPE_CHECKING, Mapping, Protocol, Sequence, TypeAlias

from nominal_api import api, scout_spatial, scout_spatial_api
from typing_extensions import Self

from nominal.core._types import PathLike
from nominal.core._utils.api_tools import HasRid, RefreshableConjureMixin
from nominal.core.point_cloud import (
    DEFAULT_POINT_CLOUD_CHANNEL,
    ColumnDataType,
    _ingest_point_cloud_csv,
    _PointCloudClients,
)
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

if TYPE_CHECKING:
    from datetime import datetime


class ScanPattern(Enum):
    """Point-cloud scan pattern, wrapping `nominal_api.scout_spatial_api.ScanPattern`."""

    FLASH = "FLASH"
    MECHANICAL = "MECHANICAL"
    ROTATING = "ROTATING"
    SOLID_STATE = "SOLID_STATE"
    UNKNOWN = "UNKNOWN"

    def _to_conjure(self) -> scout_spatial_api.ScanPattern:
        return _SCAN_PATTERN_TO_CONJURE[self]

    @classmethod
    def _from_conjure(cls, value: scout_spatial_api.ScanPattern) -> ScanPattern:
        return _SCAN_PATTERN_FROM_CONJURE.get(value, cls.UNKNOWN)


_SCAN_PATTERN_TO_CONJURE: Mapping[ScanPattern, scout_spatial_api.ScanPattern] = {
    ScanPattern.FLASH: scout_spatial_api.ScanPattern.FLASH,
    ScanPattern.MECHANICAL: scout_spatial_api.ScanPattern.MECHANICAL,
    ScanPattern.ROTATING: scout_spatial_api.ScanPattern.ROTATING,
    ScanPattern.SOLID_STATE: scout_spatial_api.ScanPattern.SOLID_STATE,
    ScanPattern.UNKNOWN: scout_spatial_api.ScanPattern.UNKNOWN,
}
_SCAN_PATTERN_FROM_CONJURE: Mapping[scout_spatial_api.ScanPattern, ScanPattern] = {
    v: k for k, v in _SCAN_PATTERN_TO_CONJURE.items()
}


@dataclass(frozen=True)
class PointCloudMetadata:
    """Point-cloud-specific metadata for a spatial asset."""

    sensor_model: str | None = None
    coordinate_system: str | None = None
    resolution_mm: float | None = None
    scan_pattern: ScanPattern | None = None

    def _to_conjure(self) -> scout_spatial_api.SpatialTypeMetadata:
        return scout_spatial_api.SpatialTypeMetadata(
            point_cloud=scout_spatial_api.PointCloudMetadata(
                sensor_model=self.sensor_model,
                coordinate_system=self.coordinate_system,
                resolution_mm=self.resolution_mm,
                scan_pattern=None if self.scan_pattern is None else self.scan_pattern._to_conjure(),
            )
        )


SpatialMetadata: TypeAlias = PointCloudMetadata


def _spatial_metadata_from_conjure(type_metadata: scout_spatial_api.SpatialTypeMetadata) -> SpatialMetadata:
    point_cloud = type_metadata.point_cloud
    if point_cloud is None:
        return PointCloudMetadata()
    return PointCloudMetadata(
        sensor_model=point_cloud.sensor_model,
        coordinate_system=point_cloud.coordinate_system,
        resolution_mm=point_cloud.resolution_mm,
        scan_pattern=None if point_cloud.scan_pattern is None else ScanPattern._from_conjure(point_cloud.scan_pattern),
    )


@dataclass(frozen=True)
class SpatialAsset(HasRid, RefreshableConjureMixin[scout_spatial_api.Spatial]):
    """A spatial asset (e.g. a point cloud) tracked by Nominal."""

    rid: str
    name: str
    description: str | None
    labels: Sequence[str]
    properties: Mapping[str, str]
    is_archived: bool
    dagger_uuid: str
    metadata: SpatialMetadata
    created_at: IntegralNanosecondsUTC
    start_timestamp: IntegralNanosecondsUTC | None
    end_timestamp: IntegralNanosecondsUTC | None
    source_handle: str | None
    """Object-storage location of the data ingested into this asset, recorded for provenance."""

    _clients: _Clients = field(repr=False)
    created_by_rid: str | None = field(default=None, repr=False)

    class _Clients(_PointCloudClients, Protocol):
        @property
        def spatial(self) -> scout_spatial.SpatialService: ...

    def _get_latest_api(self) -> scout_spatial_api.Spatial:
        return self._clients.spatial.get(self._clients.auth_header, self.rid)

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
        start_timestamp: datetime | IntegralNanosecondsUTC | None = None,
        end_timestamp: datetime | IntegralNanosecondsUTC | None = None,
    ) -> Self:
        """Replace spatial asset metadata in-place and return the updated asset.

        Only the fields passed in are replaced; the rest are left untouched.

        `start_timestamp` and `end_timestamp` bound the time range this asset covers, which
        is how it lines up against other data on a timeline. They are absolute instants,
        not a parsing format: a point-cloud CSV has no timestamp column.
        """
        request = scout_spatial_api.UpdateSpatialMetadataRequest(
            title=name,
            description=description,
            labels=None if labels is None else list(labels),
            properties=None if properties is None else dict(properties),
            start_timestamp=None if start_timestamp is None else _SecondsNanos.from_flexible(start_timestamp).to_api(),
            end_timestamp=None if end_timestamp is None else _SecondsNanos.from_flexible(end_timestamp).to_api(),
        )
        updated = self._clients.spatial.update_metadata(self._clients.auth_header, request, self.rid)
        return self._refresh_from_api(updated)

    def ingest_point_cloud_csv(
        self,
        csv_path: PathLike,
        *,
        column_types: Mapping[str, ColumnDataType] | None = None,
        channel: str = DEFAULT_POINT_CLOUD_CHANNEL,
        tags: Mapping[str, str] | None = None,
    ) -> str | None:
        """Upload a point-cloud CSV and ingest it into this asset's Dagger model.

        The CSV must contain at minimum x, y, z columns (case-insensitive); remaining
        columns are auto-classified as int/real/string by sampling the first ~1000 data
        rows. Pass ``column_types`` to override inference for specific columns.

        Scout runs the Dagger import asynchronously, so this returns as soon as the
        ingest is *accepted*, not when the point cloud is queryable.

        Args:
            csv_path: Path to the point-cloud CSV to upload.
            column_types: Per-column overrides for the int/real/string classifier.
            channel: Channel name for the point cloud series. Accepted by the API but not
                yet read by the backend; reserved for workbook integration.
            tags: Tags for the point cloud series. Accepted but not yet read by the backend.

        Returns:
            The rid of the submitted ingest job, if scout created one.

        Raises:
            FileNotFoundError: If ``csv_path`` does not exist.
            ValueError: If the CSV is empty, lacks x/y/z columns, or ``column_types``
                names a column or type that does not exist.
        """
        source_handle, ingest_job_rid = _ingest_point_cloud_csv(
            self._clients,
            self.rid,
            csv_path,
            column_types=column_types,
            channel=channel,
            tags=tags,
        )
        # The s3 location is only known after the upload, which necessarily happens
        # after the asset exists -- so provenance is recorded here rather than at
        # create time.
        self._clients.spatial.update_metadata(
            self._clients.auth_header,
            scout_spatial_api.UpdateSpatialMetadataRequest(source_handle=api.Handle(s3=source_handle)),
            self.rid,
        )
        return ingest_job_rid

    def archive(self) -> None:
        """Archive this spatial asset, hiding it from search (reversible)."""
        self._clients.spatial.archive(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """Unarchive a previously archived spatial asset."""
        self._clients.spatial.unarchive(self._clients.auth_header, self.rid)

    @classmethod
    def _from_conjure(cls, clients: _Clients, raw_spatial: scout_spatial_api.Spatial) -> Self:
        return cls(
            rid=raw_spatial.rid,
            name=raw_spatial.title,
            description=raw_spatial.description,
            labels=tuple(raw_spatial.labels),
            properties=MappingProxyType(raw_spatial.properties),
            is_archived=raw_spatial.is_archived,
            dagger_uuid=raw_spatial.dagger_uuid,
            metadata=_spatial_metadata_from_conjure(raw_spatial.type_metadata),
            created_at=_SecondsNanos.from_flexible(raw_spatial.created_at).to_nanoseconds(),
            start_timestamp=(
                None
                if raw_spatial.start_timestamp is None
                else _SecondsNanos.from_api(raw_spatial.start_timestamp).to_nanoseconds()
            ),
            end_timestamp=(
                None
                if raw_spatial.end_timestamp is None
                else _SecondsNanos.from_api(raw_spatial.end_timestamp).to_nanoseconds()
            ),
            source_handle=None if raw_spatial.source_handle is None else raw_spatial.source_handle.s3,
            _clients=clients,
            created_by_rid=raw_spatial.created_by,
        )


def _create_spatial_asset(
    auth_header: str,
    spatial_service: scout_spatial.SpatialService,
    name: str,
    *,
    metadata: SpatialMetadata,
    description: str | None,
    labels: Sequence[str],
    properties: Mapping[str, str] | None,
    workspace_rid: str,
    start_timestamp: datetime | IntegralNanosecondsUTC | None = None,
    end_timestamp: datetime | IntegralNanosecondsUTC | None = None,
) -> scout_spatial_api.Spatial:
    # The asset names the Dagger model rather than referencing an existing one:
    # scout indexes the import under this uuid when the point cloud is ingested,
    # and rejects an ingest that tries to create its own target.
    request = scout_spatial_api.CreateSpatialRequest(
        title=name,
        dagger_uuid=str(uuid.uuid4()),
        type_metadata=metadata._to_conjure(),
        labels=list(labels),
        properties=dict(properties) if properties else {},
        marking_rids=[],
        description=description,
        source_handle=None,
        workspace=workspace_rid,
        start_timestamp=None if start_timestamp is None else _SecondsNanos.from_flexible(start_timestamp).to_api(),
        end_timestamp=None if end_timestamp is None else _SecondsNanos.from_flexible(end_timestamp).to_api(),
    )
    return spatial_service.create(auth_header, request)


def _get_spatial(clients: SpatialAsset._Clients, rid: str) -> scout_spatial_api.Spatial:
    return clients.spatial.get(clients.auth_header, rid)
