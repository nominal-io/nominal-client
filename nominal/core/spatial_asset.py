from __future__ import annotations

import logging
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
    DEFAULT_RGB_ATTRIBUTE,
    ColumnDataType,
    TimeUnit,
    _ingest_point_cloud_csv,
    _PointCloudClients,
)
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

logger = logging.getLogger(__name__)

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
        rgb_column: str | None = None,
        rgb_attribute: str = DEFAULT_RGB_ATTRIBUTE,
        time_column: str | None = None,
        time_unit: TimeUnit = "s",
        start_timestamp: datetime | IntegralNanosecondsUTC | None = None,
        channel: str = DEFAULT_POINT_CLOUD_CHANNEL,
        tags: Mapping[str, str] | None = None,
    ) -> str | None:
        """Upload a point-cloud CSV and ingest it into this asset's Dagger model.

        The CSV must contain at minimum x, y, z columns (case-insensitive); remaining
        columns are auto-classified as int/real/string by sampling the first ~1000 data
        rows. Pass ``column_types`` to override inference for specific columns.

        Scout runs the Dagger import asynchronously, so this returns as soon as the
        ingest is *accepted*, not when the point cloud is queryable.

        Pass ``time_column`` for a cloud that was captured or built over time. Its extent
        is measured and recorded on the asset, which is what lets a workbook's 3D panel
        drive the cloud from the playhead -- without it the panel has no way to map
        playhead position onto per-point time and renders the whole cloud at once.

        Args:
            csv_path: Path to the point-cloud CSV to upload.
            column_types: Per-column overrides for the int/real/string classifier.
            rgb_column: Name of a column holding a six-character hex colour ("rrggbb", no
                leading #). It becomes an Rgb attribute, which is what per-point colouring
                reads; separate 0-255 columns cannot drive colour, and are silently skipped
                by the parser rather than rejected.
            rgb_attribute: Name for the resulting attribute.
            time_column: Column holding per-point time, as an offset from ``start_timestamp``
                rather than an absolute timestamp. Measuring its extent costs one extra
                pass over the file, so it is only read when named here.
            time_unit: Unit of the values in ``time_column``. Defaults to seconds.
            start_timestamp: Absolute instant that ``time_column`` counts from. Given both,
                the asset's time range and the four properties a workbook reads to drive
                the cloud from the playhead are set from the measured extent.
            channel: Channel name for the point cloud series. Accepted by the API but not
                yet read by the backend; reserved for workbook integration.
            tags: Tags for the point cloud series. Accepted but not yet read by the backend.

        Returns:
            The rid of the submitted ingest job, if scout created one.

        Raises:
            FileNotFoundError: If ``csv_path`` does not exist.
            ValueError: If the CSV is empty, lacks x/y/z columns, ``column_types`` names a
                column or type that does not exist, ``rgb_column`` is not in the header,
                or ``time_column`` is missing from the header or holds a non-numeric value.
        """
        source_handle, ingest_job_rid, time_range_us = _ingest_point_cloud_csv(
            self._clients,
            self.rid,
            csv_path,
            column_types=column_types,
            rgb_column=rgb_column,
            rgb_attribute=rgb_attribute,
            time_column=time_column,
            time_unit=time_unit,
            channel=channel,
            tags=tags,
        )
        # The s3 location is only known after the upload, which necessarily happens
        # after the asset exists -- so provenance is recorded here rather than at
        # create time. The time metadata rides along in the same call.
        # Best-effort: the ingest has already been accepted and is running. Raising
        # here would lose `ingest_job_rid`, leaving an untracked job that a retry
        # would submit a second time -- duplicating every point. Metadata can be
        # re-applied later; a duplicate ingest cannot be undone.
        try:
            request = scout_spatial_api.UpdateSpatialMetadataRequest(source_handle=api.Handle(s3=source_handle))
            if time_range_us is not None and start_timestamp is not None:
                request = self._time_metadata_request(request, time_range_us, start_timestamp)
            updated = self._clients.spatial.update_metadata(self._clients.auth_header, request, self.rid)
            self._refresh_from_api(updated)
        except Exception:
            logger.exception(
                "point cloud ingest %s was accepted, but recording metadata on %s failed; "
                "source handle and time range are unset",
                ingest_job_rid,
                self.rid,
            )
        return ingest_job_rid

    def _time_metadata_request(
        self,
        request: scout_spatial_api.UpdateSpatialMetadataRequest,
        time_range_us: tuple[int, int],
        start_timestamp: datetime | IntegralNanosecondsUTC,
    ) -> scout_spatial_api.UpdateSpatialMetadataRequest:
        """Add the asset time range and the properties a workbook reads to a request.

        Two coordinate systems, both required. `relative_*_us` is the extent of the
        time column itself, which is what per-point filtering compares against;
        `*_timestamp_us` is where that extent sits on the wall clock, which is what
        playhead position is measured over. Without the pair a workbook has nothing
        to anchor to and falls back to its own stream bounds.

        Per-point time is deliberately relative: filtering happens on the GPU in
        f32, where the spacing between representable values at epoch magnitude is
        over two minutes, in any unit.
        """
        origin_ns = _SecondsNanos.from_flexible(start_timestamp).to_nanoseconds()
        origin_us = origin_ns // 1_000
        relative_start_us, relative_end_us = time_range_us

        # `start_timestamp` is where the time column reads zero, which is not
        # necessarily where the data starts: a column running 9..180 begins nine
        # seconds later. Both bounds are offsets from the origin, so a non-zero
        # minimum has to shift the start as well as the end, or the playhead maps
        # onto the wrong part of the scan.
        data_start_ns = origin_ns + relative_start_us * 1_000
        data_end_ns = origin_ns + relative_end_us * 1_000

        # `properties` replaces the whole map rather than merging, so read the
        # current values back before overwriting it. `self.properties` is a
        # snapshot from whenever this object was last refreshed, and a large
        # upload can leave that minutes stale.
        latest = self._clients.spatial.get(self._clients.auth_header, self.rid)
        properties = {
            **dict(latest.properties),
            "relative_start_us": str(relative_start_us),
            "relative_end_us": str(relative_end_us),
            "start_timestamp_us": str(origin_us + relative_start_us),
            "end_timestamp_us": str(origin_us + relative_end_us),
        }
        return scout_spatial_api.UpdateSpatialMetadataRequest(
            source_handle=request.source_handle,
            properties=properties,
            start_timestamp=_SecondsNanos.from_nanoseconds(data_start_ns).to_api(),
            end_timestamp=_SecondsNanos.from_nanoseconds(data_end_ns).to_api(),
        )

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
