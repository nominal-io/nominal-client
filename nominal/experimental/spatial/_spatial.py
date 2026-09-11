from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Mapping, Protocol, Sequence, overload

from nominal_api import api, ingest_api, scout_spatial, scout_spatial_api, upload_api
from typing_extensions import Self

from nominal.core import Dataset, Marking, NominalClient
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._types import PathLike
from nominal.core._utils.api_tools import HasRid, RefreshableConjureMixin
from nominal.core._utils.multipart import upload_multipart_file
from nominal.core.filetype import FileTypes
from nominal.core.ingestion_job import IngestionJob
from nominal.core.marking import _marking_rids
from nominal.experimental.spatial._point_cloud import ColumnDataType, _describe_point_cloud_csv, _PointCloudCsv
from nominal.ts import IntegralNanosecondsUTC, Relative, _LiteralTimeUnit, _SecondsNanos, _validate_timestamp_pair

logger = logging.getLogger(__name__)

# Required by the ingest request, but not yet read by the backend.
_POINT_CLOUD_CHANNEL = "point_cloud"


class ScanPattern(Enum):
    """Point-cloud scan pattern, wrapping `nominal_api.scout_spatial_api.ScanPattern`."""

    FLASH = "FLASH"
    MECHANICAL = "MECHANICAL"
    ROTATING = "ROTATING"
    SOLID_STATE = "SOLID_STATE"
    UNKNOWN = "UNKNOWN"

    def _to_conjure(self) -> scout_spatial_api.ScanPattern:
        match self:
            case ScanPattern.FLASH:
                return scout_spatial_api.ScanPattern.FLASH
            case ScanPattern.MECHANICAL:
                return scout_spatial_api.ScanPattern.MECHANICAL
            case ScanPattern.ROTATING:
                return scout_spatial_api.ScanPattern.ROTATING
            case ScanPattern.SOLID_STATE:
                return scout_spatial_api.ScanPattern.SOLID_STATE
            case _:
                return scout_spatial_api.ScanPattern.UNKNOWN

    @classmethod
    def _from_conjure(cls, value: scout_spatial_api.ScanPattern) -> ScanPattern:
        match value.name:
            case "FLASH":
                return cls.FLASH
            case "MECHANICAL":
                return cls.MECHANICAL
            case "ROTATING":
                return cls.ROTATING
            case "SOLID_STATE":
                return cls.SOLID_STATE
            case _:
                # A pattern this client predates maps to UNKNOWN rather than raising:
                # reading a spatial must not fail because the platform learned a new one.
                return cls.UNKNOWN


@dataclass(frozen=True)
class PointCloudMetadata:
    """Point-cloud-specific metadata for a spatial."""

    sensor_model: str | None = None
    coordinate_system: str | None = None
    resolution_mm: float | None = None
    scan_pattern: ScanPattern | None = None

    def _to_conjure(self) -> scout_spatial_api.PointCloudMetadata:
        return scout_spatial_api.PointCloudMetadata(
            sensor_model=self.sensor_model,
            coordinate_system=self.coordinate_system,
            resolution_mm=self.resolution_mm,
            scan_pattern=None if self.scan_pattern is None else self.scan_pattern._to_conjure(),
        )

    @classmethod
    def _from_conjure(cls, type_metadata: scout_spatial_api.SpatialTypeMetadata) -> Self:
        """Read the point-cloud arm of a spatial's type metadata.

        `SpatialTypeMetadata` is a union with one arm today, so an unset arm means
        a spatial of a kind this client predates; an empty metadata reads better
        there than a crash on a field that simply is not there yet.
        """
        point_cloud = type_metadata.point_cloud
        if point_cloud is None:
            return cls()
        return cls(
            sensor_model=point_cloud.sensor_model,
            coordinate_system=point_cloud.coordinate_system,
            resolution_mm=point_cloud.resolution_mm,
            scan_pattern=(
                None if point_cloud.scan_pattern is None else ScanPattern._from_conjure(point_cloud.scan_pattern)
            ),
        )


@dataclass(frozen=True)
class _PointCloudTimeMetadata:
    """Where a measured time-column extent sits, in the two coordinate systems a workbook needs.

    `relative_*_us` is the extent of the time column itself, which is what
    per-point filtering compares against; `*_timestamp_us` is where that extent
    sits on the wall clock, which is what playhead position is measured over.
    Without the pair a workbook has nothing to anchor to and falls back to its
    own stream bounds.

    Per-point time is deliberately relative: filtering happens on the GPU in
    f32, where the spacing between representable values at epoch magnitude is
    over two minutes, in any unit.
    """

    start: IntegralNanosecondsUTC
    end: IntegralNanosecondsUTC
    properties: Mapping[str, str]

    @classmethod
    def from_extent(
        cls,
        time_range_us: tuple[int, int],
        origin: datetime | IntegralNanosecondsUTC,
        *,
        attribute: str | None = None,
        unit: _LiteralTimeUnit = "seconds",
    ) -> Self:
        """Place a measured `(start, end)` extent in microseconds against the instant it counts from."""
        origin_ns = _SecondsNanos.from_flexible(origin).to_nanoseconds()
        origin_us = origin_ns // 1_000
        relative_start_us, relative_end_us = time_range_us
        # `origin` is where the time column reads zero, which is not necessarily
        # where the data starts: a column running 9..180 begins nine seconds
        # later. Both bounds are offsets from the origin, so a non-zero minimum
        # has to shift the start as well as the end, or the playhead maps onto
        # the wrong part of the scan.
        return cls(
            start=origin_ns + relative_start_us * 1_000,
            end=origin_ns + relative_end_us * 1_000,
            properties={
                **(
                    {"time_attribute": attribute, "time_unit": unit, "time_origin_ns": str(origin_ns)}
                    if attribute is not None
                    else {}
                ),
                "relative_start_us": str(relative_start_us),
                "relative_end_us": str(relative_end_us),
                "start_timestamp_us": str(origin_us + relative_start_us),
                "end_timestamp_us": str(origin_us + relative_end_us),
            },
        )


@dataclass(frozen=True)
class Spatial(HasRid, RefreshableConjureMixin[scout_spatial_api.Spatial]):
    """A spatial data source (e.g. a point cloud) tracked by Nominal."""

    rid: str
    name: str
    description: str | None
    labels: Sequence[str]
    properties: Mapping[str, str]
    is_archived: bool
    dagger_uuid: str
    metadata: PointCloudMetadata
    created_at: IntegralNanosecondsUTC
    start_timestamp: IntegralNanosecondsUTC | None
    end_timestamp: IntegralNanosecondsUTC | None

    _clients: _Clients = field(repr=False)
    created_by_rid: str | None = field(default=None, repr=False)

    class _Clients(IngestionJob._Clients, HasScoutParams, Protocol):
        @property
        def spatial(self) -> scout_spatial.SpatialService: ...
        @property
        def ingest(self) -> ingest_api.IngestService: ...
        @property
        def upload(self) -> upload_api.UploadService: ...

    def _get_latest_api(self) -> scout_spatial_api.Spatial:
        return self._clients.spatial.get(self._clients.auth_header, self.rid)

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> Self:
        """Replace spatial metadata in-place and return the updated spatial.

        Only the fields passed in are replaced; the rest are left untouched.
        """
        request = scout_spatial_api.UpdateSpatialMetadataRequest(
            title=name,
            description=description,
            labels=None if labels is None else list(labels),
            properties=None if properties is None else dict(properties),
        )
        updated = self._clients.spatial.update_metadata(self._clients.auth_header, request, self.rid)
        return self._refresh_from_api(updated)

    @overload
    def add_point_cloud_csv(
        self,
        csv_path: PathLike,
        *,
        column_types: Mapping[str, ColumnDataType] | None = None,
        rgb_column: str | None = None,
        rgb_attribute: str = "color",
    ) -> IngestionJob: ...

    @overload
    def add_point_cloud_csv(
        self,
        csv_path: PathLike,
        *,
        column_types: Mapping[str, ColumnDataType] | None = None,
        rgb_column: str | None = None,
        rgb_attribute: str = "color",
        timestamp_column: str,
        timestamp_type: Relative,
    ) -> IngestionJob: ...

    def add_point_cloud_csv(
        self,
        csv_path: PathLike,
        *,
        column_types: Mapping[str, ColumnDataType] | None = None,
        rgb_column: str | None = None,
        rgb_attribute: str = "color",
        timestamp_column: str | None = None,
        timestamp_type: Relative | None = None,
    ) -> IngestionJob:
        """Upload a point-cloud CSV and add it to this spatial's model.

        The CSV must contain at minimum x, y, z columns (case-insensitive); remaining
        columns are classified as real or string by sampling the first ~1000 data rows.
        Numeric columns are always typed real -- pass ``column_types`` to ask for int on
        a column you know holds integers.

        The import runs asynchronously, so this returns as soon as the ingest is
        *accepted*, not when the point cloud is queryable. Poll the returned job to
        wait for it.

        Pass ``timestamp_column`` and ``timestamp_type`` for a cloud that was captured
        or built over time. The column's extent is measured and recorded on the spatial,
        which is what lets a workbook's 3D panel drive the cloud from the playhead --
        without it the panel has no way to map playhead position onto per-point time
        and renders the whole cloud at once.

        Args:
            csv_path: Path to the point-cloud CSV to upload.
            column_types: Per-column overrides for the real/string classifier. The only
                way to get the Int wire type, which is never inferred.
            rgb_column: Name of a column holding a six-character hex colour ("rrggbb", no
                leading #). It becomes an Rgb attribute, which is what per-point colouring
                reads; separate 0-255 columns cannot drive colour, and are silently skipped
                by the parser rather than rejected.
            rgb_attribute: Name for the resulting attribute.
            timestamp_column: Column holding per-point time. Measuring its extent costs one
                extra pass over the file, so it is only read when named here.
            timestamp_type: How to read ``timestamp_column``. Only `Relative` is accepted:
                per-point time has to be an offset from a start instant, because filtering
                happens on the GPU in f32, where the spacing between representable values
                at epoch magnitude is over two minutes.

        Returns:
            The submitted ingest job. Follow it with `status` and `refresh()`, which is
            the only signal it carries: the import writes into this spatial's own model
            rather than the catalog, so the job reports no `dataset_rid`, a
            `produced_file_count` of zero, and an empty `dataset_files()`.
            `as_files_ingested()` still raises if the job fails, but otherwise yields
            nothing -- an empty result there means the job produced no catalog entry,
            not that it produced no points.

        Raises:
            FileNotFoundError: If ``csv_path`` does not exist.
            ValueError: If only one of ``timestamp_column`` / ``timestamp_type`` is given,
                the CSV is empty or uses quoting, lacks x/y/z columns, ``column_types``
                names a column or type that does not exist, ``rgb_column`` is not in the
                header, or ``timestamp_column`` is missing from the header or holds a
                non-numeric value.
        """
        _validate_timestamp_pair(timestamp_column, timestamp_type)

        described = _describe_point_cloud_csv(
            csv_path,
            column_types=column_types,
            rgb_column=rgb_column,
            rgb_attribute=rgb_attribute,
            timestamp_column=timestamp_column,
            time_unit="seconds" if timestamp_type is None else timestamp_type.unit,
        )
        source_handle = self._upload_csv(described.path)

        time_metadata = (
            None
            if described.time_range_us is None or timestamp_type is None
            else _PointCloudTimeMetadata.from_extent(
                described.time_range_us, timestamp_type.start, attribute=timestamp_column, unit=timestamp_type.unit
            )
        )
        self._record_ingest_metadata(source_handle, time_metadata)
        return self._submit_ingest(source_handle, described)

    def _upload_csv(self, path: Path) -> str:
        """Upload the CSV to object storage and return its location."""
        return upload_multipart_file(
            self._clients.auth_header,
            self._clients.resolve_default_workspace_rid(),
            path,
            self._clients.upload,
            file_type=FileTypes.CSV,
            header_provider=self._clients.header_provider,
        )

    def _submit_ingest(self, source_handle: str, described: _PointCloudCsv) -> IngestionJob:
        """Submit the uploaded CSV to the point-cloud ingest pipeline and return the job it created."""
        # The target must already exist: the service rejects `PointCloudIngestTarget.new`,
        # since this spatial's daggerUuid is what names the model the import writes into.
        #
        # `channel` and `tags` are required by the request but not yet read by the
        # backend, so they are not exposed: a caller passing tags that never appear
        # anywhere would be worse than not offering them.
        response = self._clients.ingest.ingest(
            self._clients.auth_header,
            ingest_api.IngestRequest(
                options=ingest_api.IngestOptions(
                    point_cloud=ingest_api.PointCloudOpts(
                        source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=source_handle)),
                        target=ingest_api.PointCloudIngestTarget(
                            existing=ingest_api.ExistingSpatialIngestDestination(spatial_rid=self.rid)
                        ),
                        dagger_import_config=described.import_config._to_wire(),
                        channel=_POINT_CLOUD_CHANNEL,
                        tags={},
                    )
                )
            ),
        )
        if response.ingest_job_rid is None:
            raise ValueError(f"point cloud ingest for {self.rid} was accepted without an ingest job to track it")
        logger.debug(
            "submitted point cloud ingest for %s: spatial=%s ingest_job=%s",
            described.path,
            self.rid,
            response.ingest_job_rid,
        )
        job = self._clients.ingest_jobs.get_ingest_job(self._clients.auth_header, response.ingest_job_rid)
        return IngestionJob._from_conjure(self._clients, job)

    def _record_ingest_metadata(self, source_handle: str, time_metadata: _PointCloudTimeMetadata | None) -> None:
        """Record provenance, and the measured time range when there is one.

        The object-storage location is only known after the upload, which
        necessarily happens after the spatial exists, so provenance is recorded
        here rather than at create time. The time metadata rides along in the
        same call.
        """
        handle = api.Handle(s3=source_handle)
        if time_metadata is None:
            request = scout_spatial_api.UpdateSpatialMetadataRequest(source_handle=handle)
        else:
            # `properties` replaces the whole map rather than merging, so read the
            # current values back before overwriting it. `self.properties` is a
            # snapshot from whenever this object was last refreshed, and a large
            # upload can leave that minutes stale.
            latest = self._clients.spatial.get(self._clients.auth_header, self.rid)
            request = scout_spatial_api.UpdateSpatialMetadataRequest(
                source_handle=handle,
                properties={**dict(latest.properties), **time_metadata.properties},
                start_timestamp=_SecondsNanos.from_nanoseconds(time_metadata.start).to_api(),
                end_timestamp=_SecondsNanos.from_nanoseconds(time_metadata.end).to_api(),
            )
        updated = self._clients.spatial.update_metadata(self._clients.auth_header, request, self.rid)
        self._refresh_from_api(updated)

    def archive(self) -> None:
        """Archive this spatial, hiding it from search (reversible)."""
        self._clients.spatial.archive(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """Unarchive a previously archived spatial."""
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
            metadata=PointCloudMetadata._from_conjure(raw_spatial.type_metadata),
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
            _clients=clients,
            created_by_rid=raw_spatial.created_by,
        )


def create_point_cloud_spatial(
    client: NominalClient,
    name: str,
    *,
    metadata: PointCloudMetadata,
    description: str | None = None,
    labels: Sequence[str] = (),
    properties: Mapping[str, str] | None = None,
    markings: Sequence[Marking | str] | None = None,
    dataset: Dataset | None = None,
    channel: str = "point_cloud",
    tags: Mapping[str, str] | None = None,
    start_timestamp: datetime | IntegralNanosecondsUTC | None = None,
    end_timestamp: datetime | IntegralNanosecondsUTC | None = None,
) -> Spatial:
    """Create an empty spatial, ready to have a point cloud added to it.

    The spatial reserves the model that will hold its data; add the data with
    `Spatial.add_point_cloud_csv`. Create one spatial per uploaded file.
    Dataset channels require absolute coverage bounds at creation; ingest records
    the measured bounds and the exact origin of the file's relative timestamps.

    Args:
        dataset: Optional source dataset in this client's workspace.
        channel: Channel name for the uploaded spatial.
        tags: Channel tags. Requires a dataset.
        start_timestamp: Absolute coverage start, required for dataset channels.
        end_timestamp: Absolute coverage end, required for dataset channels.
        client: Client to create the spatial with.
        name: Human-readable name for the spatial.
        metadata: Point-cloud metadata, e.g. `PointCloudMetadata(sensor_model=...)`.
        description: Optional description.
        labels: Labels to apply.
        properties: Key-value properties to apply.
        markings: If present, markings (or marking RIDs) applied to the spatial. Sent as part of
            the creation request rather than applied in a follow-up call. Without any, the
            spatial is visible to everyone in the workspace.

    Returns:
        The created spatial.
    """
    if dataset is not None and (start_timestamp is None or end_timestamp is None):
        raise ValueError("Dataset channel uploads require start_timestamp and end_timestamp")
    if dataset is None and tags:
        raise ValueError("Spatial channel tags require a dataset")
    return Spatial._from_conjure(
        client._clients,
        _create_spatial_request(
            client,
            name,
            metadata=metadata,
            description=description,
            labels=labels,
            properties=properties,
            markings=markings,
            dataset=dataset,
            channel=channel,
            tags=tags,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        ),
    )


def get_spatial(client: NominalClient, rid: str) -> Spatial:
    """Retrieve a spatial by its RID."""
    return Spatial._from_conjure(client._clients, _get_spatial(client._clients, rid))


def _create_spatial_request(
    client: NominalClient,
    name: str,
    *,
    metadata: PointCloudMetadata,
    description: str | None,
    labels: Sequence[str],
    properties: Mapping[str, str] | None,
    markings: Sequence[Marking | str] | None,
    dataset: Dataset | None = None,
    channel: str = "point_cloud",
    tags: Mapping[str, str] | None = None,
    start_timestamp: datetime | IntegralNanosecondsUTC | None = None,
    end_timestamp: datetime | IntegralNanosecondsUTC | None = None,
) -> scout_spatial_api.Spatial:
    request = scout_spatial_api.CreateSpatialRequest(
        title=name,
        dagger_uuid=str(uuid.uuid4()),
        type_metadata=scout_spatial_api.SpatialTypeMetadata(point_cloud=metadata._to_conjure()),
        labels=list(labels),
        properties=dict(properties) if properties else {},
        marking_rids=_marking_rids(markings),
        description=description,
        workspace=client._clients.resolve_default_workspace_rid(),
        start_timestamp=None if start_timestamp is None else _SecondsNanos.from_flexible(start_timestamp).to_api(),
        end_timestamp=None if end_timestamp is None else _SecondsNanos.from_flexible(end_timestamp).to_api(),
    )
    if dataset is not None:
        return client._clients.spatial.create_in_channel(
            client._clients.auth_header, request, dataset.rid, channel, tags or {}
        )
    return client._clients.spatial.create(client._clients.auth_header, request)


def _get_spatial(clients: Spatial._Clients, rid: str) -> scout_spatial_api.Spatial:
    return clients.spatial.get(clients.auth_header, rid)
