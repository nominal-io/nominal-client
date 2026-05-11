from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Mapping, Sequence

from conjure_python_client import ConjureDecoder, ConjureEncoder
from nominal_api import api, scout_spatial_api

from nominal.core._types import PathLike
from nominal.core._utils.multipart import upload_multipart_file
from nominal.core.filetype import FileTypes

if TYPE_CHECKING:
    from nominal.core.client import NominalClient

logger = logging.getLogger(__name__)


def upload_point_cloud(
    client: NominalClient,
    path: PathLike,
    name: str | None = None,
    *,
    description: str | None = None,
    labels: Sequence[str] = (),
    properties: Mapping[str, str] | None = None,
    sensor_model: str | None = None,
    coordinate_system: str | None = None,
    resolution_mm: float | None = None,
    scan_pattern: str | None = None,
) -> str:
    """Upload a CSV point cloud file and trigger spatial import into Dagger.

    The CSV must contain at minimum x, y, z columns. Additional attribute
    columns (time, reflectivity, signal, etc.) are preserved.

    Args:
        client: Connected NominalClient instance.
        path: Path to the CSV file containing point cloud data.
        name: Display name for the spatial asset. Defaults to the filename stem.
        description: Optional description.
        labels: Labels for categorization.
        properties: Arbitrary key-value metadata.
        sensor_model: Sensor model name, e.g. "Ouster OS1-128".
        coordinate_system: CRS identifier, e.g. "EPSG:4326", "ENU".
        resolution_mm: Spatial resolution in millimeters.
        scan_pattern: One of "ROTATING", "SOLID_STATE", "FLASH", "MECHANICAL".

    Returns:
        The spatial asset RID.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"No such file: {path}")

    if name is None:
        name = path.stem

    clients = client._clients
    workspace_rid = clients.resolve_default_workspace_rid()

    s3_path = upload_multipart_file(
        clients.auth_header,
        workspace_rid,
        path,
        clients.upload,
        file_type=FileTypes.CSV,
        header_provider=clients.header_provider,
    )

    point_cloud_metadata = scout_spatial_api.PointCloudMetadata(
        sensor_model=sensor_model,
        coordinate_system=coordinate_system,
        resolution_mm=resolution_mm,
        scan_pattern=(
            getattr(scout_spatial_api.ScanPattern, scan_pattern) if scan_pattern is not None else None
        ),
    )
    type_metadata = scout_spatial_api.SpatialTypeMetadata(
        point_cloud=point_cloud_metadata,
    )

    request = scout_spatial_api.ImportFileRequest(
        source=api.Handle(s3=s3_path),
        title=name,
        description=description,
        labels=list(labels),
        properties=dict(properties) if properties else {},
        type_metadata=type_metadata,
        workspace=workspace_rid,
        marking_rids=[],
    )

    # spatialType is required by the server but not yet in the SDK's ImportFileRequest
    request_json = ConjureEncoder.do_encode(request)
    request_json["spatialType"] = scout_spatial_api.SpatialType.POINT_CLOUD.value

    response = clients.spatial._request(
        "POST",
        clients.spatial._uri + "/spatial/v1/spatials/import-file",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": clients.auth_header,
        },
        params={},
        json=request_json,
    )

    decoder = ConjureDecoder()
    import_response = decoder.decode(
        response.json(),
        scout_spatial_api.ImportFileResponse,
        clients.spatial._return_none_for_unknown_union_types,
    )
    return import_response.spatial_rid
