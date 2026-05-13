from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Mapping, Sequence

from dagger_client import AuthenticatedClient
from dagger_client.api.import_ import post_import
from dagger_client.api.object_space import put_object_space
from dagger_client.models import (
    Archetype,
    Attribute,
    ColumnSelection,
    FseHeader,
    GeometryType,
    ImportRequest,
    PutObjectSpaceRequest,
    RealMeasurement,
)
from dagger_client.models import (
    FseAttributeTypeType0 as _FseTypeReal,
)
from dagger_client.models import (
    FseAttributeTypeType1 as _FseTypeString,
)
from dagger_client.models import (
    FseAttributeTypeType3 as _FseTypeInt,
)
from nominal_api import api, scout_spatial_api

from nominal.core._clientsbunch import ClientsBunch
from nominal.core._types import PathLike
from nominal.core._utils.multipart import upload_multipart_file
from nominal.core.filetype import FileTypes

if TYPE_CHECKING:
    from nominal.core.client import NominalClient

logger = logging.getLogger(__name__)

# scout's signed-download endpoint, mounted on the new SignedUrlResource. Lives
# under the same /api prefix as the other scout APIs (clients._api_base_url
# already ends in /api).
# TODO: confirm exact path once the paired scout PR lands.
_PRESIGN_DOWNLOAD_PATH = "/signed-urls/v1/download"

# scout's dagger reverse proxy. Strips this prefix and forwards to dagger.
_DAGGER_PROXY_PATH = "/dagger"


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

    The CSV must contain at minimum x, y, z columns (case-insensitive). The
    remaining columns are classified as int/real/string from the first data
    row.

    Returns:
        The spatial asset RID.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"No such file: {path}")

    if name is None:
        name = path.stem

    clients = client._clients

    workspace = clients.resolve_workspace(None)
    workspace_rid = workspace.rid
    object_space = workspace.id
    tenant = _extract_rid_locator_uuid(workspace.org)

    s3_path = upload_multipart_file(
        clients.auth_header,
        workspace_rid,
        path,
        clients.upload,
        file_type=FileTypes.CSV,
        header_provider=clients.header_provider,
    )

    presigned_url = _presign_download(clients, s3_path)

    header_line, first_data_line = _read_first_two_csv_lines(path)
    columns, archetype = _build_archetype(header_line, first_data_line)

    token = clients.auth_header.removeprefix("Bearer ")
    dagger_client = AuthenticatedClient(base_url=_dagger_base_url(clients), token=token)

    put_resp = put_object_space.sync_detailed(
        id=object_space,  # type: ignore[arg-type]
        body=PutObjectSpaceRequest(),
        tenant=tenant,
        client=dagger_client,
    )
    if put_resp.status_code >= 400:
        raise RuntimeError(
            f"Dagger PUT /v1/object-spaces failed: status={put_resp.status_code} body={put_resp.content!r}"
        )

    model_uuid = uuid.uuid4()
    import_request = ImportRequest(
        archetype=archetype,
        columns=columns,
        geometry_type=GeometryType.POINT,
        source_uri=presigned_url,
    )
    import_resp = post_import.sync_detailed(
        model_uuid=model_uuid,
        body=import_request,
        tenant=tenant,
        object_space=object_space,  # type: ignore[arg-type]
        client=dagger_client,
    )
    if import_resp.status_code != 202:
        raise RuntimeError(
            f"Dagger POST /v1/imports/{model_uuid} failed: "
            f"status={import_resp.status_code} body={import_resp.content!r}"
        )

    type_metadata = scout_spatial_api.SpatialTypeMetadata(
        point_cloud=scout_spatial_api.PointCloudMetadata(
            sensor_model=sensor_model,
            coordinate_system=coordinate_system,
            resolution_mm=resolution_mm,
            scan_pattern=(getattr(scout_spatial_api.ScanPattern, scan_pattern) if scan_pattern is not None else None),
        )
    )

    create_request = scout_spatial_api.CreateSpatialRequest(  # type: ignore[call-arg]
        title=name,
        dagger_uuid=str(model_uuid),
        type_metadata=type_metadata,
        labels=list(labels),
        properties=dict(properties) if properties else {},
        marking_rids=[],
        description=description,
        source_handle=api.Handle(s3=s3_path),
        workspace=workspace_rid,
    )

    spatial = clients.spatial.create(clients.auth_header, create_request)
    return spatial.rid


def _presign_download(clients: ClientsBunch, s3_path: str) -> str:
    """POST /signed-urls/v1/download → presigned GET URL.

    Uses raw HTTP rather than the SDK because the SignedUrlResource is not yet
    code-generated. Swap to the typed client when the SDK regenerates.
    """
    response = clients.spatial._request(
        "POST",
        clients.spatial._uri + _PRESIGN_DOWNLOAD_PATH,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": clients.auth_header,
        },
        params={},
        json={"path": s3_path},
    )
    url = response.json()["url"]
    assert isinstance(url, str)
    return url


def _dagger_base_url(clients: ClientsBunch) -> str:
    # scout's API base already ends in `/api`; dagger proxy is mounted at
    # `/api/dagger`, so appending `/dagger` gives the right outside URL.
    return clients._api_base_url.rstrip("/") + _DAGGER_PROXY_PATH


def _extract_rid_locator_uuid(rid: str) -> uuid.UUID:
    locator = rid.split(".", 4)[-1]
    return uuid.UUID(locator)


def _read_first_two_csv_lines(path: Path) -> tuple[str, str]:
    with path.open("r", newline="") as f:
        try:
            header = next(f).rstrip("\r\n")
        except StopIteration:
            raise ValueError(f"CSV is empty: {path}")
        try:
            first_data = next(f).rstrip("\r\n")
        except StopIteration:
            first_data = ""
    return header, first_data


def _build_archetype(header_line: str, data_line: str) -> tuple[ColumnSelection, Archetype]:
    if not header_line.strip():
        raise ValueError("CSV header is empty")
    headers = [h.strip() for h in header_line.split(",")]
    values = [v.strip() for v in data_line.split(",")] if data_line else []
    if len(values) < len(headers):
        values = values + [""] * (len(headers) - len(values))

    geometry_indices = _find_geometry_indices(headers)
    geom_set = set(geometry_indices)

    int_indices: list[int] = []
    real_indices: list[int] = []
    string_indices: list[int] = []
    attributes: list[Attribute] = []
    for i, name in enumerate(headers):
        if i in geom_set:
            continue
        kind = _classify(values[i])
        if kind == "int":
            int_indices.append(i)
            ty: object = _FseTypeInt.INT
        elif kind == "real":
            real_indices.append(i)
            ty = _FseTypeReal(real=RealMeasurement.INDEPENDENTVALUE)
        else:
            string_indices.append(i)
            ty = _FseTypeString.STRING
        attributes.append(Attribute(header=FseHeader(name=name, ty=ty), reductions=[]))  # type: ignore[arg-type]

    columns = ColumnSelection(
        bool_=[],
        geometry=geometry_indices,
        int_=int_indices,
        normal=[],
        real=real_indices,
        rgb=[],
        string=string_indices,
    )
    return columns, Archetype(attributes=attributes)


def _find_geometry_indices(headers: Sequence[str]) -> list[int]:
    lowered = [h.lower() for h in headers]
    try:
        return [lowered.index("x"), lowered.index("y"), lowered.index("z")]
    except ValueError as e:
        raise ValueError(f"CSV is missing required point-cloud columns x/y/z; got headers={list(headers)}") from e


def _classify(value: str) -> str:
    if not value:
        return "string"
    try:
        int(value)
        return "int"
    except ValueError:
        pass
    try:
        float(value)
        return "real"
    except ValueError:
        pass
    return "string"
