from __future__ import annotations

import logging
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Mapping, Sequence, get_args

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
    SamplerType,
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
from nominal_api import ingest_api

from nominal.core._clientsbunch import ClientsBunch
from nominal.core._types import PathLike
from nominal.core._utils.multipart import upload_multipart_file
from nominal.core.exceptions import NominalIngestError
from nominal.core.filetype import FileTypes
from nominal.core.spatial_asset import DaggerModel

if TYPE_CHECKING:
    from nominal.core.client import NominalClient

logger = logging.getLogger(__name__)

# scout's dagger reverse proxy is mounted at `@Path("/api/dagger")` in
# combined-service. `NominalClient._api_base_url` is the bare host (no `/api`
# suffix), so the constant must carry the full proxy prefix.
_DAGGER_PROXY_PATH = "/api/dagger"

# Per-column data type accepted in `create_dagger_model`'s `column_types` override
# and produced by the CSV sampling classifier.
ColumnDataType = Literal["int", "real", "string"]


def create_dagger_model(
    client: NominalClient,
    csv_path: PathLike,
    *,
    column_types: Mapping[str, ColumnDataType] | None = None,
) -> DaggerModel:
    """Upload a point-cloud CSV and import it into a Dagger model.

    The CSV must contain at minimum x, y, z columns (case-insensitive); remaining columns
    are auto-classified as int/real/string by sampling the first ~1000 data rows. Pass
    ``column_types`` to override inference for specific columns.

    Returns:
        A `DaggerModel` referencing the created model (uuid + the uploaded CSV's s3 source handle).

    Raises:
        FileNotFoundError: If ``csv_path`` does not exist.
        NominalIngestError: If the Dagger object-space or import request fails.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"No such file: {path}")

    clients = client._clients

    workspace = clients.resolve_workspace(None)
    workspace_rid = workspace.rid
    object_space = _extract_rid_locator_uuid(workspace_rid)
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

    header_line, sample_lines = _read_csv_header_and_samples(path)
    columns, archetype = _build_archetype(header_line, sample_lines, column_types or {})

    token = clients.auth_header.removeprefix("Bearer ")
    dagger_client = AuthenticatedClient(base_url=_dagger_base_url(clients), token=token)

    put_resp = put_object_space.sync_detailed(
        id=object_space,
        body=PutObjectSpaceRequest(),
        tenant=tenant,
        client=dagger_client,
    )
    if put_resp.status_code >= 400:
        raise NominalIngestError(
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
        object_space=object_space,
        client=dagger_client,
    )
    if import_resp.status_code != 202:
        raise NominalIngestError(
            f"Dagger POST /v1/imports/{model_uuid} failed: "
            f"status={import_resp.status_code} body={import_resp.content!r}"
        )

    return DaggerModel(dagger_uuid=str(model_uuid), source_handle=s3_path)


def _presign_download(clients: ClientsBunch, s3_path: str) -> str:
    response = clients.upload.sign_download(
        clients.auth_header,
        ingest_api.SignDownloadRequest(path=s3_path),
    )
    return response.url


def _dagger_base_url(clients: ClientsBunch) -> str:
    # Tolerate both forms of base_url stored on ClientsBunch: bare host
    # ("https://api.gov.nominal.io") and host+"/api" (matches the docstring on
    # NominalClient.create). The proxy is mounted at "/api/dagger" in either
    # case, so strip a trailing "/api" before appending.
    base = clients._api_base_url.rstrip("/")
    if base.endswith("/api"):
        base = base[: -len("/api")]
    return base + _DAGGER_PROXY_PATH


def _extract_rid_locator_uuid(rid: str) -> uuid.UUID:
    locator = rid.split(".", 4)[-1]
    return uuid.UUID(locator)


# Sample size for column type inference. Picked large enough that an
# integer-valued first row for a float column (e.g. `stress=1` followed by
# `stress=0.998`) gets promoted to real, but small enough to stay cheap on
# multi-GB CSVs (only the first N rows are read, not the whole file).
_TYPE_INFERENCE_SAMPLE_ROWS = 1000


def _read_csv_header_and_samples(path: Path, n_samples: int = _TYPE_INFERENCE_SAMPLE_ROWS) -> tuple[str, list[str]]:
    """Read the header row + up to n_samples non-empty data rows."""
    with path.open("r", newline="") as f:
        try:
            header = next(f).rstrip("\r\n")
        except StopIteration:
            raise ValueError(f"CSV is empty: {path}")
        samples: list[str] = []
        for line in f:
            stripped = line.rstrip("\r\n")
            if stripped:
                samples.append(stripped)
            if len(samples) >= n_samples:
                break
    return header, samples


def _build_archetype(
    header_line: str,
    sample_lines: Sequence[str],
    column_type_overrides: Mapping[str, ColumnDataType] | None = None,
) -> tuple[ColumnSelection, Archetype]:
    overrides = column_type_overrides or {}
    if not header_line.strip():
        raise ValueError("CSV header is empty")
    headers = [h.strip() for h in header_line.split(",")]
    n_cols = len(headers)

    header_set = set(headers)
    unknown = [name for name in overrides if name not in header_set]
    if unknown:
        raise ValueError(
            f"column_types references columns not in CSV header: {sorted(unknown)}; available columns: {headers}"
        )
    valid_types = get_args(ColumnDataType)
    bad_types = {name: ty for name, ty in overrides.items() if ty not in valid_types}
    if bad_types:
        raise ValueError(f"column_types values must be one of {sorted(valid_types)}: got {bad_types}")

    parsed_samples: list[list[str]] = []
    for line in sample_lines:
        row = [v.strip() for v in line.split(",")]
        if len(row) < n_cols:
            row = row + [""] * (n_cols - len(row))
        parsed_samples.append(row)

    geometry_indices = _find_geometry_indices(headers)
    geom_set = set(geometry_indices)

    int_indices: list[int] = []
    real_indices: list[int] = []
    string_indices: list[int] = []
    attributes: list[Attribute] = []
    for i, name in enumerate(headers):
        if i in geom_set:
            continue
        # Caller-supplied type wins; fall through to sample-based inference.
        kind = overrides.get(name)
        if kind is None:
            col_values = [row[i] for row in parsed_samples]
            kind = _classify_column(col_values)
        # Reductions are pre-computed aggregations (per-partition Min /
        # Max / Mean / etc.) stored as separate columns at ingest time.
        # The renderer's hierarchical LOD pipeline samples them at coarse
        # zoom levels — without them, the attribute can't drive ramp
        # coloring or ValueRange filtering at all.
        #
        # Real attributes get Min + Max + Mean. Int attributes get Min +
        # Max only — `SamplerType.MEAN` is not a valid pairing with an
        # Int-typed attribute. Min/Max alone still satisfy
        # `VolumetricFilter::ValueRange` (a two-sided filter) and drive
        # `ColorSource::Ramp` for Geometry coloring.
        #
        # String / bool attributes have no useful scalar aggregation,
        # so we leave their reductions empty.
        reductions: list[SamplerType] = []
        if kind == "int":
            int_indices.append(i)
            ty: _FseTypeInt | _FseTypeReal | _FseTypeString = _FseTypeInt.INT
            reductions = [SamplerType.MIN, SamplerType.MAX]
        elif kind == "real":
            real_indices.append(i)
            ty = _FseTypeReal(real=RealMeasurement.INDEPENDENTVALUE)
            reductions = [SamplerType.MIN, SamplerType.MAX, SamplerType.MEAN]
        else:
            string_indices.append(i)
            ty = _FseTypeString.STRING
        attributes.append(
            Attribute(
                header=FseHeader(name=name, ty=ty),
                reductions=reductions,
            )
        )

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


def _classify_column(values: Sequence[str]) -> ColumnDataType:
    """Most permissive type that covers every non-empty sample value.

    Any non-numeric value forces string. A mix of int- and float-looking
    values promotes to real (so a column whose first row is "1" but later
    rows are "0.998" classifies as real, not int). All-empty defaults to
    string, matching the legacy single-row behavior for columns the sample
    happens not to populate.
    """
    seen_real = False
    nonempty = 0
    for v in values:
        if not v:
            continue
        nonempty += 1
        kind = _classify(v)
        if kind == "string":
            return "string"
        if kind == "real":
            seen_real = True
    if not nonempty:
        return "string"
    return "real" if seen_real else "int"


def _find_geometry_indices(headers: Sequence[str]) -> list[int]:
    lowered = [h.lower() for h in headers]
    try:
        return [lowered.index("x"), lowered.index("y"), lowered.index("z")]
    except ValueError as e:
        raise ValueError(f"CSV is missing required point-cloud columns x/y/z; got headers={list(headers)}") from e


def _classify(value: str) -> ColumnDataType:
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
