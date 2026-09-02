"""Point-cloud CSV ingest against the spatial assets pipeline.

Scout owns the Dagger side of a point-cloud ingest: given a source object and a
`daggerImportConfig`, it presigns the source, ensures the workspace's object
space exists, starts the Dagger import, and polls it to completion. The client's
job is to upload the CSV and describe its column layout.

That description -- the archetype and column selection -- is domain knowledge
only the producer of the file has, so it is built here rather than inferred
server-side.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Literal, Mapping, Protocol, Sequence, get_args

from nominal_api import ingest_api, upload_api

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._types import PathLike
from nominal.core._utils.multipart import upload_multipart_file
from nominal.core.filetype import FileTypes

logger = logging.getLogger(__name__)

# Wire values for the Dagger v2 `ImportRequest` body carried by
# `PointCloudOpts.daggerImportConfig`. Scout deserializes that `any` payload
# with FAIL_ON_UNKNOWN_PROPERTIES and rejects the legacy v1 shape (which carried
# `geometry_type` and `columns` at the top level rather than under `format`), so
# these strings must match Dagger's OpenAPI enums exactly. They are PascalCase,
# not the SCREAMING_CASE used by conjure enums.
_GEOMETRY_TYPE_POINT = "Point"
_SAMPLER_MIN = "Min"
_SAMPLER_MAX = "Max"
_SAMPLER_MEAN = "Mean"
_FSE_TYPE_INT = "Int"
_FSE_TYPE_STRING = "String"
_FSE_TYPE_REAL = {"Real": "IndependentValue"}

DEFAULT_POINT_CLOUD_CHANNEL = "point_cloud"

# Per-column data type accepted in the `column_types` override and produced by
# the CSV sampling classifier.
ColumnDataType = Literal["int", "real", "string"]


class _PointCloudClients(HasScoutParams, Protocol):
    @property
    def ingest(self) -> ingest_api.IngestService: ...
    @property
    def upload(self) -> upload_api.UploadService: ...


def _ingest_point_cloud_csv(
    clients: _PointCloudClients,
    spatial_rid: str,
    csv_path: PathLike,
    *,
    column_types: Mapping[str, ColumnDataType] | None = None,
    channel: str = DEFAULT_POINT_CLOUD_CHANNEL,
    tags: Mapping[str, str] | None = None,
    workspace_rid: str | None = None,
) -> tuple[str, str | None]:
    """Upload a point-cloud CSV and submit it to the spatial ingest pipeline.

    Returns:
        `(s3_path, ingest_job_rid)` for the submitted ingest.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"No such file: {path}")

    # Inference runs before the upload so a malformed CSV fails fast, rather
    # than after pushing potentially many GB to object storage.
    header_line, sample_lines = _read_csv_header_and_samples(path)
    import_config = _build_import_config(header_line, sample_lines, column_types or {})

    resolved_workspace_rid = clients.resolve_workspace(workspace_rid).rid
    s3_path = upload_multipart_file(
        clients.auth_header,
        resolved_workspace_rid,
        path,
        clients.upload,
        file_type=FileTypes.CSV,
        header_provider=clients.header_provider,
    )

    # The target must already exist: scout rejects `PointCloudIngestTarget.new`,
    # since the asset's daggerUuid is what names the model Dagger imports into.
    response = clients.ingest.ingest(
        clients.auth_header,
        ingest_api.IngestRequest(
            options=ingest_api.IngestOptions(
                point_cloud=ingest_api.PointCloudOpts(
                    source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path)),
                    target=ingest_api.PointCloudIngestTarget(
                        existing=ingest_api.ExistingSpatialIngestDestination(spatial_rid=spatial_rid)
                    ),
                    dagger_import_config=import_config,
                    channel=channel,
                    tags=dict(tags) if tags else {},
                )
            )
        ),
    )
    logger.debug(
        "submitted point cloud ingest for %s: spatial=%s ingest_job=%s", path, spatial_rid, response.ingest_job_rid
    )
    return s3_path, response.ingest_job_rid


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


def _build_import_config(
    header_line: str,
    sample_lines: Sequence[str],
    column_type_overrides: Mapping[str, ColumnDataType] | None = None,
) -> dict[str, Any]:
    """Build the Dagger v2 `ImportRequest` body, minus `source_uri`.

    Scout fills `source_uri` in from the presigned URL it derives for the
    uploaded object.
    """
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
    attributes: list[dict[str, Any]] = []
    for i, name in enumerate(headers):
        if i in geom_set:
            continue
        # Caller-supplied type wins; fall through to sample-based inference.
        kind = overrides.get(name)
        if kind is None:
            col_values = [row[i] for row in parsed_samples]
            kind = _classify_column(col_values)
        # Reductions are pre-computed aggregations (per-partition Min / Max /
        # Mean / etc.) stored as separate columns at ingest time. The renderer's
        # hierarchical LOD pipeline samples them at coarse zoom levels -- without
        # them, the attribute can't drive ramp coloring or ValueRange filtering
        # at all.
        #
        # Real attributes get Min + Max + Mean. Int attributes get Min + Max
        # only -- Mean is not a valid pairing with an Int-typed attribute.
        # Min/Max alone still satisfy `VolumetricFilter::ValueRange` (a two-sided
        # filter) and drive `ColorSource::Ramp` for Geometry coloring.
        #
        # String / bool attributes have no useful scalar aggregation, so we
        # leave their reductions empty.
        reductions: list[str] = []
        ty: Any
        if kind == "int":
            int_indices.append(i)
            ty = _FSE_TYPE_INT
            reductions = [_SAMPLER_MIN, _SAMPLER_MAX]
        elif kind == "real":
            real_indices.append(i)
            ty = _FSE_TYPE_REAL
            reductions = [_SAMPLER_MIN, _SAMPLER_MAX, _SAMPLER_MEAN]
        else:
            string_indices.append(i)
            ty = _FSE_TYPE_STRING
        attributes.append({"header": {"name": name, "ty": ty}, "reductions": reductions})

    return {
        "archetype": {"attributes": attributes},
        "format": {
            "kind": "csv",
            "geometry_type": _GEOMETRY_TYPE_POINT,
            "columns": {
                "geometry": geometry_indices,
                "real": real_indices,
                "int": int_indices,
                "string": string_indices,
                "rgb": [],
                "normal": [],
                "bool": [],
            },
        },
    }


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
