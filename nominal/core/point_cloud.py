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
import math
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
_FSE_TYPE_RGB = "Rgb"
_FSE_TYPE_REAL = {"Real": "IndependentValue"}

# An Rgb attribute is ONE csv column holding a six-character hex string
# ("rrggbb", no leading #). Quiche's parser reads that cell with
# `u8::from_str_radix` on three 2-character slices and skips the cell entirely
# if it is not exactly six characters -- three separate 0-255 numeric columns
# parse to nothing and every point ends up the default colour, black.
# It also needs at least one reduction, or the renderer reports the attribute as
# not colourable and falls back to solid white.
DEFAULT_RGB_ATTRIBUTE = "color"

DEFAULT_POINT_CLOUD_CHANNEL = "point_cloud"

# Per-column data type accepted in the `column_types` override and produced by
# the CSV sampling classifier.
ColumnDataType = Literal["int", "real", "string"]

# Unit of the values in a point cloud's time column. Mirrors the workbook's
# `SpatialTimeUnit`, which is what the renderer converts the stored range back
# into when it maps the playhead onto per-point values.
TimeUnit = Literal["ns", "us", "ms", "s"]

# Microseconds per unit. The spatial asset always stores its time range in
# microseconds, whatever unit the column itself is in.
_MICROS_PER: Mapping[TimeUnit, float] = {"ns": 1e-3, "us": 1.0, "ms": 1e3, "s": 1e6}


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
    rgb_column: str | None = None,
    rgb_attribute: str = DEFAULT_RGB_ATTRIBUTE,
    time_column: str | None = None,
    time_unit: TimeUnit = "s",
    channel: str = DEFAULT_POINT_CLOUD_CHANNEL,
    tags: Mapping[str, str] | None = None,
    workspace_rid: str | None = None,
) -> tuple[str, str | None, tuple[int, int] | None]:
    """Upload a point-cloud CSV and submit it to the spatial ingest pipeline.

    Returns:
        `(s3_path, ingest_job_rid, time_range_us)`, where `time_range_us` is the
        measured `(start, end)` extent of `time_column` in microseconds, or None
        when no time column was named. The ingest API has no field for it, so the
        caller records it on the spatial asset.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"No such file: {path}")

    # Inference runs before the upload so a malformed CSV fails fast, rather
    # than after pushing potentially many GB to object storage.
    header_line, sample_lines = _read_csv_header_and_samples(path)
    import_config = _build_import_config(
        header_line, sample_lines, column_types or {}, rgb_column=rgb_column, rgb_attribute=rgb_attribute
    )
    time_range = None if time_column is None else _read_time_range(path, header_line, time_column, time_unit)

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
    return s3_path, response.ingest_job_rid, time_range


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


def _read_time_range(
    path: Path,
    header_line: str,
    time_column: str,
    time_unit: TimeUnit,
) -> tuple[int, int]:
    """Scan the time column and return its extent, in microseconds.

    The renderer maps the workbook playhead onto per-point time by interpolating
    across this range, so it has to be the true extent rather than an estimate
    from the sampled rows -- a range short of the real maximum clips the tail of
    the cloud, and one past it stalls the sweep before the end.

    That means a full pass over the file. Only this one column is parsed, and
    nothing is retained, so the cost is a read of the file rather than a parse
    of it.
    """
    if time_unit not in _MICROS_PER:
        raise ValueError(f"time_unit must be one of {sorted(_MICROS_PER)}: got {time_unit!r}")

    headers = [h.strip() for h in header_line.split(",")]
    try:
        index = headers.index(time_column)
    except ValueError:
        raise ValueError(
            f"time_column {time_column!r} is not in the CSV header; available columns: {headers}"
        ) from None

    minimum = math.inf
    maximum = -math.inf
    with path.open("r", newline="") as f:
        next(f)  # header, already parsed by the caller
        for line_number, line in enumerate(f, start=2):
            row = line.rstrip("\r\n")
            if not row:
                continue
            fields = row.split(",")
            if index >= len(fields):
                continue
            raw = fields[index].strip()
            if not raw:
                continue
            try:
                value = float(raw)
            except ValueError:
                raise ValueError(
                    f"time_column {time_column!r} holds a non-numeric value {raw!r} on line {line_number}"
                ) from None
            minimum = min(minimum, value)
            maximum = max(maximum, value)

    if minimum > maximum:
        raise ValueError(f"time_column {time_column!r} has no values to derive a time range from")

    micros = _MICROS_PER[time_unit]
    # Widen to whole microseconds so rounding can never land inside the data and
    # clip the first or last points.
    return math.floor(minimum * micros), math.ceil(maximum * micros)


def _build_import_config(
    header_line: str,
    sample_lines: Sequence[str],
    column_type_overrides: Mapping[str, ColumnDataType] | None = None,
    *,
    rgb_column: str | None = None,
    rgb_attribute: str = DEFAULT_RGB_ATTRIBUTE,
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
    rgb_indices = _find_rgb_index(headers, rgb_column)
    # Excluded from scalar classification: a hex colour cell would otherwise be
    # sampled as a string column.
    rgb_set = set(rgb_indices)

    int_indices: list[int] = []
    real_indices: list[int] = []
    string_indices: list[int] = []
    # Quiche assigns each column an attribute slot by walking the buckets in the
    # order real, int, string, rgb, normal, bool -- not the order the columns
    # appear in the header. The archetype has to be declared in that same order
    # or attribute k is named and typed after one column while holding another
    # column's values.
    by_bucket: dict[str, list[dict[str, Any]]] = {"real": [], "int": [], "string": []}
    for i, name in enumerate(headers):
        if i in geom_set or i in rgb_set:
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
        by_bucket[kind].append({"header": {"name": name, "ty": ty}, "reductions": reductions})

    attributes = [*by_bucket["real"], *by_bucket["int"], *by_bucket["string"]]
    if rgb_indices:
        # Mean is the reduction that makes sense at coarse LOD: a parent node
        # takes the average colour of the points it stands in for. `MeanSampler`
        # is implemented for Rgb8, so this is a valid pairing.
        attributes.append({"header": {"name": rgb_attribute, "ty": _FSE_TYPE_RGB}, "reductions": [_SAMPLER_MEAN]})

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
                "rgb": rgb_indices,
                "normal": [],
                "bool": [],
            },
        },
    }


def _find_rgb_index(headers: Sequence[str], rgb_column: str | None) -> list[int]:
    """Resolve the colour column name to a single index, as a list for the wire shape."""
    if rgb_column is None:
        return []
    try:
        return [headers.index(rgb_column)]
    except ValueError:
        raise ValueError(
            f"rgb_column {rgb_column!r} is not in the CSV header; available columns: {list(headers)}"
        ) from None


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
