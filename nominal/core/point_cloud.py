"""Describe a point-cloud CSV for the spatial ingest pipeline.

The platform owns the indexing side of a point-cloud ingest: given an uploaded
source object and an import config, it presigns the source, ensures the
workspace's object space exists, starts the import, and polls it to completion.
The client's job is to upload the CSV and describe its column layout.

That description -- the archetype and column selection -- is domain knowledge
only the producer of the file has, so it is built here rather than inferred
server-side.

Everything in this module is pure: it reads a local file and returns a
description of it. All calls against the platform live in `nominal.core.spatial`.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, Mapping, Sequence, get_args

from nominal.core._types import PathLike
from nominal.ts import _MICROSECONDS_PER_TIME_UNIT, _LiteralTimeUnit

logger = logging.getLogger(__name__)

# Wire values for the v2 `ImportRequest` body carried by
# `PointCloudOpts.daggerImportConfig`. The backend deserializes that `any`
# payload with FAIL_ON_UNKNOWN_PROPERTIES and rejects the legacy v1 shape (which
# carried `geometry_type` and `columns` at the top level rather than under
# `format`), so these strings must match the importer's OpenAPI enums exactly.
# They are PascalCase, not the SCREAMING_CASE used by conjure enums.
_GEOMETRY_TYPE_POINT = "Point"
_SAMPLER_MIN = "Min"
_SAMPLER_MAX = "Max"
_SAMPLER_MEAN = "Mean"
_FSE_TYPE_INT = "Int"
_FSE_TYPE_STRING = "String"
_FSE_TYPE_RGB = "Rgb"
_FSE_TYPE_REAL = {"Real": "IndependentValue"}

# An Rgb attribute is ONE csv column holding a six-character hex string
# ("rrggbb", no leading #). The importer reads that cell with
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

# The importer assigns each column an attribute slot by walking the buckets in
# this order -- not the order the columns appear in the header. The archetype
# has to be declared in the same order, or attribute k is named and typed after
# one column while holding another column's values. (The full walk continues
# rgb, normal, bool; the last two are always empty here.)
_BUCKET_ORDER: tuple[ColumnDataType, ...] = ("real", "int", "string")

# Wire type and LOD reductions per column type.
#
# Reductions are pre-computed aggregations (per-partition Min / Max / Mean)
# stored as separate columns at ingest time. The renderer's hierarchical LOD
# pipeline samples them at coarse zoom levels -- without them the attribute
# cannot drive ramp colouring or value-range filtering at all.
#
# Mean is not a valid pairing with an Int-typed attribute, so int gets Min + Max
# only; those two alone still satisfy a two-sided value-range filter and drive
# ramp colouring for geometry. String attributes have no useful scalar
# aggregation, so their reductions stay empty.
_WIRE_TYPE_AND_REDUCTIONS: Mapping[ColumnDataType, tuple[Any, tuple[str, ...]]] = MappingProxyType(
    {
        "real": (_FSE_TYPE_REAL, (_SAMPLER_MIN, _SAMPLER_MAX, _SAMPLER_MEAN)),
        "int": (_FSE_TYPE_INT, (_SAMPLER_MIN, _SAMPLER_MAX)),
        "string": (_FSE_TYPE_STRING, ()),
    }
)

# Sample size for column type inference. Only the first N rows are read, not the
# whole file, so this stays cheap on multi-GB CSVs.
_TYPE_INFERENCE_SAMPLE_ROWS = 1000


@dataclass(frozen=True)
class _PointCloudCsv:
    """A point-cloud CSV, described for the ingest request."""

    path: Path
    import_config: dict[str, Any]
    time_range_us: tuple[int, int] | None
    """Measured `(start, end)` extent of the time column in microseconds, or None if none was named.

    The ingest API has no field for it, so the caller records it on the spatial asset.
    """


def _describe_point_cloud_csv(
    csv_path: PathLike,
    *,
    column_types: Mapping[str, ColumnDataType] | None = None,
    rgb_column: str | None = None,
    rgb_attribute: str = DEFAULT_RGB_ATTRIBUTE,
    time_column: str | None = None,
    time_unit: _LiteralTimeUnit = "seconds",
) -> _PointCloudCsv:
    """Read a point-cloud CSV and build everything the ingest request needs from it.

    Runs before the upload so a malformed CSV fails fast, rather than after
    pushing potentially many GB to object storage.

    Raises:
        FileNotFoundError: If `csv_path` does not exist.
        ValueError: If the CSV is empty, uses quoting, lacks x/y/z columns,
            `column_types` names a column or type that does not exist,
            `rgb_column` is not in the header, or `time_column` is missing from
            the header or holds a non-numeric value.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"No such file: {path}")

    header_line, sample_lines = _read_csv_header_and_samples(path)
    return _PointCloudCsv(
        path=path,
        import_config=_build_import_config(
            header_line, sample_lines, column_types or {}, rgb_column=rgb_column, rgb_attribute=rgb_attribute
        ),
        time_range_us=None if time_column is None else _read_time_range(path, header_line, time_column, time_unit),
    )


def _reject_quoted_fields(line: str, where: str) -> None:
    """Refuse a CSV that uses quoting.

    The importer does not implement it: it splits rows on raw commas and counts
    columns with a plain memchr, with no quote handling anywhere. Parsing quotes
    here would be worse than not, because the column indices this module
    computes would then disagree with the ones the importer actually reads,
    silently shifting every attribute after the quoted field. Rejecting is the
    only option that cannot corrupt the result.
    """
    if '"' in line:
        raise ValueError(
            f"CSV quoting is not supported by the point cloud importer, but {where} contains a "
            f"double quote: {line[:120]!r}. Remove quoting, or replace commas inside fields."
        )


def _read_csv_header_and_samples(path: Path, n_samples: int = _TYPE_INFERENCE_SAMPLE_ROWS) -> tuple[str, list[str]]:
    """Read the header row + up to n_samples non-empty data rows."""
    with path.open("r", newline="") as f:
        try:
            header = next(f).rstrip("\r\n")
        except StopIteration:
            raise ValueError(f"CSV is empty: {path}")
        _reject_quoted_fields(header, "the header")
        samples: list[str] = []
        for line in f:
            stripped = line.rstrip("\r\n")
            if stripped:
                _reject_quoted_fields(stripped, f"data row {len(samples) + 2}")
                samples.append(stripped)
            if len(samples) >= n_samples:
                break
    return header, samples


def _read_time_range(
    path: Path,
    header_line: str,
    time_column: str,
    time_unit: _LiteralTimeUnit,
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
    if time_unit not in _MICROSECONDS_PER_TIME_UNIT:
        raise ValueError(f"time_unit must be one of {sorted(_MICROSECONDS_PER_TIME_UNIT)}: got {time_unit!r}")

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

    micros = _MICROSECONDS_PER_TIME_UNIT[time_unit]
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
    """Build the v2 `ImportRequest` body, minus `source_uri`.

    The backend fills `source_uri` in from the presigned URL it derives for the
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
    rgb_indices = _find_rgb_index(headers, rgb_column)
    # Geometry and colour columns are excluded from scalar classification: a hex
    # colour cell would otherwise be sampled as a string column.
    reserved = {*geometry_indices, *rgb_indices}

    # One bucket per column type is the single source of truth here: both the
    # archetype and the column selection below are derived from it, so they
    # cannot drift out of step.
    columns: dict[ColumnDataType, list[tuple[int, str]]] = {kind: [] for kind in _BUCKET_ORDER}
    for i, name in enumerate(headers):
        if i in reserved:
            continue
        # Caller-supplied type wins; fall through to sample-based inference.
        kind = overrides.get(name) or _classify_column([row[i] for row in parsed_samples])
        columns[kind].append((i, name))

    attributes = [_attribute(name, kind) for kind in _BUCKET_ORDER for _, name in columns[kind]]
    if rgb_indices:
        # Mean is the reduction that makes sense at coarse LOD: a parent node
        # takes the average colour of the points it stands in for. The mean
        # sampler is implemented for Rgb8, so this is a valid pairing.
        attributes.append({"header": {"name": rgb_attribute, "ty": _FSE_TYPE_RGB}, "reductions": [_SAMPLER_MEAN]})

    return {
        "archetype": {"attributes": attributes},
        "format": {
            "kind": "csv",
            "geometry_type": _GEOMETRY_TYPE_POINT,
            "columns": {
                "geometry": geometry_indices,
                "real": [i for i, _ in columns["real"]],
                "int": [i for i, _ in columns["int"]],
                "string": [i for i, _ in columns["string"]],
                "rgb": rgb_indices,
                "normal": [],
                "bool": [],
            },
        },
    }


def _attribute(name: str, kind: ColumnDataType) -> dict[str, Any]:
    """One archetype attribute: the column's wire type and its LOD reductions."""
    ty, reductions = _WIRE_TYPE_AND_REDUCTIONS[kind]
    return {"header": {"name": name, "ty": ty}, "reductions": [*reductions]}


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


def _find_geometry_indices(headers: Sequence[str]) -> list[int]:
    lowered = [h.lower() for h in headers]
    try:
        return [lowered.index("x"), lowered.index("y"), lowered.index("z")]
    except ValueError as e:
        raise ValueError(f"CSV is missing required point-cloud columns x/y/z; got headers={list(headers)}") from e


def _classify_column(values: Sequence[str]) -> ColumnDataType:
    """Classify a column from sampled values: all-numeric is real, anything else is string.

    `int` is deliberately never inferred. Only the first
    `_TYPE_INFERENCE_SAMPLE_ROWS` rows are sampled, so an integer-looking sample
    is no evidence the rest of the column is integral -- and an Int-typed
    attribute truncates every float the importer reads into it, silently, for
    the whole file. Real represents integral values exactly well past any
    plausible point-cloud magnitude, so typing a genuine int column as real
    costs nothing that matters, while the reverse corrupts the data. Callers
    who want the Int wire type ask for it explicitly via `column_types`.

    All-empty columns default to string: there is nothing to measure, and string
    is the only type that cannot misrepresent the values.
    """
    populated = [v for v in values if v]
    if not populated:
        return "string"
    return "real" if all(_is_numeric(v) for v in populated) else "string"


def _is_numeric(value: str) -> bool:
    try:
        float(value)
    except ValueError:
        return False
    return True
