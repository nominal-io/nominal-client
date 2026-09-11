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
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Iterable, Literal, Mapping, Sequence, TypeAlias, get_args

from nominal.core._types import PathLike
from nominal.ts import _MICROSECONDS_PER_TIME_UNIT, _LiteralTimeUnit

logger = logging.getLogger(__name__)

# Per-column data type accepted in the `column_types` override and produced by
# the CSV sampling classifier. This is the client's own vocabulary, not the
# importer's -- callers write `column_types={"count": "int"}` -- so it stays a
# Literal rather than becoming an enum.
ColumnDataType = Literal["int", "real", "string"]

# Sample size for column type inference. Only the first N rows are classified,
# so this stays cheap on multi-GB CSVs.
_TYPE_INFERENCE_SAMPLE_ROWS = 1000


# --- the importer's wire vocabulary -------------------------------------------
#
# These mirror the enums in the importer's OpenAPI schema, which the backend
# deserializes with FAIL_ON_UNKNOWN_PROPERTIES: a value it does not recognise
# fails the request outright. They are PascalCase, not the SCREAMING_CASE used
# by conjure enums. Members this module never emits are listed anyway, so the
# full set is visible without going back to the schema.


class _GeometryType(str, Enum):
    """Runtime label for the geometry each row carries. Only points are produced here."""

    AABB = "Aabb"
    BALL = "Ball"
    POINT = "Point"


class _Sampler(str, Enum):
    """A pre-computed aggregation stored alongside an attribute for coarse zoom levels.

    The renderer's hierarchical LOD pipeline samples these; without at least one,
    an attribute cannot drive ramp colouring or value-range filtering at all.
    """

    MIN = "Min"
    MAX = "Max"
    MEAN = "Mean"
    MODE = "Mode"
    AND = "And"
    OR = "Or"


class _RealMeasurement(str, Enum):
    """How a real quantity relates to the geometry, which is what CSG operates on."""

    INDEPENDENT_VALUE = "IndependentValue"
    VALUE_BY_WEIGHT = "ValueByWeight"
    VALUE_BY_VOLUME = "ValueByVolume"
    DENSITY = "Density"


class _ScalarAttributeType(str, Enum):
    """The attribute types that serialize as a bare string."""

    INT = "Int"
    STRING = "String"
    RGB = "Rgb"
    NORMAL = "Normal"
    BOOL = "Bool"
    UV = "Uv"

    def _to_wire(self) -> str:
        return self.value


@dataclass(frozen=True)
class _RealAttributeType:
    """The one attribute type that carries a value rather than serializing as a bare string."""

    measurement: _RealMeasurement = _RealMeasurement.INDEPENDENT_VALUE

    def _to_wire(self) -> dict[str, str]:
        return {"Real": self.measurement.value}


_AttributeType: TypeAlias = _ScalarAttributeType | _RealAttributeType


# --- the import config --------------------------------------------------------


@dataclass(frozen=True)
class _FseHeader:
    name: str
    ty: _AttributeType

    def _to_wire(self) -> dict[str, Any]:
        return {"name": self.name, "ty": self.ty._to_wire()}


@dataclass(frozen=True)
class _Attribute:
    """A named column plus the reductions the importer should build for it."""

    header: _FseHeader
    reductions: tuple[_Sampler, ...] = ()

    def _to_wire(self) -> dict[str, Any]:
        return {"header": self.header._to_wire(), "reductions": [sampler.value for sampler in self.reductions]}


@dataclass(frozen=True)
class _ColumnSelection:
    """Column indices per bucket.

    All seven buckets are required on the wire even when empty, which is why they
    are fields with defaults rather than keys assembled by hand.
    """

    geometry: tuple[int, ...]
    real: tuple[int, ...] = ()
    ints: tuple[int, ...] = ()
    string: tuple[int, ...] = ()
    rgb: tuple[int, ...] = ()
    normal: tuple[int, ...] = ()
    bools: tuple[int, ...] = ()

    def _to_wire(self) -> dict[str, list[int]]:
        return {
            "geometry": list(self.geometry),
            "real": list(self.real),
            "int": list(self.ints),
            "string": list(self.string),
            "rgb": list(self.rgb),
            "normal": list(self.normal),
            "bool": list(self.bools),
        }


@dataclass(frozen=True)
class _ImportConfig:
    """The v2 `ImportRequest` body, minus `source_uri`.

    The backend fills `source_uri` in from the presigned URL it derives for the
    uploaded object. The rejected v1 shape carried `geometry_type` and `columns`
    at the top level rather than under `format`.
    """

    attributes: tuple[_Attribute, ...]
    columns: _ColumnSelection
    geometry_type: _GeometryType = _GeometryType.POINT

    def _to_wire(self) -> dict[str, Any]:
        return {
            "archetype": {"attributes": [attribute._to_wire() for attribute in self.attributes]},
            "format": {
                "kind": "csv",
                "geometry_type": self.geometry_type.value,
                "columns": self.columns._to_wire(),
            },
        }


# The importer assigns each column an attribute slot by walking the buckets in
# this order -- not the order the columns appear in the header. The archetype has
# to be declared in the same order, or attribute k is named and typed after one
# column while holding another column's values. (The walk continues rgb, normal,
# bool; the last two are always empty here.)
_BUCKET_ORDER: tuple[ColumnDataType, ...] = ("real", "int", "string")

# Wire type and reductions per column type. Mean is not a valid pairing with an
# Int-typed attribute, so int gets Min + Max only; those two alone still satisfy
# a two-sided value-range filter and drive ramp colouring for geometry. String
# attributes have no useful scalar aggregation, so their reductions stay empty.
_WIRE_TYPE_AND_REDUCTIONS: Mapping[ColumnDataType, tuple[_AttributeType, tuple[_Sampler, ...]]] = MappingProxyType(
    {
        "real": (_RealAttributeType(), (_Sampler.MIN, _Sampler.MAX, _Sampler.MEAN)),
        "int": (_ScalarAttributeType.INT, (_Sampler.MIN, _Sampler.MAX)),
        "string": (_ScalarAttributeType.STRING, ()),
    }
)


@dataclass(frozen=True)
class _PointCloudCsv:
    """A point-cloud CSV, described for the ingest request."""

    path: Path
    import_config: _ImportConfig
    time_range_us: tuple[int, int] | None
    """Measured `(start, end)` extent of the time column in microseconds, or None if none was named.

    The ingest API has no field for it, so the caller records it on the spatial.
    """


@dataclass(frozen=True)
class _CsvScan:
    """Everything one pass over the file yields."""

    headers: tuple[str, ...]
    samples: tuple[tuple[str, ...], ...]
    """The first `_TYPE_INFERENCE_SAMPLE_ROWS` rows, split and padded to the header width."""
    time_extent: tuple[float, float] | None
    """Raw `(min, max)` of the time column in its own units, or None if none was named."""


def _describe_point_cloud_csv(
    csv_path: PathLike,
    *,
    column_types: Mapping[str, ColumnDataType] | None = None,
    rgb_column: str | None = None,
    rgb_attribute: str = "color",
    timestamp_column: str | None = None,
    time_unit: _LiteralTimeUnit = "seconds",
) -> _PointCloudCsv:
    """Read a point-cloud CSV and build everything the ingest request needs from it.

    Runs before the upload so a malformed CSV fails fast, rather than after
    pushing potentially many GB to object storage.

    Raises:
        FileNotFoundError: If `csv_path` does not exist.
        ValueError: If the CSV is empty, uses quoting, lacks x/y/z columns,
            `column_types` names a column or type that does not exist,
            `rgb_column` is not in the header, or `timestamp_column` is missing from
            the header or holds a non-numeric value.
    """
    if time_unit not in _MICROSECONDS_PER_TIME_UNIT:
        raise ValueError(f"time_unit must be one of {sorted(_MICROSECONDS_PER_TIME_UNIT)}: got {time_unit!r}")

    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"No such file: {path}")

    scan = _scan_csv(path, timestamp_column)
    return _PointCloudCsv(
        path=path,
        import_config=_build_import_config(
            scan, column_types or {}, rgb_column=rgb_column, rgb_attribute=rgb_attribute
        ),
        time_range_us=None if scan.time_extent is None else _to_microseconds(scan.time_extent, time_unit),
    )


# --- reading the file ---------------------------------------------------------


def _scan_csv(path: Path, timestamp_column: str | None, n_samples: int = _TYPE_INFERENCE_SAMPLE_ROWS) -> _CsvScan:
    """Read the file once to sample rows, measure the time column, and reject quoting.

    One pass answers all three because their needs overlap: classification wants
    the first `n_samples` rows, the time extent wants every row, and the quoting
    check wants every row too. A prefix-only quoting check is not enough -- a
    quoted field anywhere in the file shifts every attribute after it -- and one
    sequential local read is cheap against an upload of the same bytes.

    The extent has to come from every row rather than the sample: the renderer
    interpolates the playhead across it, so a maximum short of the real one clips
    the tail of the cloud and one past it stalls the sweep before the end.
    """
    minimum = math.inf
    maximum = -math.inf
    samples: list[tuple[str, ...]] = []

    with path.open("r", newline="") as f:
        try:
            header_line = next(f).rstrip("\r\n")
        except StopIteration:
            raise ValueError(f"CSV is empty: {path}") from None
        _reject_quoted_fields(header_line, "the header")

        headers = tuple(h.strip() for h in header_line.split(","))
        if not any(headers):
            raise ValueError("CSV header is empty")
        index = None if timestamp_column is None else _column_index(headers, timestamp_column, "timestamp_column")

        for line_number, line in enumerate(f, start=2):
            row = line.rstrip("\r\n")
            if not row:
                continue
            _reject_quoted_fields(row, f"line {line_number}")

            if len(samples) < n_samples:
                samples.append(_split_row(row, len(headers)))

            if index is not None:
                value = _time_value(row, index, timestamp_column, line_number)
                if value is not None:
                    # Plain comparisons rather than min()/max(): this runs once per
                    # row of a file that can hold tens of millions.
                    minimum = min(minimum, value)
                    maximum = max(maximum, value)

    if index is None:
        return _CsvScan(headers=headers, samples=tuple(samples), time_extent=None)
    if minimum > maximum:
        raise ValueError(f"timestamp_column {timestamp_column!r} has no values to derive a time range from")
    return _CsvScan(headers=headers, samples=tuple(samples), time_extent=(minimum, maximum))


def _reject_quoted_fields(line: str, where: str) -> None:
    """Refuse a CSV that uses quoting.

    The importer does not implement it: it splits rows on raw commas and counts
    columns with a plain memchr, with no quote handling anywhere. Parsing quotes
    here would be worse than not, because the column indices this module computes
    would then disagree with the ones the importer actually reads, silently
    shifting every attribute after the quoted field. Rejecting is the only option
    that cannot corrupt the result.
    """
    if '"' in line:
        raise ValueError(
            f"CSV quoting is not supported by the point cloud importer, but {where} contains a "
            f"double quote: {line[:120]!r}. Remove quoting, or replace commas inside fields."
        )


def _split_row(row: str, n_cols: int) -> tuple[str, ...]:
    """Split a sampled row and pad it to the header width, so column lookups cannot go out of range."""
    fields = [value.strip() for value in row.split(",")]
    if len(fields) < n_cols:
        fields.extend([""] * (n_cols - len(fields)))
    return tuple(fields)


def _field(row: str, index: int) -> str | None:
    """The index-th comma-separated field, or None if the row has fewer fields than that.

    Walks commas rather than splitting: the caller wants one field out of a row
    that may hold dozens, on every row of a very large file, and `split` would
    allocate a string per column to reach it.
    """
    start = 0
    for _ in range(index):
        comma = row.find(",", start)
        if comma < 0:
            return None
        start = comma + 1
    end = row.find(",", start)
    return row[start:] if end < 0 else row[start:end]


def _time_value(row: str, index: int, timestamp_column: str | None, line_number: int) -> float | None:
    """Parse the time column out of a row, or None when the row does not carry one."""
    raw = _field(row, index)
    if raw is None:
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        raise ValueError(
            f"timestamp_column {timestamp_column!r} holds a non-numeric value {raw!r} on line {line_number}"
        ) from None


def _to_microseconds(extent: tuple[float, float], time_unit: _LiteralTimeUnit) -> tuple[int, int]:
    """Convert a raw extent to whole microseconds, which is what a spatial stores.

    Widened outward so rounding can never land inside the data and clip the first
    or last points.
    """
    micros = _MICROSECONDS_PER_TIME_UNIT[time_unit]
    minimum, maximum = extent
    return math.floor(minimum * micros), math.ceil(maximum * micros)


# --- building the config ------------------------------------------------------


def _build_import_config(
    scan: _CsvScan,
    column_type_overrides: Mapping[str, ColumnDataType] | None = None,
    *,
    rgb_column: str | None = None,
    rgb_attribute: str,
) -> _ImportConfig:
    """Assign every column to a bucket and declare the archetype in the importer's walk order."""
    overrides = column_type_overrides or {}
    headers = scan.headers
    _validate_overrides(overrides, headers)

    geometry = _find_geometry_indices(headers)
    rgb = () if rgb_column is None else (_column_index(headers, rgb_column, "rgb_column"),)
    # Geometry and colour columns are excluded from scalar classification: a hex
    # colour cell would otherwise be sampled as a string column.
    reserved = {*geometry, *rgb}

    # One bucket per column type is the single source of truth: both the archetype
    # and the column selection are derived from it, so they cannot drift apart.
    columns: dict[ColumnDataType, list[tuple[int, str]]] = {kind: [] for kind in _BUCKET_ORDER}
    for i, name in enumerate(headers):
        if i in reserved:
            continue
        # Caller-supplied type wins; fall through to sample-based inference.
        kind = overrides.get(name) or _classify_column(row[i] for row in scan.samples)
        columns[kind].append((i, name))

    attributes = [_attribute(name, kind) for kind in _BUCKET_ORDER for _, name in columns[kind]]
    if rgb:
        # An Rgb attribute is ONE csv column holding a six-character hex string
        # ("rrggbb", no leading #). The importer reads that cell with
        # `u8::from_str_radix` on three 2-character slices and skips it entirely
        # unless it is exactly six characters -- three separate 0-255 numeric
        # columns parse to nothing and every point ends up black.
        #
        # Mean is the reduction that makes sense at coarse LOD: a parent node
        # takes the average colour of the points it stands in for, and the mean
        # sampler is implemented for Rgb8. Without at least one reduction the
        # renderer reports the attribute as not colourable and falls back to
        # solid white.
        attributes.append(
            _Attribute(header=_FseHeader(name=rgb_attribute, ty=_ScalarAttributeType.RGB), reductions=(_Sampler.MEAN,))
        )

    return _ImportConfig(
        attributes=tuple(attributes),
        columns=_ColumnSelection(
            geometry=geometry,
            real=tuple(i for i, _ in columns["real"]),
            ints=tuple(i for i, _ in columns["int"]),
            string=tuple(i for i, _ in columns["string"]),
            rgb=rgb,
        ),
    )


def _validate_overrides(overrides: Mapping[str, ColumnDataType], headers: Sequence[str]) -> None:
    """Reject overrides naming a column or a type that does not exist."""
    unknown = [name for name in overrides if name not in set(headers)]
    if unknown:
        raise ValueError(
            f"column_types references columns not in CSV header: {sorted(unknown)}; available columns: {list(headers)}"
        )
    valid_types = get_args(ColumnDataType)
    bad_types = {name: ty for name, ty in overrides.items() if ty not in valid_types}
    if bad_types:
        raise ValueError(f"column_types values must be one of {sorted(valid_types)}: got {bad_types}")


def _attribute(name: str, kind: ColumnDataType) -> _Attribute:
    """One archetype attribute: the column's wire type and its LOD reductions."""
    ty, reductions = _WIRE_TYPE_AND_REDUCTIONS[kind]
    return _Attribute(header=_FseHeader(name=name, ty=ty), reductions=reductions)


def _column_index(headers: Sequence[str], column: str, argument: str) -> int:
    """Resolve a caller-named column to its index.

    Matched exactly, unlike the x/y/z lookup: those are a fixed convention whose
    case varies between producers, where these are names the caller read off their
    own header.
    """
    try:
        return headers.index(column)
    except ValueError:
        raise ValueError(
            f"{argument} {column!r} is not in the CSV header; available columns: {list(headers)}"
        ) from None


def _find_geometry_indices(headers: Sequence[str]) -> tuple[int, int, int]:
    """Locate the x/y/z columns, case-insensitively."""
    lowered = [h.lower() for h in headers]
    try:
        return lowered.index("x"), lowered.index("y"), lowered.index("z")
    except ValueError as e:
        raise ValueError(f"CSV is missing required point-cloud columns x/y/z; got headers={list(headers)}") from e


def _classify_column(values: Iterable[str]) -> ColumnDataType:
    """Classify a column from sampled values: all-numeric is real, anything else is string.

    `int` is deliberately never inferred. Only the first
    `_TYPE_INFERENCE_SAMPLE_ROWS` rows are sampled, so an integer-looking sample
    is no evidence the rest of the column is integral -- and an Int-typed
    attribute truncates every float the importer reads into it, silently, for the
    whole file. Real represents integral values exactly well past any plausible
    point-cloud magnitude, so typing a genuine int column as real costs nothing
    that matters, while the reverse corrupts the data. Callers who want the Int
    wire type ask for it explicitly via `column_types`.

    All-empty columns default to string: there is nothing to measure, and string
    is the only type that cannot misrepresent the values.

    Returns on the first non-numeric value, so a string column costs one failed
    parse rather than one per sampled row.
    """
    populated = False
    for value in values:
        if not value:
            continue
        if not _is_numeric(value):
            return "string"
        populated = True
    return "real" if populated else "string"


def _is_numeric(value: str) -> bool:
    try:
        float(value)
    except ValueError:
        return False
    return True
