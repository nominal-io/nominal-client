"""Generate a wide variety of CSV/Parquet edge-case files and ingest them as one job.

Manual exercise tool (no tests). Run with --dry-run to generate + inspect files without a
backend, or with --profile <name> to create a dataset and kick off a single ingest job.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import json
import logging
import tempfile
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl
from nominal_streaming import NominalDatasetStream, PyNominalStreamOpts

from nominal.core import Dataset, IngestionJob, IngestionJobStatus, NominalClient
from nominal.core.exceptions import NominalIngestError, NominalIngestFailed
from nominal.experimental.ingest import IngestionJobBuilder
from nominal.ts import Custom, Relative, _AnyTimestampType

logger = logging.getLogger("edge_case_ingest")

TIMESTAMP_COLUMN = "timestamp"
_BASE = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
# Sub-second, sub-millisecond step (1.000271 s/row) so each row carries a distinct microsecond
# component. This exposes encodings that quantize timestamps: epoch_milliseconds truncates the
# sub-ms part, the float-encoded epoch units (s/min/hr/day) quantize to a ~256 ns grid, while
# iso_8601 / epoch_microseconds / custom-with-.SSSSSS round-trip exactly.
_STEP_NS = 1_000_271_000
_STEP = dt.timedelta(microseconds=_STEP_NS // 1000)
_UNIX_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)

# Java DateTimeFormatter pattern (Custom.format) -> Python strftime, for the custom files.
# Both carry .SSSSSS (microseconds) so the custom encoding itself is lossless.
_CUSTOM_STRFTIME = {
    "yyyy-MM-dd HH:mm:ss.SSSSSS": "%Y-%m-%d %H:%M:%S.%f",
    "DDD HH:mm:ss.SSSSSS": "%j %H:%M:%S.%f",
}

# epoch_<unit> -> multiplier so the column value is epoch_seconds * multiplier.
_EPOCH_MULTIPLIER = {
    "epoch_seconds": 1.0,
    "epoch_milliseconds": 1000.0,
    "epoch_microseconds": 1e6,
    "epoch_minutes": 1.0 / 60.0,
    "epoch_hours": 1.0 / 3600.0,
    "epoch_days": 1.0 / 86400.0,
}


def _instants(n: int) -> list[dt.datetime]:
    return [_BASE + i * _STEP for i in range(n)]


def _epoch_seconds(n: int) -> np.ndarray:
    return _BASE.timestamp() + np.arange(n, dtype=np.float64) * (_STEP_NS / 1e9)


def _start_seconds(start: dt.datetime | int) -> float:
    # Relative.start is a datetime or integral-nanoseconds-UTC.
    return start.timestamp() if isinstance(start, dt.datetime) else start / 1e9


def _timestamp_column(ts_type: _AnyTimestampType, n: int) -> pl.Series:
    """Build the timestamp column of length n, encoded to match ts_type."""
    if isinstance(ts_type, Relative):
        offsets = _epoch_seconds(n) - _start_seconds(ts_type.start)
        return pl.Series(TIMESTAMP_COLUMN, offsets, dtype=pl.Float64)
    if isinstance(ts_type, Custom):
        fmt = _CUSTOM_STRFTIME.get(ts_type.format)
        if fmt is None:
            raise ValueError(f"no strftime mapping for Custom.format {ts_type.format!r}; add it to _CUSTOM_STRFTIME")
        return pl.Series(TIMESTAMP_COLUMN, [t.strftime(fmt) for t in _instants(n)], dtype=pl.Utf8)
    if ts_type == "iso_8601":
        vals = [t.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z" for t in _instants(n)]
        return pl.Series(TIMESTAMP_COLUMN, vals, dtype=pl.Utf8)
    if ts_type == "epoch_nanoseconds":
        base_ns = int(_BASE.timestamp()) * 1_000_000_000
        ns = base_ns + np.arange(n, dtype=np.int64) * _STEP_NS
        return pl.Series(TIMESTAMP_COLUMN, ns, dtype=pl.Int64)
    if isinstance(ts_type, str) and ts_type in _EPOCH_MULTIPLIER:
        return pl.Series(TIMESTAMP_COLUMN, _epoch_seconds(n) * _EPOCH_MULTIPLIER[ts_type], dtype=pl.Float64)
    raise ValueError(f"unsupported timestamp type for generation: {ts_type!r}")


def _floats_nan(n: int, name: str) -> pl.Series:
    arr = np.arange(n, dtype=np.float64) + 0.5
    vals: list[float | None] = arr.tolist()
    if n > 2:
        vals[1] = float("nan")  # NaN (not null)
        vals[2] = None  # null (not NaN)
    return pl.Series(name, vals, dtype=pl.Float64)


# Narrow integer dtypes whose range is smaller than the row counts we generate; values are
# taken modulo the dtype's cardinality so columns stay dense and in-range (not null-filled).
_INT_MODULUS: dict[type[pl.DataType] | pl.DataType, int] = {
    pl.Int8: 128,
    pl.UInt8: 256,
    pl.Int16: 32768,
    pl.UInt16: 65536,
}


def _ints(n: int, name: str, dtype: type[pl.DataType] | pl.DataType = pl.Int64) -> pl.Series:
    arr = np.arange(n, dtype=np.int64)
    modulus = _INT_MODULUS.get(dtype)
    if modulus is not None:
        arr = arr % modulus
    return pl.Series(name, arr, dtype=dtype)


def _bools_null(n: int, name: str) -> pl.Series:
    vals: list[bool | None] = [(i % 2 == 0) for i in range(n)]
    if n > 0:
        vals[0] = None
    return pl.Series(name, vals, dtype=pl.Boolean)


def _strings_null(n: int, name: str) -> pl.Series:
    vals: list[str | None] = [f"s{i}" for i in range(n)]
    if n > 1:
        vals[1] = None
    if n > 2:
        vals[2] = ""  # empty string (distinct from null)
    return pl.Series(name, vals, dtype=pl.Utf8)


def _categorical(n: int, name: str) -> pl.Series:
    cats = ["alpha", "beta", "gamma"]
    return pl.Series(name, [cats[i % len(cats)] for i in range(n)], dtype=pl.Categorical)


def _extreme_floats(n: int, name: str) -> pl.Series:
    pool = [float("inf"), float("-inf"), 1e308, -1e308, 1e-308, 0.0, -0.0]
    return pl.Series(name, [pool[i % len(pool)] for i in range(n)], dtype=pl.Float64)


def _messy_strings(n: int, name: str) -> pl.Series:
    pool = ["has,comma", 'has"quote', "has\nnewline", "  padded  ", "unié☃", ""]
    return pl.Series(name, [pool[i % len(pool)] for i in range(n)], dtype=pl.Utf8)


def _all_null(n: int, name: str) -> pl.Series:
    return pl.Series(name, [None] * n, dtype=pl.Float64)


def _list_floats(n: int, name: str) -> pl.Series:
    rows: list[list[float] | None] = [[float(i), float(i) + 0.5, float("nan")] for i in range(n)]
    if n > 1:
        rows[1] = None  # null list
    if n > 2:
        rows[2] = []  # empty list
    return pl.Series(name, rows, dtype=pl.List(pl.Float64))


def _list_int(n: int, name: str, dtype: type[pl.DataType] | pl.DataType = pl.Int32) -> pl.Series:
    rows = [[i, i + 1, i + 2] for i in range(n)]
    return pl.Series(name, rows, dtype=pl.List(dtype))


def _list_strings(n: int, name: str) -> pl.Series:
    rows: list[list[str] | None] = [[f"a{i}", f"b{i}"] for i in range(n)]
    if n > 1:
        rows[1] = None
    return pl.Series(name, rows, dtype=pl.List(pl.Utf8))


def _struct(n: int, name: str) -> pl.Series:
    rows: list[dict[str, object] | None] = [{"x": float(i), "label": f"p{i}", "ok": i % 2 == 0} for i in range(n)]
    if n > 1:
        rows[1] = None  # null struct
    return pl.Series(name, rows, dtype=pl.Struct({"x": pl.Float64, "label": pl.Utf8, "ok": pl.Boolean}))


def _nested_struct(n: int, name: str) -> pl.Series:
    rows = [{"inner": {"a": float(i), "b": f"q{i}"}, "arr": [float(i), float(i) + 1.0]} for i in range(n)]
    dtype = pl.Struct({"inner": pl.Struct({"a": pl.Float64, "b": pl.Utf8}), "arr": pl.List(pl.Float64)})
    return pl.Series(name, rows, dtype=dtype)


def _list_of_struct(n: int, name: str) -> pl.Series:
    rows = [[{"k": float(i), "v": f"r{i}"}] for i in range(n)]
    return pl.Series(name, rows, dtype=pl.List(pl.Struct({"k": pl.Float64, "v": pl.Utf8})))


def _json_struct_strings(n: int, name: str) -> pl.Series:
    rows = [json.dumps({"x": i, "label": f"p{i}"}) for i in range(n)]
    return pl.Series(name, rows, dtype=pl.Utf8)


# Array columns need a distinct 32-hex uuid in the prefix so ClickHouse doesn't merge them into one Nested;
# struct columns just need the _nominal_struct_ marker.
def _array_prefixed(name: str) -> str:
    return f"_nominal_array_{uuid.uuid4().hex}.{name}"


def _struct_prefixed(name: str) -> str:
    return f"_nominal_struct_{name}"


@dataclass(frozen=True)
class FileSpec:
    name: str
    fmt: str  # "csv" | "parquet" | "avro"
    n_rows: int
    timestamp_type: _AnyTimestampType = "epoch_nanoseconds"  # ignored for avro (records carry ns)
    build_values: Callable[[int], pl.DataFrame] | None = None  # csv / parquet
    avro_writer: Callable[[Path, int], None] | None = None  # avro
    tag_columns: Mapping[str, str] | None = None
    tags: Mapping[str, str] | None = None


def _basic_values(n: int) -> pl.DataFrame:
    return pl.DataFrame([_floats_nan(n, "value"), _ints(n, "count"), _strings_null(n, "label")])


def generate(spec: FileSpec, out_dir: Path) -> Path:
    """Write the spec's file and return its path.

    Avro is written by its `avro_writer` (records carry their own ns timestamps); csv/parquet build
    a polars frame and prepend the encoded timestamp column.
    """
    path = out_dir / f"{spec.name}.{spec.fmt}"
    if spec.fmt == "avro":
        assert spec.avro_writer is not None, f"avro spec {spec.name!r} needs an avro_writer"
        spec.avro_writer(path, spec.n_rows)
        return path
    assert spec.build_values is not None, f"spec {spec.name!r} needs build_values"
    df = spec.build_values(spec.n_rows)
    df = df.insert_column(0, _timestamp_column(spec.timestamp_type, spec.n_rows))
    if spec.fmt == "csv":
        df.write_csv(path)
    elif spec.fmt == "parquet":
        df.write_parquet(path)
    else:
        raise ValueError(f"unknown format {spec.fmt!r} for spec {spec.name!r}")
    return path


def _values_messy(n: int) -> pl.DataFrame:
    return pl.DataFrame([_messy_strings(n, "text"), _extreme_floats(n, "extreme"), _ints(n, "count")])


def _values_nan_null(n: int) -> pl.DataFrame:
    return pl.DataFrame([_floats_nan(n, "f"), _all_null(n, "all_null"), _strings_null(n, "s")])


def _values_wide(n: int) -> pl.DataFrame:
    return pl.DataFrame([_floats_nan(n, f"ch_{i:03d}") for i in range(200)])


def _values_scalars(n: int) -> pl.DataFrame:
    return pl.DataFrame(
        [
            _ints(n, "i8", pl.Int8),
            _ints(n, "i16", pl.Int16),
            _ints(n, "i32", pl.Int32),
            _ints(n, "i64", pl.Int64),
            _ints(n, "u32", pl.UInt32),
            _ints(n, "u64", pl.UInt64),
            _floats_nan(n, "f64").cast(pl.Float32).alias("f32"),
            _floats_nan(n, "f64"),
            _bools_null(n, "flag"),
            _strings_null(n, "label"),
            _categorical(n, "cat"),
            _all_null(n, "empty"),
        ]
    )


def _values_arrays_native(n: int) -> pl.DataFrame:
    return pl.DataFrame(
        [
            _list_floats(n, "arr_f64"),
            _list_int(n, "arr_i32", pl.Int32),
            _list_strings(n, "arr_str"),
        ]
    )


def _values_arrays_prefixed(n: int) -> pl.DataFrame:
    return pl.DataFrame(
        [
            _list_floats(n, _array_prefixed("arr_f64")),
            _list_int(n, _array_prefixed("arr_i32"), pl.Int32),
            _list_strings(n, _array_prefixed("arr_str")),
        ]
    )


def _values_structs_native(n: int) -> pl.DataFrame:
    return pl.DataFrame([_struct(n, "obj"), _nested_struct(n, "nested")])


def _values_structs_prefixed(n: int) -> pl.DataFrame:
    return pl.DataFrame([_json_struct_strings(n, _struct_prefixed("obj_json"))])


def _values_unsupported(n: int) -> pl.DataFrame:
    return pl.DataFrame([_list_int(n, "arr_i64", pl.Int64), _list_of_struct(n, "arr_struct")])


def _values_with_complex(n: int) -> pl.DataFrame:
    return pl.DataFrame([_floats_nan(n, "value"), _list_floats(n, "arr"), _struct(n, "obj")])


# ---------------------------------------------------------------------------
# Avro stream files (canonical io.nominal.ingest.AvroStream).
#
# Valid + edge-value files are written by the Rust streaming client (nominal_streaming), which is
# how real producers emit avro; corrupt files are hand-built with fastavro / polars / raw bytes to
# exercise the backend's validation and decode paths. Records carry their own absolute-ns
# timestamps, so there is no separate timestamp column (FileSpec.timestamp_type is unused for avro).
# ---------------------------------------------------------------------------

# The exact writer schema nominal_streaming emits; reused verbatim so fastavro-built corrupt files
# parse as AvroStream (and the backend rejects them on content, not on schema).
_AVRO_STREAM_SCHEMA: dict[str, object] = {
    "type": "record",
    "name": "io.nominal.ingest.AvroStream",
    "fields": [
        {"name": "channel", "type": "string"},
        {"name": "timestamps", "type": {"type": "array", "items": "long"}},
        {
            "name": "values",
            "type": {
                "type": "array",
                "items": [
                    "double",
                    "string",
                    "long",
                    {"type": "record", "name": "io.nominal.ingest.DoubleArray",
                     "fields": [{"name": "items", "type": {"type": "array", "items": "double"}}]},
                    {"type": "record", "name": "io.nominal.ingest.StringArray",
                     "fields": [{"name": "items", "type": {"type": "array", "items": "string"}}]},
                    {"type": "record", "name": "io.nominal.ingest.JsonStruct",
                     "fields": [{"name": "json", "type": "string"}]},
                ],
            },
        },
        {"name": "tags", "type": {"type": "map", "values": "string"}, "default": {}},
    ],
}
_JSON_STRUCT = "io.nominal.ingest.JsonStruct"  # fastavro union branch selector for struct values


def _avro_ts(n: int) -> list[int]:
    """Absolute-ns timestamps on the same grid as the csv/parquet files (_BASE + i*_STEP_NS)."""
    base_ns = int(_BASE.timestamp()) * 1_000_000_000
    return [base_ns + i * _STEP_NS for i in range(n)]


@contextlib.contextmanager
def _avro_stream(path: Path) -> Iterator[NominalDatasetStream]:
    """Open a file-backed Nominal stream, yield it, and drain+close on exit."""
    with NominalDatasetStream("", PyNominalStreamOpts()).to_file(path) as stream:
        yield stream


def _write_avro_valid(path: Path, n: int) -> None:
    """All value kinds on the canonical grid: scalar double/long/string, float/string arrays, struct."""
    ts = _avro_ts(n)
    with _avro_stream(path) as s:
        s.enqueue_batch("scalar_double", ts, [float(i) + 0.5 for i in range(n)])
        s.enqueue_batch("scalar_long", ts, list(range(n)))
        s.enqueue_batch("scalar_string", ts, [f"s{i}" for i in range(n)])
        for i in range(n):
            s.enqueue_float_array("float_array", ts[i], [float(i), float(i) + 0.5, float(i) + 1.0])
            s.enqueue_string_array("string_array", ts[i], [f"a{i}", f"b{i}"])
            s.enqueue_struct("struct_ch", ts[i], {"x": i, "label": f"p{i}", "ok": i % 2 == 0})


def _write_avro_nan_inf(path: Path, n: int) -> None:
    """Valid schema, edge float values: NaN / +-Inf / huge magnitudes, as scalars and array elements."""
    ts = _avro_ts(n)
    pool = [0.5, float("nan"), float("inf"), float("-inf"), 1e308, -1e308]
    with _avro_stream(path) as s:
        s.enqueue_batch("nan_inf_double", ts, [pool[i % len(pool)] for i in range(n)])
        for i in range(n):
            s.enqueue_float_array("nan_inf_array", ts[i], [float("nan"), float("inf"), float(i)])


def _write_avro_unsorted_ts(path: Path, n: int) -> None:
    """Valid schema, timestamps emitted in descending order (the backend must order them)."""
    rev = list(reversed(_avro_ts(n)))
    with _avro_stream(path) as s:
        s.enqueue_batch("unsorted_double", rev, [float(i) for i in range(n)])


def _write_avro_records(path: Path, records: list[dict[str, object]]) -> None:
    """Write raw AvroStream records with fastavro (codec=null, so no snappy/cramjam dependency).

    fastavro is an ephemeral `uv run --with fastavro` dependency, so import it lazily here.
    """
    from fastavro import parse_schema, writer  # type: ignore[import-not-found]

    with open(path, "wb") as f:
        writer(f, parse_schema(_AVRO_STREAM_SCHEMA), records, codec="null")


def _write_avro_missing_timestamps(path: Path, n: int) -> None:
    """Corrupt: values present but the timestamps array is empty (no timestamp for any point)."""
    _write_avro_records(path, [
        {"channel": "missing_ts", "timestamps": [], "values": [float(i) + 0.5 for i in range(n)], "tags": {}},
    ])


def _write_avro_len_mismatch(path: Path, n: int) -> None:
    """Corrupt: timestamps and values arrays have different lengths."""
    _write_avro_records(path, [
        {"channel": "len_mismatch", "timestamps": _avro_ts(n), "values": [float(i) for i in range(max(1, n // 2))],
         "tags": {}},
    ])


def _write_avro_bad_json_struct(path: Path, n: int) -> None:
    """Corrupt: JsonStruct values whose `json` field is not parseable JSON."""
    ts = _avro_ts(n)
    bad = ["{not valid json", "}}}", "", "nan", "{'single': 'quotes'}"]
    values = [(_JSON_STRUCT, {"json": bad[i % len(bad)]}) for i in range(n)]
    _write_avro_records(path, [{"channel": "bad_json_struct", "timestamps": ts, "values": values, "tags": {}}])


def _write_avro_wrong_schema(path: Path, n: int) -> None:
    """Corrupt: a valid avro container, but a plain tabular schema instead of AvroStream."""
    pl.DataFrame([_floats_nan(n, "value"), _ints(n, "count"), _strings_null(n, "label")]).write_avro(path)


def _write_avro_garbage(path: Path, n: int) -> None:
    """Corrupt: not an avro container at all (wrong magic), so the decoder must reject it outright."""
    path.write_bytes(b"Obj\x00 this is not a valid avro file\n" * max(1, n))


_TS_TYPES: list[tuple[str, _AnyTimestampType]] = [
    ("iso_8601", "iso_8601"),
    ("epoch_nanoseconds", "epoch_nanoseconds"),
    ("epoch_microseconds", "epoch_microseconds"),
    ("epoch_milliseconds", "epoch_milliseconds"),
    ("epoch_seconds", "epoch_seconds"),
    ("epoch_minutes", "epoch_minutes"),
    ("epoch_hours", "epoch_hours"),
    ("epoch_days", "epoch_days"),
    ("relative_unix_epoch", Relative(unit="seconds", start=_UNIX_EPOCH)),
    ("relative_prev_day", Relative(unit="seconds", start=_BASE - dt.timedelta(days=1))),
    ("relative_file_start", Relative(unit="seconds", start=_BASE)),
    ("custom", Custom(format="yyyy-MM-dd HH:mm:ss.SSSSSS")),
    ("custom_default_year", Custom(format="DDD HH:mm:ss.SSSSSS", default_year=2024)),
]

SPECS: list[FileSpec] = [
    # Timestamp coverage (CSV, 1k rows, one per type).
    *[FileSpec(f"ts_{label}", "csv", 1_000, ts, _basic_values) for label, ts in _TS_TYPES],
    # CSV edge cases.
    FileSpec("csv_messy", "csv", 1_000, "epoch_nanoseconds", _values_messy),
    FileSpec("csv_nan_null", "csv", 1_000, "epoch_nanoseconds", _values_nan_null),
    FileSpec("csv_wide", "csv", 1_000, "epoch_nanoseconds", _values_wide),
    # CSV sizes.
    FileSpec("size_10", "csv", 10, "epoch_nanoseconds", _basic_values),
    FileSpec("size_1k", "csv", 1_000, "epoch_nanoseconds", _basic_values),
    FileSpec("size_100k", "csv", 100_000, "epoch_nanoseconds", _basic_values),
    FileSpec("size_1M", "csv", 1_000_000, "epoch_nanoseconds", _basic_values),
    # Parquet scalar types.
    FileSpec("pq_scalars", "parquet", 1_000, "epoch_nanoseconds", _values_scalars),
    # Parquet array channels (native + prefixed).
    FileSpec("pq_arrays_native", "parquet", 1_000, "epoch_nanoseconds", _values_arrays_native),
    FileSpec("pq_arrays_prefixed", "parquet", 1_000, "epoch_nanoseconds", _values_arrays_prefixed),
    # Parquet struct channels (native + prefixed).
    FileSpec("pq_structs_native", "parquet", 1_000, "epoch_nanoseconds", _values_structs_native),
    FileSpec("pq_structs_prefixed", "parquet", 1_000, "epoch_nanoseconds", _values_structs_prefixed),
    # Parquet deliberate expected-fail edges.
    FileSpec("pq_unsupported", "parquet", 1_000, "epoch_nanoseconds", _values_unsupported),
    # Parquet sizes / timestamp variants.
    FileSpec("pq_size_10", "parquet", 10, "iso_8601", _basic_values),
    FileSpec("pq_size_1M", "parquet", 1_000_000, "epoch_nanoseconds", _basic_values),
    FileSpec("pq_timestamps_iso", "parquet", 1_000, "iso_8601", _values_with_complex),
    # Avro streams: valid + edge values (nominal_streaming), then corrupt files (fastavro/polars/raw).
    FileSpec("avro_valid", "avro", 1_000, avro_writer=_write_avro_valid),
    FileSpec("avro_nan_inf", "avro", 1_000, avro_writer=_write_avro_nan_inf),
    FileSpec("avro_unsorted_ts", "avro", 1_000, avro_writer=_write_avro_unsorted_ts),
    FileSpec("avro_missing_timestamps", "avro", 100, avro_writer=_write_avro_missing_timestamps),
    FileSpec("avro_len_mismatch", "avro", 100, avro_writer=_write_avro_len_mismatch),
    FileSpec("avro_bad_json_struct", "avro", 100, avro_writer=_write_avro_bad_json_struct),
    FileSpec("avro_wrong_schema", "avro", 1_000, avro_writer=_write_avro_wrong_schema),
    FileSpec("avro_garbage", "avro", 10, avro_writer=_write_avro_garbage),
]


def build_job(
    client: NominalClient,
    dataset: Dataset,
    specs: list[FileSpec],
    out_dir: Path,
) -> tuple[IngestionJobBuilder, list[str]]:
    """Generate each spec's file and register it on a builder. Returns (builder, skipped names)."""
    builder = IngestionJobBuilder(client, dataset)
    skipped: list[str] = []
    for spec in specs:
        try:
            path = generate(spec, out_dir)
            if spec.fmt == "avro":
                builder.add_avro_stream(path, tags=spec.tags)
            else:
                builder.add_tabular(
                    path,
                    TIMESTAMP_COLUMN,
                    spec.timestamp_type,
                    tag_columns=spec.tag_columns,
                    tags=spec.tags,
                )
        except Exception as exc:  # one bad generator should not sink the run
            logger.warning("Skipping spec %s: %s", spec.name, exc)
            skipped.append(spec.name)
    return builder, skipped


def _dry_run(specs: list[FileSpec], out_dir: Path) -> int:
    for spec in specs:
        try:
            path = generate(spec, out_dir)
            size_kb = path.stat().st_size / 1024
            logger.info("generated %-26s %-7s rows=%-8d %.1f KiB", spec.name, spec.fmt, spec.n_rows, size_kb)
        except Exception as exc:
            logger.warning("FAILED to generate %s: %s", spec.name, exc)
    return 0


_TERMINAL_JOB_STATUS = {
    IngestionJobStatus.COMPLETED,
    IngestionJobStatus.FAILED,
    IngestionJobStatus.CANCELLED,
    IngestionJobStatus.UNKNOWN,
}


def report(job: IngestionJob, *, fail_on_ingest_error: bool) -> int:
    """Poll the job + its files to terminal (without raising), print a status table, return exit code."""
    # UNKNOWN is treated as terminal so an unmodeled server status can't hang the wait;
    # the per-file poll below still re-checks each file.
    while job.status not in _TERMINAL_JOB_STATUS:
        time.sleep(1)
        job = job.refresh()

    job = job.refresh()

    files = job.dataset_files()
    failed = 0
    logger.info("")
    logger.info("Per-file ingest status (%d files):", len(files))
    for f in files:
        try:
            f.poll_until_ingestion_completed()
        except (NominalIngestError, NominalIngestFailed):
            pass
        f = f.refresh()  # noqa: PLW2901 — re-bind to the refreshed file; the stale handle is not reused
        status = f.ingest_status.name
        ok = status == "SUCCESS"
        if not ok:
            failed += 1
        mark = "OK " if ok else "ERR"
        detail = "" if ok else f"  ({f._ingest_error_message or 'see app'})"
        logger.info("  [%s] %-30s %s%s", mark, f.name, status, detail)

    logger.info("")
    logger.info("Job %s: %d file(s), %d failed. %s", job.status.name, len(files), failed, job.nominal_url)
    return 1 if (failed and fail_on_ingest_error) else 0


def _select_specs(names: list[str] | None) -> list[FileSpec]:
    """Return SPECS filtered to the given names (order preserved); error on any unknown name."""
    if not names:
        return SPECS
    wanted = set(names)
    selected = [s for s in SPECS if s.name in wanted]
    unknown = wanted - {s.name for s in selected}
    if unknown:
        raise SystemExit(f"unknown --files spec(s): {', '.join(sorted(unknown))}")
    return selected


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate and ingest edge-case CSV/Parquet/Avro files.")
    parser.add_argument("--profile", help="Nominal config profile (required unless --dry-run).")
    parser.add_argument("--dataset-name", default=None, help="Target dataset name (default: timestamped).")
    parser.add_argument("--output-dir", default=None, help="Where to write files (default: a temp dir).")
    parser.add_argument("--keep-files", action="store_true", help="Do not delete generated files on exit.")
    parser.add_argument("--fail-on-ingest-error", action="store_true", help="Exit non-zero if any file fails ingest.")
    parser.add_argument("--dry-run", action="store_true", help="Generate + inspect files; no client, no submit.")
    parser.add_argument("--files", nargs="+", default=None, help="Only these spec names (default: all).")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("nominal_streaming").setLevel(logging.WARNING)  # silence per-stream lifecycle chatter

    if not args.dry_run and not args.profile:
        parser.error("--profile is required unless --dry-run is given")

    specs = _select_specs(args.files)

    with contextlib.ExitStack() as stack:
        if args.output_dir:
            out_dir = Path(args.output_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
        else:
            out_dir = Path(stack.enter_context(tempfile.TemporaryDirectory()))
            if args.keep_files:
                logger.warning("--keep-files ignored without --output-dir (temp dir is removed)")

        if args.dry_run:
            return _dry_run(specs, out_dir)

        client = NominalClient.from_profile(args.profile)
        name = args.dataset_name or f"edge-case-ingest-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}"
        dataset = client.create_dataset(name)
        logger.info("Created dataset %s (%s)", name, dataset.rid)

        builder, skipped = build_job(client, dataset, specs, out_dir)
        if skipped:
            logger.warning("Skipped %d spec(s) during generation: %s", len(skipped), ", ".join(skipped))
        job = builder.submit()
        logger.info("Submitted ingest job %s", job.rid)
        return report(job, fail_on_ingest_error=args.fail_on_ingest_error)


if __name__ == "__main__":
    raise SystemExit(main())
