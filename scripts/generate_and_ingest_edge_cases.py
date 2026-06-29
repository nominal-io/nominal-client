"""Generate a wide variety of CSV/Parquet edge-case files and ingest them as one job.

Manual exercise tool (no tests). Run with --dry-run to generate + inspect files without a
backend, or with --profile <name> to create a dataset and kick off a single ingest job.
"""

from __future__ import annotations

import datetime as dt
import uuid

import numpy as np
import polars as pl

from nominal.ts import Custom, Relative, _AnyTimestampType

TIMESTAMP_COLUMN = "timestamp"
_BASE = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
_STEP = dt.timedelta(seconds=1)
_UNIX_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)

# Java DateTimeFormatter pattern (Custom.format) -> Python strftime, for the custom files.
_CUSTOM_STRFTIME = {
    "yyyy-MM-dd HH:mm:ss": "%Y-%m-%d %H:%M:%S",
    "DDD HH:mm:ss": "%j %H:%M:%S",
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
    return _BASE.timestamp() + np.arange(n, dtype=np.float64)


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
        vals = [t.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" for t in _instants(n)]
        return pl.Series(TIMESTAMP_COLUMN, vals, dtype=pl.Utf8)
    if ts_type == "epoch_nanoseconds":
        base_ns = int(_BASE.timestamp() * 1e9)
        ns = base_ns + np.arange(n, dtype=np.int64) * 1_000_000_000
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


def _ints(n: int, name: str, dtype: type[pl.DataType] | pl.DataType = pl.Int64) -> pl.Series:
    return pl.Series(name, np.arange(n, dtype=np.int64), dtype=dtype)


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
    import json

    rows = [json.dumps({"x": i, "label": f"p{i}"}) for i in range(n)]
    return pl.Series(name, rows, dtype=pl.Utf8)


def _array_prefixed(name: str) -> str:
    return f"_nominal_array_{uuid.uuid4().hex}.{name}"


def _struct_prefixed(name: str) -> str:
    return f"_nominal_struct_{name}"
