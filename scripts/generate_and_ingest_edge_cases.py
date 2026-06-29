"""Generate a wide variety of CSV/Parquet edge-case files and ingest them as one job.

Manual exercise tool (no tests). Run with --dry-run to generate + inspect files without a
backend, or with --profile <name> to create a dataset and kick off a single ingest job.
"""

from __future__ import annotations

import datetime as dt

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
