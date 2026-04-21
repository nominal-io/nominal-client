from __future__ import annotations

import json
import logging
import pathlib
from dataclasses import dataclass
from types import TracebackType
from typing import IO, Literal, NoReturn, Protocol, TypedDict, cast

import fastavro
import polars as pl

logger = logging.getLogger(__name__)


_BYTES_PER_MIB = 1024 * 1024
_BYTES_PER_GIB = 1024 * 1024 * 1024

AvroCodec = Literal["null", "deflate", "snappy", "bzip2", "xz"]
"""Avro compression codec. ``null`` and ``deflate`` are part of the Avro
spec; ``snappy`` goes through ``cramjam`` (bundled); ``bzip2`` and ``xz``
use stdlib modules."""

_DEFAULT_CODEC: AvroCodec = "snappy"
_DEFAULT_CHANNEL_BATCH_SIZE = 50_000
_DEFAULT_MAX_FILE_BYTES = _BYTES_PER_GIB


_AVRO_STREAM_SCHEMA: dict[str, object] = fastavro.parse_schema(
    {
        "type": "record",
        "name": "AvroStream",
        "namespace": "io.nominal.ingest",
        "fields": [
            {"name": "channel", "type": "string"},
            {"name": "timestamps", "type": {"type": "array", "items": "long"}},
            {
                "name": "values",
                "type": {
                    "type": "array",
                    # Union arm order must match the backend's V2 spec exactly:
                    # (double, long, string, DoubleArray, StringArray, JsonStruct).
                    # The backend validates schema canonical form; reordering raises
                    # InvalidAvroStreamSchema at ingest.
                    "items": [
                        "double",
                        "long",
                        "string",
                        {
                            "type": "record",
                            "name": "DoubleArray",
                            "fields": [{"name": "items", "type": {"type": "array", "items": "double"}}],
                        },
                        {
                            "type": "record",
                            "name": "StringArray",
                            "fields": [{"name": "items", "type": {"type": "array", "items": "string"}}],
                        },
                        {
                            "type": "record",
                            "name": "JsonStruct",
                            "fields": [{"name": "json", "type": "string"}],
                        },
                    ],
                },
            },
            {
                "name": "tags",
                "type": {"type": "map", "values": "string"},
                "default": {},
            },
        ],
    }
)  # type: ignore[assignment]  # fastavro.parse_schema returns str|list|dict


_REJECTED_DTYPE_HINTS: dict[type, str] = {
    pl.Boolean: (
        "the schema has no boolean arm -- cast with .cast(pl.Float64) for a metric channel "
        "or .cast(pl.Utf8) for a label"
    ),
    pl.Datetime: "the schema has no datetime arm -- convert via .dt.epoch('ns').cast(pl.Int64)",
    pl.Date: "the schema has no date arm -- convert via .dt.epoch('ns').cast(pl.Int64)",
    pl.Duration: "the schema has no duration arm -- cast to Int64 nanoseconds or another numeric unit",
    pl.Time: "the schema has no time arm -- cast to Int64 ns-since-midnight, or to a Utf8 label",
    pl.Categorical: "the schema has no categorical arm -- cast with .cast(pl.Utf8) or .to_physical()",
    pl.Enum: "the schema has no enum arm -- cast with .cast(pl.Utf8) to emit the label string",
    pl.Decimal: "the schema has no decimal arm -- cast with .cast(pl.Float64), accepting precision loss",
}

_MAX_TIMESTAMP_NS = 2**63 - 1
_RESERVED_TAG_KEYS = frozenset({"nom.ingest_rid", "_nominal_ingest_rid"})


_AvroValue = float | int | str | dict[str, list[float]] | dict[str, list[str]] | dict[str, str]


class _AvroRecord(TypedDict):
    channel: str
    timestamps: list[int]
    values: list[_AvroValue]
    tags: dict[str, str]


class _AvroStreamWriter(Protocol):
    def write(self, record: _AvroRecord) -> None: ...

    def flush(self) -> None: ...


@dataclass
class _OpenFile:
    path: pathlib.Path
    handle: IO[bytes]
    writer: _AvroStreamWriter


def _reject(series: pl.Series, dtype_name: str) -> NoReturn:
    hint = _REJECTED_DTYPE_HINTS.get(
        type(series.dtype),
        "not a representable Avro union arm; cast before calling add()",
    )
    raise TypeError(f"Column {series.name!r} has dtype {dtype_name}: {hint}")


def _validate_struct_field_dtype(dtype: pl.DataType, path: str) -> None:
    if isinstance(dtype, pl.Struct):
        for field in dtype.fields:
            _validate_struct_field_dtype(cast(pl.DataType, field.dtype), f"{path}.{field.name}")
        return
    if isinstance(dtype, (pl.List, pl.Array)):
        _validate_struct_field_dtype(cast(pl.DataType, dtype.inner), f"{path}[]")
        return
    if dtype in (pl.Utf8, pl.Boolean, pl.Null):
        return
    if dtype.is_integer() or dtype.is_float():
        return
    raise TypeError(
        f"Struct field {path!r} has dtype {dtype}, which has no strict JSON mapping. "
        f"Cast the field to Int*/UInt*/Float*/Utf8/Boolean before encoding."
    )


def _encode_struct_series(series: pl.Series) -> list[_AvroValue]:
    _validate_struct_field_dtype(series.dtype, path=str(series.name))
    result: list[_AvroValue] = []
    for idx, row in enumerate(series.to_list()):
        if row is None:
            result.append({"json": "{}"})
            continue
        try:
            serialized = json.dumps(row, allow_nan=False)
        except (ValueError, TypeError) as exc:
            raise TypeError(f"Column {series.name!r} row {idx}: JSON serialization failed ({exc}).") from exc
        result.append({"json": serialized})
    return result


def _encode_object_as_string(series: pl.Series) -> list[_AvroValue]:
    values = series.to_list()
    sample = next((v for v in values if v is not None), None)
    if sample is not None and not isinstance(sample, str):
        raise TypeError(
            f"Column {series.name!r} is a pl.Object with non-string values (first sample: {type(sample).__name__})."
        )
    return ["" if v is None else v for v in values]


def _series_to_avro_values(  # noqa: PLR0911
    series: pl.Series, integers_as_double: bool = False
) -> list[_AvroValue]:
    dtype = series.dtype
    # Cache null_count so we call it once (O(n) polars op) instead of re-checking
    # on each branch, and so we can skip fill_null entirely in the common
    # no-nulls case — fill_null always allocates a new Series even when there's
    # nothing to fill.
    has_nulls = series.null_count() > 0

    if dtype == pl.Utf8:
        return (series.fill_null("") if has_nulls else series).to_list()

    if isinstance(dtype, pl.Struct):
        return _encode_struct_series(series)

    if isinstance(dtype, pl.Object):
        return _encode_object_as_string(series)

    if type(dtype) in _REJECTED_DTYPE_HINTS:
        _reject(series, str(dtype))

    if dtype.is_float():
        return (series.fill_null(float("nan")) if has_nulls else series).to_list()

    if dtype.is_integer():
        if integers_as_double:
            s = series.cast(pl.Float64)
            return (s.fill_null(float("nan")) if has_nulls else s).to_list()
        if has_nulls:
            raise TypeError(
                f"Column {series.name!r} is an integer column with {series.null_count()} null(s); "
                f"the AvroStream 'long' arm has no null representation. Either .fill_null(<sentinel>) "
                f"or construct the writer with integers_as_double=True to upcast to NaN-filled Float64."
            )
        return series.to_list()

    if isinstance(dtype, (pl.List, pl.Array)):
        inner = cast(pl.DataType, dtype.inner)
        if type(inner) in _REJECTED_DTYPE_HINTS:
            _reject(series, f"{dtype} (inner {inner})")
        if inner.is_numeric():
            float_series = series.cast(pl.List(pl.Float64))
            return [{"items": v} if v is not None else {"items": []} for v in float_series.to_list()]
        if inner == pl.Utf8:
            return [{"items": list(v)} if v is not None else {"items": []} for v in series.to_list()]
        _reject(series, f"{dtype} (inner {inner})")

    _reject(series, str(dtype))


class PolarsAvroWriter:
    """Writes polars DataFrames to Nominal ``AvroStream`` files.

    Encodes and writes records inline in ``add()``. Produced files match
    the schema consumed by ``nominal.core.Dataset.add_avro_stream``.

    Usage::

        with PolarsAvroWriter(pathlib.Path("out.avro"), timestamp_column="ts") as w:
            w.add("group_a", df_a)
            w.add("group_b", df_b)
    """

    def __init__(
        self,
        base_path: pathlib.Path,
        timestamp_column: str,
        *,
        channel_batch_size: int = _DEFAULT_CHANNEL_BATCH_SIZE,
        max_file_bytes: int = _DEFAULT_MAX_FILE_BYTES,
        codec: AvroCodec = _DEFAULT_CODEC,
        integers_as_double: bool = False,
    ) -> None:
        """Initialize the writer.

        Args:
            base_path: Destination file path. Output files are numbered
                with zero-padded indices before the extension.
            timestamp_column: Column name containing absolute Unix
                nanosecond timestamps (``pl.Int64``).
            channel_batch_size: Maximum rows per channel slice.
            max_file_bytes: Approximate maximum bytes per output file
                before rolling.
            codec: Avro compression codec. See ``AvroCodec``.
            integers_as_double: Emit integer columns via the ``double``
                arm (Float64 upcast, NaN for nulls) instead of the
                spec-native ``long`` arm.
        """
        self.base_path = base_path
        self._timestamp_column = timestamp_column
        self._channel_batch_size = channel_batch_size
        self._max_file_bytes = max_file_bytes
        self._codec = codec
        self._integers_as_double = integers_as_double

        self._file_index = 0
        self._current: _OpenFile | None = None
        self._files_written: list[pathlib.Path] = []
        self._records_written = 0
        self._points_written = 0
        self._closed = False

    def __enter__(self) -> PolarsAvroWriter:
        """Return self so the writer can be used as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Finalize the current file and close on context exit."""
        self.close()

    @property
    def files_written(self) -> list[pathlib.Path]:
        return list(self._files_written)

    def add(
        self,
        group_name: str,
        df: pl.DataFrame,
        tags: dict[str, str] | None = None,
    ) -> None:
        """Slice a DataFrame into channel batches, encode inline, and append to the current file.

        Rolls to a new file when the current one exceeds ``max_file_bytes``.

        Raises:
            ValueError: If ``df`` is missing the timestamp column, if a
                timestamp is out of ``[0, 2**63)``, or if ``tags`` contains
                a reserved key.
            TypeError: If the timestamp column is not ``Int64``.
            RuntimeError: If the writer has been closed.
        """
        if self._closed:
            raise RuntimeError("Cannot add after close() has been called")

        if self._timestamp_column not in df.columns:
            raise ValueError(
                f"DataFrame for group {group_name!r} is missing timestamp "
                f"column {self._timestamp_column!r}; available: {sorted(df.columns)}"
            )

        ts_series = df[self._timestamp_column]
        if ts_series.dtype != pl.Int64:
            raise TypeError(
                f"Timestamp column {self._timestamp_column!r} in group {group_name!r} has dtype "
                f"{ts_series.dtype}; expected Int64 nanoseconds since Unix epoch."
            )

        if ts_series.len() > 0:
            # ts.min/max skip nulls, so a null-tolerant range check would let
            # null rows through and surface as an opaque fastavro encode error.
            null_count = ts_series.null_count()
            if null_count > 0:
                raise ValueError(
                    f"Timestamp column {self._timestamp_column!r} in group {group_name!r} "
                    f"contains {null_count} null(s); timestamps must not be null."
                )
            ts_min = cast("int | None", ts_series.min())
            ts_max = cast("int | None", ts_series.max())
            if ts_min is not None and ts_min < 0:
                raise ValueError(f"Timestamp column in group {group_name!r} contains a negative value ({ts_min}).")
            if ts_max is not None and ts_max >= _MAX_TIMESTAMP_NS:
                raise ValueError(
                    f"Timestamp column in group {group_name!r} contains a value ({ts_max}) "
                    f"at or above Long.MAX_VALUE ({_MAX_TIMESTAMP_NS})."
                )

        if tags:
            reserved = _RESERVED_TAG_KEYS.intersection(tags)
            if reserved:
                raise ValueError(f"Tag key(s) {sorted(reserved)!r} on group {group_name!r} are reserved.")

        base_tags = {"source_group": group_name}
        if tags:
            base_tags.update(tags)

        data_columns = [c for c in df.columns if c != self._timestamp_column]
        n_rows = df.height
        batch_size = self._channel_batch_size
        integers_as_double = self._integers_as_double

        for start in range(0, n_rows, batch_size):
            length = min(batch_size, n_rows - start)
            batch_timestamps = ts_series.slice(start, length).to_list()

            # base_tags is read-only from here on; share the same dict across
            # all records in this add() call rather than allocating a fresh
            # copy per channel per batch. Saves O(records) dict allocations.
            for col_name in data_columns:
                record = _AvroRecord(
                    channel=col_name,
                    timestamps=batch_timestamps,
                    values=_series_to_avro_values(
                        df[col_name].slice(start, length),
                        integers_as_double=integers_as_double,
                    ),
                    tags=base_tags,
                )
                self._write_record(record)

    def _write_record(self, record: _AvroRecord) -> None:
        if self._current is None:
            self._open_next_file()
        assert self._current is not None
        self._current.writer.write(record)
        self._records_written += 1
        self._points_written += len(record["timestamps"])
        if self._current.handle.tell() >= self._max_file_bytes:
            self._finalize_current_file()

    def _open_next_file(self) -> None:
        stem = self.base_path.stem
        suffix = self.base_path.suffix
        path = self.base_path.with_name(f"{stem}_{self._file_index:03d}{suffix}")
        self._file_index += 1
        handle = open(path, "wb")
        try:
            writer = cast(
                _AvroStreamWriter,
                fastavro.write.Writer(handle, _AVRO_STREAM_SCHEMA, codec=self._codec),
            )
        except BaseException:
            handle.close()
            raise
        self._current = _OpenFile(path=path, handle=handle, writer=writer)

    def _finalize_current_file(self) -> None:
        if self._current is None:
            return
        current = self._current
        current.writer.flush()
        current.handle.close()
        size = current.path.stat().st_size
        logger.info("Finalized file %s (%.2f MiB)", current.path, size / _BYTES_PER_MIB)
        self._files_written.append(current.path)
        self._current = None

    def close(self) -> list[pathlib.Path]:
        if self._closed:
            return list(self._files_written)
        self._closed = True
        try:
            self._finalize_current_file()
        finally:
            # Safety net if finalize threw mid-flush; ensure handle isn't leaked.
            if self._current is not None and not self._current.handle.closed:
                try:
                    self._current.handle.close()
                except Exception:
                    logger.exception("Failed to close %s during shutdown", self._current.path)
                self._current = None
        total_size = sum(p.stat().st_size for p in self._files_written)
        logger.info(
            "Complete: wrote %s points from %d records across %d file(s) [%.2f MiB total]",
            f"{self._points_written:,}",
            self._records_written,
            len(self._files_written),
            total_size / _BYTES_PER_MIB,
        )
        return list(self._files_written)
