from __future__ import annotations

import datetime
import os
import pathlib
from types import TracebackType
from typing import Any, Mapping, Sequence

import polars as pl
from nominal_streaming._nominal_streaming import PyNominalDatasetStream, PyNominalStreamOpts
from nominal_streaming.nominal_dataset_stream import _parse_timestamp

TimestampLike = str | int | datetime.datetime
ScalarValue = int | float | str


class RollingAvroWriter:
    """Rolling-file Avro writer backed by the ``nominal-streaming`` Rust client.

    Each finalized file follows the schema written by ``NominalDatasetStream.to_file()``:
    snappy-compressed Avro with the Nominal ``AvroStream`` record layout. Files
    are numbered with a zero-padded index inserted before the extension, e.g.
    ``out.avro`` → ``out_000.avro``, ``out_001.avro``.

    Every ``NominalDatasetStream.enqueue_*`` method is exposed here under an
    ``add_*`` name. :meth:`add_dataframe` is a convenience that dispatches
    polars columns to the matching ``add_*`` based on dtype.

    Rolling:
        A "point" is one ``(channel, timestamp, value)`` triple.
        :meth:`add_batch` counts as ``len(timestamps)`` points and splits its
        input internally so no single underlying call crosses a file
        boundary. :meth:`add_from_dict` counts as ``len(channel_values)``
        points. The remaining ``add_*`` methods are 1 point each. All
        methods other than :meth:`add_batch` are atomic with respect to
        rolling: a call that would cross the threshold is written in full
        to the current file, and the next call rolls. Because the Rust
        backend flushes asynchronously, on-disk bytes lag the enqueued
        count; use :attr:`files_written` after :meth:`close` for the final
        list of paths.

    Durability:
        When ``fsync`` is true (default), each finalized file is fsync'd
        before being appended to :attr:`files_written`. The Rust close path
        flushes the Avro writer's in-memory buffer to the OS but does not
        call ``sync_all``; without fsync, a crash between close and OS
        pageout can truncate the tail.

    Thread safety:
        Not thread-safe. Serialize calls externally if multiple producers
        need to share one writer.

    Example:
        >>> with RustAvroWriter(pathlib.Path("out.avro")) as w:
        ...     w.add("speed", 1_000_000_000, 42.5)
        ...     w.add_batch("altitude", [1_000_000_000, 2_000_000_000], [100.0, 101.0])
        ...     w.add_float_array("vec", 3_000_000_000, [1.0, 2.0, 3.0])
        ...     w.add_dataframe(df, timestamp_column="ts")
        >>> paths = w.files_written
    """

    def __init__(
        self,
        base_path: pathlib.Path,
        *,
        max_points_per_file: int = 100_000_000,
        opts: PyNominalStreamOpts | None = None,
        fsync: bool = True,
    ) -> None:
        """Construct a writer that emits rolling files at ``base_path``.

        Args:
            base_path: Template path for output files. The file index is
                inserted before the extension, e.g. ``out.avro`` →
                ``out_000.avro``.
            max_points_per_file: Target maximum number of points per output
                file. :meth:`add_batch` splits at this boundary exactly;
                other ``add_*`` methods may overshoot by up to one call's
                worth of points. See the class docstring for rolling
                semantics.
            opts: Options forwarded to the underlying ``PyNominalDatasetStream``.
                Defaults to ``PyNominalStreamOpts()``. For file-only output
                (no upload to Nominal core), the worker pool serializes on
                a single Avro-writer mutex, so setting
                ``num_upload_workers=num_runtime_workers=1`` is typically
                sufficient.
            fsync: Call ``os.fsync`` on each finalized file before reporting
                it in :attr:`files_written`.
        """
        self._base_path = base_path
        self._max_points_per_file = max_points_per_file
        self._opts = opts or PyNominalStreamOpts()
        self._fsync = fsync
        self._file_index = 0
        self._current: PyNominalDatasetStream | None = None
        self._current_path: pathlib.Path | None = None
        self._points_in_current = 0
        self.files_written: list[pathlib.Path] = []

    def __enter__(self) -> RustAvroWriter:
        """Return ``self`` so the writer can be used as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Finalize the current file on context exit."""
        del exc_type, exc_value, traceback
        self.close()

    def close(self) -> list[pathlib.Path]:
        """Flush and finalize the in-progress file, if any.

        Idempotent: subsequent calls with no pending data are no-ops. The writer
        itself remains usable for further ``add_*`` calls, which will open a
        fresh file. Internally, :meth:`close` is what :meth:`_stream` invokes
        on a roll, so the two share the file-finalization path.

        Returns:
            The cumulative list of finalized file paths in the order they
            rolled.
        """
        if self._current is not None:
            # Move state off `self` before running tear-down so a failure in
            # Rust close or fsync leaves the writer in a clean "no current
            # file" state rather than a half-torn-down one.
            current, path = self._current, self._current_path
            self._current = None
            self._current_path = None
            current.close()
            assert path is not None
            if self._fsync:
                fd = os.open(path, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            self.files_written.append(path)
        return self.files_written

    def add(
        self,
        channel_name: str,
        timestamp: TimestampLike,
        value: ScalarValue,
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Write a single value. Counts as 1 point against the file limit.

        Args:
            channel_name: Channel to write to.
            timestamp: Absolute UTC timestamp. Strings and ``datetime`` are
                normalized to nanoseconds since the Unix epoch.
            value: Scalar value (``int``, ``float``, or ``str``).
            tags: Optional key-value tags attached to the record.
        """
        self._stream().enqueue(channel_name, _parse_timestamp(timestamp), value, {**(tags or {})})
        self._points_in_current += 1

    def add_batch(
        self,
        channel_name: str,
        timestamps: Sequence[TimestampLike],
        values: Sequence[ScalarValue],
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Write a homogeneously-typed batch for one channel.

        Counts as ``len(timestamps)`` points against the file limit. Splits
        internally at file boundaries so no underlying call exceeds the
        configured ``max_points_per_file``.

        Args:
            channel_name: Channel to write to.
            timestamps: Absolute UTC timestamps aligned with ``values``.
            values: Values with uniform type (``int``, ``float``, or ``str``).
            tags: Optional key-value tags attached to every emitted record.
        """
        # Materialize timestamps and values so we can slice windows off them
        # without caring whether the caller passed a generator, iterator, or
        # list. Python-side cost, but this method is typically called with
        # large inputs where the copy is dwarfed by the downstream work.
        parsed_ts = [_parse_timestamp(ts) for ts in timestamps]
        values = list(values)
        i, n = 0, len(parsed_ts)
        while i < n:
            stream = self._stream()
            # `_stream()` has already rolled if we crossed the limit, so
            # `_points_in_current` is now strictly less than the ceiling.
            # Use whatever capacity remains in this file; the next iteration
            # will roll when `_stream()` is called again.
            chunk = min(n - i, self._max_points_per_file - self._points_in_current)
            stream.enqueue_batch(
                channel_name,
                parsed_ts[i : i + chunk],
                values[i : i + chunk],
                {**(tags or {})},
            )
            self._points_in_current += chunk
            i += chunk

    def add_float_array(
        self,
        channel_name: str,
        timestamp: TimestampLike,
        value: Sequence[float],
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Write a single array-of-doubles record. Counts as 1 point.

        Args:
            channel_name: Channel to write to.
            timestamp: Absolute UTC timestamp.
            value: Sequence of floats. Integer elements are coerced to float
                by the Rust layer.
            tags: Optional key-value tags attached to the record.
        """
        self._stream().enqueue_float_array(channel_name, _parse_timestamp(timestamp), list(value), {**(tags or {})})
        self._points_in_current += 1

    def add_string_array(
        self,
        channel_name: str,
        timestamp: TimestampLike,
        value: Sequence[str],
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Write a single array-of-strings record. Counts as 1 point.

        Args:
            channel_name: Channel to write to.
            timestamp: Absolute UTC timestamp.
            value: Sequence of strings.
            tags: Optional key-value tags attached to the record.
        """
        self._stream().enqueue_string_array(channel_name, _parse_timestamp(timestamp), list(value), {**(tags or {})})
        self._points_in_current += 1

    def add_struct(
        self,
        channel_name: str,
        timestamp: TimestampLike,
        value: Mapping[str, Any],
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Write a single struct record (JSON-encoded by the Rust layer). Counts as 1 point.

        Args:
            channel_name: Channel to write to.
            timestamp: Absolute UTC timestamp.
            value: Mapping of struct fields. Nested values must be
                JSON-native (``int``, ``float``, ``str``, ``bool``,
                ``None``, ``list``, ``dict``); non-native types raise
                ``TypeError`` from the Rust layer.
            tags: Optional key-value tags attached to the record.
        """
        self._stream().enqueue_struct(channel_name, _parse_timestamp(timestamp), dict(value), {**(tags or {})})
        self._points_in_current += 1

    def add_from_dict(
        self,
        timestamp: TimestampLike,
        channel_values: Mapping[str, ScalarValue],
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Write multiple channel values sharing one timestamp.

        Counts as ``len(channel_values)`` points against the file limit — one
        per emitted ``(channel, value)`` pair.

        Args:
            timestamp: Absolute UTC timestamp shared by every value.
            channel_values: Mapping of channel name to scalar value.
            tags: Optional key-value tags attached to every emitted record.
        """
        self._stream().enqueue_from_dict(_parse_timestamp(timestamp), dict(channel_values), {**(tags or {})})
        self._points_in_current += len(channel_values)

    def add_dataframe(
        self,
        df: pl.DataFrame,
        timestamp_column: str,
        tags: Mapping[str, str] | None = None,
    ) -> None:
        """Emit each data column of ``df`` as a separate channel.

        Dispatches per dtype:
            * Scalar (``int`` / ``float`` / ``str``) columns → :meth:`add_batch`.
            * ``pl.List`` / ``pl.Array`` columns → :meth:`add_string_array`
              (when the inner dtype is ``pl.Utf8``) or :meth:`add_float_array`
              otherwise, one call per row.
            * ``pl.Struct`` columns → :meth:`add_struct`, one call per row.

        Value-level validation is delegated to the Rust layer; dtype
        mismatches surface as exceptions from the underlying ``add_*`` call.

        For DataFrames larger than ``max_points_per_file``, call this method
        once per chunk (e.g. via ``df.iter_slices(n_rows=...)``); a single
        ``add_dataframe`` call is one rolling unit only through the splitting
        inside :meth:`add_batch`, and per-row ``add_struct`` /
        ``add_*_array`` calls can overshoot the target by at most one row.

        Args:
            df: Source DataFrame. Must contain ``timestamp_column``.
            timestamp_column: Name of the column holding ``pl.Int64`` absolute
                UTC timestamps in nanoseconds.
            tags: Optional key-value tags attached to every emitted record.
        """
        ts_list = df[timestamp_column].to_list()
        for col in (c for c in df.columns if c != timestamp_column):
            series = df[col]
            dtype = series.dtype
            if isinstance(dtype, pl.Struct):
                # `row or {}` guards against null rows so we hand the Rust
                # layer a valid JSON object rather than `None`.
                for ts, row in zip(ts_list, series.to_list()):
                    self.add_struct(col, ts, row or {}, tags)
            elif isinstance(dtype, (pl.List, pl.Array)):
                # Inner dtype selects which array arm in the Avro union to
                # target. Non-Utf8 inners flow to the float-array arm; the
                # Rust layer raises on inner types it can't coerce.
                add = self.add_string_array if dtype.inner == pl.Utf8 else self.add_float_array
                for ts, row in zip(ts_list, series.to_list()):
                    add(col, ts, row or [], tags)
            else:
                self.add_batch(col, ts_list, series.to_list(), tags)

    def _stream(self) -> PyNominalDatasetStream:
        """Return the underlying stream, rolling or opening a file as needed.

        Callers are expected to increment :attr:`_points_in_current` themselves
        after forwarding to the returned stream, so that the rolling check on
        the next call reflects the work just done.
        """
        if self._current is not None and self._points_in_current >= self._max_points_per_file:
            # Hit the point ceiling for this file; closing here reuses the
            # same finalization path that the public `close()` runs, so
            # rolled and final files are fsync'd identically.
            self.close()
        if self._current is None:
            stem, suffix = self._base_path.stem, self._base_path.suffix
            self._current_path = self._base_path.with_name(f"{stem}_{self._file_index:03d}{suffix}")
            self._file_index += 1
            self._current = PyNominalDatasetStream(self._opts).to_file(self._current_path)
            self._current.open()
            self._points_in_current = 0
        return self._current
