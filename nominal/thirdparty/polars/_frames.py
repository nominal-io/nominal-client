"""Decode export files and assemble caller-visible Polars frames on integer nanosecond clocks."""

import itertools
from collections.abc import Mapping, Sequence
from typing import BinaryIO

import polars as pl
from nominal.core.channel import Channel, ChannelDataType
from nominal.ts import Epoch, Iso8601, Relative, _AnyExportableTimestampType, _SecondsNanos, _to_typed_timestamp_type

POLARS_TYPES = {ChannelDataType.INT: pl.Int64, ChannelDataType.DOUBLE: pl.Float64, ChannelDataType.STRING: pl.String}


def format_time_col(df: pl.DataFrame, time_col: str, timestamp_type: _AnyExportableTimestampType) -> pl.DataFrame:
    timestamp = _to_typed_timestamp_type(timestamp_type)
    nanos = pl.col(time_col)
    if isinstance(timestamp, Iso8601):
        value = nanos.cast(pl.Datetime("ns", "UTC"))
    elif isinstance(timestamp, (Epoch, Relative)):
        if isinstance(timestamp, Relative):
            nanos = nanos - _SecondsNanos.from_flexible(timestamp.start).to_nanoseconds()
        ns_per_unit = {
            "picoseconds": 0.001,
            "nanoseconds": 1,
            "microseconds": 1_000,
            "milliseconds": 1_000_000,
            "seconds": 1e9,
            "minutes": 60e9,
            "hours": 3600e9,
            "days": 86400e9,
        }
        value = nanos.cast(pl.Float64) / ns_per_unit[timestamp.unit]
    else:
        raise ValueError(f"Unsupported export timestamp type: {timestamp_type!r}")
    return df.with_columns(value.alias(time_col))


def _native_clock_frames(points: pl.DataFrame, names: Sequence[str], time_col: str) -> list[pl.DataFrame]:
    """Group raw columns with identical nanosecond timestamps within this job and window."""
    if len(names) == 1:
        return [points.select(pl.col("ts").dt.epoch("ns").alias(time_col), pl.col("value").alias(names[0]))]
    # SQL orders samples by time. Grouping on the entire timestamp list also
    # distinguishes clocks with the same rate and endpoints but different interior samples.
    channels = points.group_by("channel").agg(pl.col("ts").dt.epoch("ns"), "value")
    clocks = channels.group_by("ts").agg("channel", "value")
    frames = []
    for i in range(clocks.height):
        samples = clocks["value"][i]
        values = {name: samples[j].alias(name) for j, name in enumerate(clocks["channel"][i])}
        columns = [values[name] for name in names if name in values]
        frames.append(pl.DataFrame([clocks["ts"][i].alias(time_col), *columns]))
    return frames


def _exact_float(integers: pl.Series, channel: str) -> pl.Series:
    floating = integers.cast(pl.Float64)
    if not floating.cast(pl.Int64, strict=False).equals(integers):
        raise ValueError(f"Channel {channel!r} mixes floats and integers that lose precision in Float64")
    return floating


def _restore_numeric_values(points: pl.DataFrame, channel: str) -> pl.DataFrame:
    """Choose one exact dtype for a channel whose physical type changed between samples."""
    integers = points["integer_value"]
    if points["value"].null_count() == points.height:
        return points.drop("value").rename({"integer_value": "value"})
    if integers.null_count() != points.height:
        points = points.with_columns(pl.coalesce("value", _exact_float(integers, channel)).alias("value"))
    return points.drop("integer_value")


def _wide_frame(df: pl.DataFrame, time_col: str, schema: Mapping[str, type[pl.DataType] | pl.DataType]) -> pl.DataFrame:
    """Retain requested columns without samples, followed by any tag-expanded compute columns."""
    present = set(df.columns)
    missing = [pl.lit(None, dtype=dtype).alias(name) for name, dtype in schema.items() if name not in present]
    extra = [col for col in df.columns if col != time_col and col not in schema]
    return df.with_columns(missing).select(time_col, *schema, *extra).sort(time_col)


def read_raw_frames(
    file: BinaryIO, channels: Sequence[Channel], time_col: str, joined: bool, mixed_numeric: bool
) -> list[pl.DataFrame]:
    names = [ch.name for ch in channels]
    columns = None
    if not joined and len(names) == 1:
        columns = ["ts", "value", "integer_value"] if mixed_numeric else ["ts", "value"]
    points = pl.read_parquet(file, columns=columns)
    if points.is_empty():
        if not joined:
            return []
        dtype = (
            POLARS_TYPES[channels[0].data_type]
            if mixed_numeric and channels[0].data_type is not None
            else points.schema["value"]
        )
        return [pl.DataFrame(schema={time_col: pl.Int64, **dict.fromkeys(names, dtype)})]
    if mixed_numeric:
        points = _restore_numeric_values(points, names[0])
    if not joined:
        return _native_clock_frames(points, names, time_col)
    # Use the caller-safe timestamp name before pivoting a channel named "ts".
    wide = points.with_columns(pl.col("ts").dt.epoch("ns").alias(time_col)).pivot(
        on="channel", index=time_col, values="value"
    )
    return [_wide_frame(wide, time_col, dict.fromkeys(names, points.schema["value"]))]


def read_compute_frames(
    file: BinaryIO, channels: Mapping[str, Channel], time_col: str, joined: bool
) -> list[pl.DataFrame]:
    schema = {ch.name: POLARS_TYPES[ch.data_type] for ch in channels.values() if ch.data_type is not None}
    csv = pl.read_csv(file, infer_schema_length=0, schema_overrides={"timestamp": pl.Int64}, raise_if_empty=False)
    if csv.is_empty():
        return [pl.DataFrame(schema={time_col: pl.Int64, **schema})] if joined else []
    columns = [pl.col("timestamp").alias(time_col)]
    for column in csv.columns:
        if column == "timestamp":
            continue
        # Compute appends tag values to the wire ID, including for channel names that contain underscores.
        wire_name, separator, tags = column.partition("_")
        name = channels[wire_name].name
        output_name = name + separator + tags
        if separator and output_name in schema:
            raise ValueError(f"Tag-expanded column {output_name!r} matches a requested channel name")
        columns.append(pl.col(column).cast(schema[name]).alias(output_name))
    return [_wide_frame(csv.select(columns), time_col, schema)]


def join_window(results: Sequence[tuple[str | None, list[pl.DataFrame]]], time_col: str) -> pl.DataFrame:
    groups = []
    # The planner submits each channel's time slices consecutively. Multi-channel jobs
    # can return both SQL and compute frames, which must be joined on time.
    for name, jobs in itertools.groupby(results, key=lambda item: item[0]):
        frames = [frame for _, batch in jobs for frame in batch]
        if name is None or len(frames) == 1:
            groups.extend(frames)
            continue
        if {f.schema[name] for f in frames} == {pl.Int64, pl.Float64}:
            # An empty or all-null float slice must not force a loss of integer precision.
            has_float = any(f[name].dtype == pl.Float64 and f[name].count() for f in frames)
            dtype = pl.Float64 if has_float else pl.Int64
            for i, frame in enumerate(frames):
                values = frame[name]
                if values.dtype != dtype:
                    values = _exact_float(values, name) if has_float else values.cast(pl.Int64)
                    frames[i] = frame.with_columns(values)
        groups.append(pl.concat(frames, how="diagonal"))
    if len(groups) == 1:
        return groups[0]

    names = [name for group in groups for name in group.columns if name != time_col]
    if len(names) != len(set(names)):
        raise ValueError("Export jobs returned overlapping column names. Use join_batches=False.")
    # Polars uses a temporary right-hand clock even with coalesce=True.
    suffix = "_right"
    while time_col + suffix in names:
        suffix += "_right"
    merged = groups[0].lazy()
    for group in groups[1:]:
        merged = merged.join(group.lazy(), on=time_col, how="full", coalesce=True, suffix=suffix)
    return merged.sort(time_col).collect()
