#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "nominal==1.77.0",
#   "pandas>=2.3.2",
#   "polars>=1.31.0",
#   "pyarrow>=21.0.0",
#   "rich>=14.1.0",
# ]
# ///

"""Terminal UX: Click + Rich CLI to browse assets → datasets → select channels by
exact-match queries → download to disk for users to open in matlab.
"""

from __future__ import annotations

import atexit
import datetime
import logging
import logging.handlers
import pathlib
import queue
import warnings
from typing import Any, List, Mapping, Optional, Sequence

import click
import pandas as pd
from rich.box import HORIZONTALS
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.prompt import Confirm, IntPrompt, Prompt
from rich.style import Style
from rich.syntax import Syntax
from rich.table import Column, Table

import nominal
from nominal.cli.util.global_decorators import client_options
from nominal.core.event import Event

logger = logging.getLogger(__name__)

###################################
# Inlined future changes to ts.py #
###################################

from typing import Union

from nominal_api import scout_dataexport_api
from typing_extensions import TypeAlias

from nominal.ts import (
    Custom,
    Epoch,
    Iso8601,
    Relative,
    _LiteralAbsolute,
    _SecondsNanos,
    _time_unit_to_conjure,
    _to_typed_timestamp_type,
)

_TypedNativeTimestampType: TypeAlias = Union[Iso8601, Epoch, Relative]
"""Type alias for all of the strongly typed timestamp types that can be converted to a native python datetime"""

TypedTimestampType: TypeAlias = Union[_TypedNativeTimestampType, Custom]
"""Type alias for all of the strongly typed timestamp types."""

_AnyNativeTimestampType: TypeAlias = Union[_TypedNativeTimestampType, _LiteralAbsolute]
"""Type alias for all of the allowable timestamp types that can be converted to a native python datetime"""


def _to_export_timestamp_format(type_: _AnyNativeTimestampType) -> scout_dataexport_api.TimestampFormat:
    typed_timestamp_format = _to_typed_timestamp_type(type_)
    if isinstance(typed_timestamp_format, Iso8601):
        return scout_dataexport_api.TimestampFormat(iso8601=scout_dataexport_api.Iso8601TimestampFormat())
    elif isinstance(typed_timestamp_format, Epoch):
        # Returning epoch based timestamps is the same as returning relative timestamps to unix epoch
        return scout_dataexport_api.TimestampFormat(
            relative=scout_dataexport_api.RelativeTimestampFormat(
                relative_to=_SecondsNanos.from_nanoseconds(0).to_api(),
                time_unit=_time_unit_to_conjure(typed_timestamp_format.unit),
            )
        )
    elif isinstance(typed_timestamp_format, Relative):
        return scout_dataexport_api.TimestampFormat(
            relative=scout_dataexport_api.RelativeTimestampFormat(
                relative_to=_SecondsNanos.from_flexible(typed_timestamp_format.start).to_api(),
                time_unit=_time_unit_to_conjure(typed_timestamp_format.unit),
            )
        )
    else:
        raise TypeError(f"Unsupported timestamp type for data export: {type_}")


#############################################
# Inlined upcoming polars_export_handler.py #
#############################################

import collections
import concurrent.futures
import dataclasses
import datetime
import logging
from typing import Iterator, Mapping, Sequence

import polars as pl
from nominal_api import api, datasource_api, scout_compute_api, scout_dataexport_api
from typing_extensions import Self

from nominal import NominalClient
from nominal._utils import LogTiming
from nominal.core.channel import Channel, ChannelDataType
from nominal.experimental.compute import batch_compute_buckets
from nominal.experimental.compute.dsl import exprs
from nominal.ts import (
    Epoch,
    IntegralNanosecondsDuration,
    IntegralNanosecondsUTC,
    Iso8601,
    Relative,
    _AnyNativeTimestampType,
    _InferrableTimestampType,
    _SecondsNanos,
    _to_export_timestamp_format,
    _to_typed_timestamp_type,
)

# Number of workers to use across thread / processes pools when hitting the api
DEFAULT_NUM_WORKERS = 8

# Number of points to export at once in a single request to the data export service.
# Nominal has a hard limit of 10 million unique timestaps within a single request,
# however, empirical performance is better with a smaller size
DEFAULT_POINTS_PER_REQUEST = 1_000_000

# Number of points to export within each dataframe exported at a time
DEFAULT_POINTS_PER_DATAFRAME = 25_000_000

# Maximum number of channels to get data for within a single request to Nominal
DEFAULT_CHANNELS_PER_REQUEST = 25

DEFAULT_EXPORTED_TIMESTAMP_COL_NAME = "timestamp"
_INTERNAL_TS_COL = "__nmnl_ts__"  # internal join key, chosen to avoid collision with channel names

logger = logging.getLogger(__name__)


def _group_channels_by_datatype(channels: Sequence[Channel]) -> Mapping[ChannelDataType, Sequence[Channel]]:
    """Partition the provided channels by data type.

    Channels with no datatype are grouped into the UNKNOWN partition of channels.

    Args:
        channels: Channels to partition
    Returns:
        Mapping of data type to a list of the corresponding channels
    """
    channel_groups = collections.defaultdict(list)
    for channel in channels:
        channel_groups[channel.data_type or ChannelDataType.UNKNOWN].append(channel)
    return {**channel_groups}


def _has_data_with_tags(
    client: NominalClient, channel: Channel, tags: Mapping[str, str], start_ns: int, end_ns: int
) -> bool:
    resp = client._clients.datasource.get_available_tags_for_channel(
        client._clients.auth_header,
        datasource_api.GetAvailableTagsForChannelRequest(
            datasource_api.ChannelWithTagFilters(
                channel=channel.name,
                data_source_rid=channel.data_source,
                tag_filters={**tags},
            ),
            start_time=_SecondsNanos.from_nanoseconds(start_ns).to_scout_run_api(),
            end_time=_SecondsNanos.from_nanoseconds(end_ns).to_scout_run_api(),
        ),
    )

    # No data matches the given tags
    if not resp.available_tags.available_tags:
        return False

    bad_tag_items = {name: values for name, values in resp.available_tags.available_tags.items() if len(values) > 1}
    if bad_tag_items:
        logger.warning(
            "Channel %s has underconstrained tags-- results may have duplicate rows: %s", channel.name, bad_tag_items
        )

    return True


def _build_point_rate_expressions(
    client: NominalClient,
    channels: Sequence[Channel],
    start_ns: IntegralNanosecondsUTC,
    end_ns: IntegralNanosecondsUTC,
    tags: Mapping[str, str],
) -> Sequence[tuple[Channel, exprs.NumericExpr | None]]:
    expressions: list[tuple[Channel, exprs.NumericExpr | None]] = []
    for channel in channels:
        if channel.data_type is not ChannelDataType.DOUBLE:
            logger.warning(
                "Can only compute points per second on float channels, but %s has type: %s",
                channel.name,
                channel.data_type,
            )
            expressions.append((channel, None))
        elif tags and not _has_data_with_tags(client, channel, tags, start_ns, end_ns):
            logger.warning("No points found in range for channel '%s'", channel.name)
            expressions.append((channel, None))
        else:
            expressions.append((channel, exprs.NumericExpr.datasource_channel(channel.data_source, channel.name, tags)))

    return expressions


def _batch_channel_points_per_second(
    client: NominalClient,
    channels: Sequence[Channel],
    start: _InferrableTimestampType,
    end: _InferrableTimestampType,
    tags: dict[str, str],
    num_buckets: int,
) -> Mapping[str, float | None]:
    """For each provided channel, determine the maximum number of points per second in the given range.

    NOTE: Not intended for direct use-- see `channel_points_per_second`
    NOTE: do not use with more than 300 channels, or 500 concurrently across all requests, or concurrency limits
          will be breached and the request will fail.

    Args:
        client: Nominal request client
        channels: Channels to query data rates for
        start: Start of the time range to query over
        end: End of the time range to query over
        tags: Key-value pairs of tags to filter data with
        num_buckets: Number of buckets to use-- more typically leads to better results.
            NOTE: max number of buckets allowed is 1000

    Returns:
        Mapping of channel name to maximum points/second for the respective channels
    """
    if not channels:
        logger.warning("No channels given!")
        return {}
    elif num_buckets > 1000:
        raise ValueError("num_buckets must be <=1000")

    start_ns = _SecondsNanos.from_flexible(start).to_nanoseconds()
    end_ns = _SecondsNanos.from_flexible(end).to_nanoseconds()

    # For each channel that has data with the given tags within the provided time range, add a
    # compute expression to later retrieve decimated bucket stats
    results: dict[str, float | None] = {}
    expressions = []
    channels_in_expressions = []
    for channel, expression in _build_point_rate_expressions(client, list(channels), start_ns, end_ns, tags):
        if expression is None:
            results[channel.name] = None
        else:
            expressions.append(expression)
            channels_in_expressions.append(channel)

    # For each channel, compute the number of points across the desired number of buckets.
    # Compute the approximate average points/second in each bucket, and use the largest
    # across all buckets as the points per second for that channel.
    try:
        batch_buckets = batch_compute_buckets(client, expressions, start_ns, end_ns, buckets=num_buckets)
    except Exception:
        logger.exception("Failed to compute buckets for channels: %s", [ch.name for ch in channels_in_expressions])
        return {ch.name: None for ch in channels}

    for channel, buckets in zip(channels_in_expressions, batch_buckets):
        if len(buckets) == 0:
            logger.warning("No points found in range for channel '%s'", channel.name)
            results[channel.name] = 0
        elif len(buckets) == 1:
            results[channel.name] = buckets[0].count / ((end_ns - start_ns) / 1e9)
        else:
            max_points_per_second = 0.0
            for idx in range(1, len(buckets)):
                bucket = buckets[idx]
                last_bucket = buckets[idx - 1]
                bucket_duration = (bucket.timestamp - last_bucket.timestamp) / 1e9
                points_per_second = bucket.count / bucket_duration
                max_points_per_second = max(max_points_per_second, points_per_second)
                results[channel.name] = max_points_per_second

    return results


def _channel_points_per_second(
    client: NominalClient,
    channels: Sequence[Channel],
    start: _InferrableTimestampType,
    end: _InferrableTimestampType,
    tags: Mapping[str, str] | None = None,
    num_buckets: int = 100,
    num_workers: int = DEFAULT_NUM_WORKERS,
) -> Mapping[str, float | None]:
    """For each provided channel, determine the maximum number of points per second in the given range.

    This method will take the list of channels provided, and group them into batches (as determined by
    `batch_size`) and perform queries in parallel using the provided `Executor`.

    NOTE: may take a long time for large channel counts. Takes approx. 30s for 1000 channels with good internet,
          but varies based on how many points are within the query bounds.

    Args:
        client: Nominal client to make requests with
        channels: Channels to query data rates for
        start: Start of the time range to query over
        end: End of the time range to query over
        tags: Key-value pairs of tags to filter data with
        num_buckets: Number of buckets to use when computing points per second
        num_workers: Number of parallel requests to make

    Returns:
        Mapping of channel name to maximum points/second for the respective channels
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = {}
        for idx in range(0, len(channels), DEFAULT_CHANNELS_PER_REQUEST):
            channel_batch = channels[idx : idx + DEFAULT_CHANNELS_PER_REQUEST]
            fut = pool.submit(
                _batch_channel_points_per_second,
                client,
                channel_batch,
                start=start,
                end=end,
                tags=dict(tags or {}),
                num_buckets=num_buckets,
            )
            futures[fut] = channel_batch

        results = {}
        num_processed = 0
        for fut in concurrent.futures.as_completed(futures):
            channel_batch = futures[fut]
            num_processed += len(channel_batch)
            logger.debug("Completed querying %d/%d channels for update rate", num_processed, len(channels))

            ex = fut.exception()
            if ex is not None:
                logger.error(
                    "Failed to extract %d channel sample rates: %s",
                    len(channel_batch),
                    [ch.name for ch in channel_batch],
                    exc_info=ex,
                )
                continue

            res = fut.result()
            for channel, rate in res.items():
                results[channel] = rate

        return results


def _build_channel_groups(
    points_per_second: Mapping[str, float],
    channels_by_name: Mapping[str, Channel],
    points_per_request: int,
    channels_per_request: int,
    batch_duration: datetime.timedelta,
) -> tuple[list[list[Channel]], list[Channel]]:
    # Channels that can be read entirely in a single export request for a batch
    channel_groups = []

    # Channels that wouldn't fit in a single export request for a batch
    large_channels = []

    # Compute channel groups for numeric channels
    allowed_rate_per_group = points_per_request / batch_duration.total_seconds()
    curr_group: list[Channel] = []
    curr_rate = 0.0
    for channel_name, channel_rate in sorted(points_per_second.items(), key=lambda tup: tup[1], reverse=True):
        # We build channel groups starting with the highest data rate channels to reduce the number of
        # NaNs that are provided by the backend during data export
        channel = channels_by_name[channel_name]
        if channel_rate > allowed_rate_per_group:
            large_channels.append(channel)
            continue

        # If the current group is too big to be able to add the current channel, add to channel groups
        if curr_rate + channel_rate > allowed_rate_per_group or len(curr_group) >= channels_per_request:
            channel_groups.append(curr_group)
            curr_group = []
            curr_rate = 0.0

        curr_group.append(channel)
        curr_rate += channel_rate

    if curr_group:
        channel_groups.append(curr_group)

    return channel_groups, large_channels


@dataclasses.dataclass(frozen=True, unsafe_hash=True, order=True)
class TimeRange:
    start_time: IntegralNanosecondsUTC
    end_time: IntegralNanosecondsUTC

    @property
    def start_api(self) -> api.Timestamp:
        """Gets the start time of the range in conjure API format."""
        return _SecondsNanos.from_nanoseconds(self.start_time).to_api()

    @property
    def end_api(self) -> api.Timestamp:
        """Gets the end time of the range in conjure API format."""
        return _SecondsNanos.from_nanoseconds(self.end_time).to_api()

    def duration_ns(self) -> IntegralNanosecondsDuration:
        return self.end_time - self.start_time

    def subdivide_ns(self, duration: IntegralNanosecondsDuration) -> Sequence[Self]:
        return [
            self.__class__(curr_ns, min(curr_ns + duration, self.end_time))
            for curr_ns in range(self.start_time, self.end_time, duration)
        ]

    def duration(self) -> datetime.timedelta:
        return datetime.timedelta(seconds=(self.end_time - self.start_time) / 1e9)

    def subdivide(self, duration: datetime.timedelta) -> Sequence[Self]:
        """Subdivides the duration into chunks with at most the given duration."""
        return self.subdivide_ns(int(duration.total_seconds() * 1e9))


@dataclasses.dataclass(frozen=True, unsafe_hash=True)
class ExportJob:
    """Represents an individual export task suitable for giving to subprocesses."""

    dataset_rid: str
    channel_names: list[str]

    # Time bounds to export
    time_slice: TimeRange

    # Key-value pairs to filter channels by
    tags: dict[str, str]

    # Decimation settings
    buckets: int | None = None
    resolution: IntegralNanosecondsDuration | None = None

    # Timestamp formatting
    timestamp_type: _AnyNativeTimestampType = "epoch_seconds"

    def resolution_options(self) -> scout_dataexport_api.ResolutionOption:
        """Construct data export resolution options based on bucketing and resolution parameters."""
        if self.buckets is not None and self.resolution is not None:
            raise ValueError("Only one of buckets or resolution may be provided")
        elif self.buckets is None and self.resolution is None:
            return scout_dataexport_api.ResolutionOption(undecimated=scout_dataexport_api.UndecimatedResolution())
        else:
            return scout_dataexport_api.ResolutionOption(nanoseconds=self.resolution, buckets=self.buckets)

    def export_channels(self, client: NominalClient) -> scout_dataexport_api.ExportChannels:
        """Construct data export channels for the configured channels and export options."""
        channels = client.get_dataset(self.dataset_rid).get_channels(names=self.channel_names)
        return scout_dataexport_api.ExportChannels(
            time_domain=scout_dataexport_api.ExportTimeDomainChannels(
                channels=[channel._to_time_domain_channel(tags=self.tags) for channel in channels],
                merge_timestamp_strategy=scout_dataexport_api.MergeTimestampStrategy(
                    none=scout_dataexport_api.NoneStrategy()
                ),
                output_timestamp_format=_to_export_timestamp_format(self.timestamp_type),
            )
        )

    def export_request(self, client: NominalClient) -> scout_dataexport_api.ExportDataRequest:
        """Construct conjure export request given the provided configuration options."""
        return scout_dataexport_api.ExportDataRequest(
            channels=self.export_channels(client=client),
            context=scout_compute_api.Context(function_variables={}, variables={}),
            end_time=self.time_slice.end_api,
            start_time=self.time_slice.start_api,
            resolution=self.resolution_options(),
            compression=scout_dataexport_api.CompressionFormat.GZIP,
            format=scout_dataexport_api.ExportFormat(
                csv=scout_dataexport_api.Csv(),
            ),
        )


def _format_time_col(df: pl.DataFrame, time_col: str, job: ExportJob) -> pl.DataFrame:
    typed_timestamp_type = _to_typed_timestamp_type(job.timestamp_type)

    if isinstance(typed_timestamp_type, (Relative, Epoch)):
        # Already numeric/relative per export service; no transform.
        return df
    elif isinstance(typed_timestamp_type, Iso8601):
        # Parse ISO8601 into timezone-aware datetime
        return df.with_columns(pl.col(time_col).str.strptime(pl.Datetime, strict=False, exact=False).alias(time_col))
    else:
        raise ValueError("Expected timestamp type to be a typed timestamp type")


def _get_exported_timestamp_channel(channel_names: list[str]) -> str:
    # Handle channel names that will be renamed during export
    renamed_timestamp_col = DEFAULT_EXPORTED_TIMESTAMP_COL_NAME
    if DEFAULT_EXPORTED_TIMESTAMP_COL_NAME in channel_names:
        idx = 1
        while True:
            other_col_name = f"timestamp.{idx}"
            if other_col_name not in channel_names:
                renamed_timestamp_col = other_col_name
                break
            else:
                idx += 1

    return renamed_timestamp_col


def _export_job(job: ExportJob, auth_header: str, base_url: str, workspace_rid: str | None) -> pl.DataFrame:
    client = NominalClient.from_token(token=auth_header, base_url=base_url, workspace_rid=workspace_rid)
    if not job.channel_names:
        raise ValueError("No channels to extract")

    # Warn user about renamed channels, handle data with channel names of "timestamp"
    time_col = _get_exported_timestamp_channel(job.channel_names)

    req = job.export_request(client)
    resp = client._clients.dataexport.export_channel_data(auth_header, req)

    # Read CSV via Polars
    df = pl.read_csv(resp)

    if len(df[time_col].unique()) != len(df[time_col]):
        logger.error("Dataframe has duplicate timestamps! %s", df.head())

    if df.is_empty():
        logger.warning("No data found for export for channels %s", job.channel_names)
        return pl.DataFrame({col: [] for col in [*job.channel_names, time_col]})
    else:
        df = _format_time_col(df, time_col, job)

    # Create internal timestamp column for consistent joins; keep original time_col out of the way
    df = df.rename({time_col: _INTERNAL_TS_COL}).with_columns(pl.col(_INTERNAL_TS_COL).cast(pl.Float64))

    # Place __ts__ first, then the channel columns
    ordered_cols = [_INTERNAL_TS_COL] + [c for c in df.columns if c not in (_INTERNAL_TS_COL, time_col)]

    # Drop the export-visible time col to avoid collisions with channel names in other batches
    df = df.select(ordered_cols).sort(by=pl.col(_INTERNAL_TS_COL))

    return df


def _merge_dfs(dfs: Sequence[pl.DataFrame]) -> pl.DataFrame:
    if not dfs:
        return pl.DataFrame()

    # Vertically concat frames that have the exact same non-ts set of columns
    df_idx_by_channel_set: Mapping[frozenset[str], set[int]] = collections.defaultdict(set)
    for idx, df in enumerate(dfs):
        channel_cols = frozenset([c for c in df.columns if c != _INTERNAL_TS_COL])
        df_idx_by_channel_set[channel_cols].add(idx)

    logger.debug("Concatenating vertical columns")
    full_dfs: list[pl.LazyFrame] = []
    for channels, idxs in df_idx_by_channel_set.items():
        group_dfs = [dfs[i] for i in idxs if not dfs[i].is_empty()]
        if not group_dfs:
            logger.warning("Skipping empty dataframes for columns: %s", channels)
            continue

        sorted_dfs = sorted(group_dfs, key=lambda df: df[_INTERNAL_TS_COL].min() or 0)
        combined = pl.concat([df.lazy() for df in sorted_dfs], how="vertical", rechunk=True).sort(
            by=pl.col(_INTERNAL_TS_COL)
        )
        full_dfs.append(combined)

    if len(full_dfs) == 0:
        return pl.DataFrame()
    elif len(full_dfs) == 1:
        return full_dfs[0].sort(_INTERNAL_TS_COL).collect()

    logger.debug("Merging dataframes")

    # Outer-join all groups on internal ts
    merged = full_dfs[0]
    for next_df in full_dfs[1:]:
        merged = merged.join(next_df, on=_INTERNAL_TS_COL, how="full", coalesce=True)

    return merged.sort(_INTERNAL_TS_COL).collect()


class PolarsExportHandler:
    """Manages streaming data out of Nominal using DataFrames.

    Steps:
    * If no bucket duration is provided, compute a max duration that fits the configured batch size.
    * Compute read schedule, channel groupings, and time slices.
    * For each time slice:
        * in parallel, fetch each channel group
        * stitch vertically within groups and then outer-join across groups on timestamp column
    * Yield merged DataFrame batches
    """

    def __init__(
        self,
        client: NominalClient,
        points_per_request: int = DEFAULT_POINTS_PER_REQUEST,
        points_per_dataframe: int = DEFAULT_POINTS_PER_DATAFRAME,
        channels_per_request: int = DEFAULT_CHANNELS_PER_REQUEST,
        num_workers: int = DEFAULT_NUM_WORKERS,
    ):
        """Initialize export handler"""
        self._client = client
        self._points_per_request = points_per_request
        self._points_per_dataframe = points_per_dataframe
        self._channels_per_request = channels_per_request

        self._num_workers = num_workers

    def _compute_export_jobs(
        self,
        channels: Sequence[Channel],
        time_range: TimeRange,
        timestamp_type: _AnyNativeTimestampType,
        tags: dict[str, str] | None = None,
        buckets: int | None = None,
        resolution: IntegralNanosecondsDuration | None = None,
        batch_duration: datetime.timedelta | None = None,
    ) -> Mapping[TimeRange, Sequence[ExportJob]]:
        """Compute the mapping of export time slices to the sequence of export jobs to produce data for that range."""
        if buckets is not None and resolution is not None:
            raise ValueError("Cannot provide `buckets` and `resolution`")

        partitioned_channels = _group_channels_by_datatype(channels)
        enum_channels = partitioned_channels.get(ChannelDataType.STRING, [])

        numeric_channels = [
            *partitioned_channels.get(ChannelDataType.DOUBLE, []),
            *partitioned_channels.get(ChannelDataType.INT, []),
        ]
        if batch_duration is None and not numeric_channels:
            raise ValueError("If no numeric channels are provided, a `batch_duration` must be provided!")

        unknown_channels = partitioned_channels.get(ChannelDataType.UNKNOWN, [])
        if unknown_channels:
            logger.warning("Could not determine datatypes of %d channels-- ignoring for export", len(unknown_channels))

        channels_by_name = {channel.name: channel for channel in channels}
        all_points_per_second = _channel_points_per_second(
            client=self._client,
            channels=numeric_channels,
            start=time_range.start_time,
            end=time_range.end_time,
            tags=tags,
        )
        points_per_second = {channel: rate for channel, rate in all_points_per_second.items() if rate}

        # If the user has not given us a specific batch duration (expected), compute the duration
        # that would support the provided batch size parameters (i.e. max points per request)
        batch_duration_ns = 0
        if batch_duration is None:
            if enum_channels:
                logger.warning(
                    "No `batch_duration` provided, but exporting %d enum channels. "
                    "These will not be accounted for in the computed `batch_duration`",
                    len(enum_channels),
                )

            # Compute the theoretical max data rate in an second within the export time range
            total_point_rate = sum(points_per_second.values())
            computed_duration = datetime.timedelta(seconds=self._points_per_dataframe / total_point_rate)

            # If the computed max batch duration is greater than the requested export duration, truncate
            batch_duration_ns = min(int(computed_duration.total_seconds() * 1e9), time_range.duration_ns())
        else:
            batch_duration_ns = int(batch_duration.total_seconds() * 1e9)

        # group channels by dataset
        channel_names_by_dataset = collections.defaultdict(set)
        for channel_group in (numeric_channels, enum_channels):
            for channel in channel_group:
                channel_names_by_dataset[channel.data_source].add(channel.name)

        jobs = collections.defaultdict(list)
        time_slices = time_range.subdivide_ns(batch_duration_ns)
        for dataset_rid, channel_names in channel_names_by_dataset.items():
            channel_groups, large_channels = _build_channel_groups(
                {k: v for k, v in points_per_second.items() if k in channel_names},
                {k: v for k, v in channels_by_name.items() if k in channel_names},
                self._points_per_request,
                self._channels_per_request,
                datetime.timedelta(seconds=batch_duration_ns / 1e9),
            )
            channel_groups.extend([[channel] for channel in enum_channels if channel.name in channel_names])

            for slice in time_slices:
                for channel_group in channel_groups:
                    jobs[slice].append(
                        ExportJob(
                            dataset_rid=dataset_rid,
                            channel_names=[ch.name for ch in channel_group],
                            time_slice=slice,
                            tags=tags if tags else {},
                            buckets=buckets,
                            resolution=resolution,
                            timestamp_type=timestamp_type,
                        )
                    )

                # Add subdivided slices for large channels that cannot be read in a single export request
                # For large channels, we need to subdivide the time range based on their data rates
                for channel in large_channels:
                    channel_rate = points_per_second[channel.name]
                    sub_offset = datetime.timedelta(seconds=self._points_per_request / channel_rate)
                    for sub_slice in slice.subdivide(sub_offset):
                        jobs[slice].append(
                            ExportJob(
                                dataset_rid=dataset_rid,
                                channel_names=[channel.name],
                                time_slice=sub_slice,
                                tags=tags if tags else {},
                                buckets=buckets,
                                resolution=resolution,
                                timestamp_type=timestamp_type,
                            )
                        )

        return jobs

    def export(
        self,
        channels: Sequence[Channel],
        start: IntegralNanosecondsUTC,
        end: IntegralNanosecondsUTC,
        tags: Mapping[str, str] | None = None,
        batch_duration: datetime.timedelta | None = None,
        timestamp_type: _AnyNativeTimestampType = "epoch_seconds",
        buckets: int | None = None,
        resolution: IntegralNanosecondsDuration | None = None,
        join_batches: bool = True,
    ) -> Iterator[pl.DataFrame]:
        """Yield DataFrame slices"""
        # Ensure user has selected channels to export
        if not channels:
            logger.warning("No channels requested for export-- returning")
            return

        # Ensure user has not selected incompatible decimation options
        if None not in (buckets, resolution):
            raise ValueError("Cannot export data decimated with both buckets and resolution")

        # Determine download schedule
        export_jobs = self._compute_export_jobs(
            channels, TimeRange(start, end), timestamp_type, dict(tags or {}), buckets, resolution, batch_duration
        )

        export_time_col = _get_exported_timestamp_channel([ch.name for ch in channels])

        # Kick off downloads
        with (
            LogTiming(f"Downloaded {len(export_jobs)} batches"),
            concurrent.futures.ThreadPoolExecutor(max_workers=self._num_workers) as pool,
        ):
            futures: list[concurrent.futures.Future[pl.DataFrame]] = []

            time_slices = sorted(export_jobs.keys())
            for idx, time_slice in enumerate(time_slices):
                logger.info(
                    "Starting to download data for slice %s (%d / %d)",
                    time_slice,
                    idx + 1,
                    len(time_slices),
                )
                with LogTiming(f"Downloaded data for slice {time_slice} ({idx + 1} / {len(time_slices)})"):
                    # Start by downloading if this is the first batch
                    if not futures:
                        futures = [
                            pool.submit(
                                _export_job,
                                task,
                                self._client._clients.auth_header.split()[-1],
                                self._client._clients.dataexport._uri,
                                self._client._clients.workspace_rid,
                            )
                            for task in export_jobs[time_slice]
                        ]

                    results: list[pl.DataFrame] = []
                    for future_idx, future in enumerate(concurrent.futures.as_completed(futures)):
                        ex = future.exception()
                        if ex is not None:
                            logger.error("Failed to extract batch", exc_info=ex)
                            continue

                        res = future.result()
                        logger.info("Finished extracting batch %d/%d", future_idx + 1, len(futures))
                        if join_batches:
                            results.append(res)
                        elif res.is_empty():
                            continue
                        else:
                            yield res.rename({_INTERNAL_TS_COL: export_time_col})

                    # Schedule next batch of downloads before starting merge
                    if idx < len(time_slices) - 1:
                        futures = [
                            pool.submit(
                                _export_job,
                                task,
                                self._client._clients.auth_header.split()[-1],
                                self._client._clients.dataexport._uri,
                                self._client._clients.workspace_rid,
                            )
                            for task in export_jobs[time_slices[idx + 1]]
                        ]

                if join_batches:
                    with LogTiming(f"Merged {len(results)} exports"):
                        logger.info("Merging dataframes")
                        merged_df = _merge_dfs(results)
                        if merged_df.is_empty():
                            logger.warning("Dataframe empty after merging...")
                        else:
                            yield merged_df.rename({_INTERNAL_TS_COL: export_time_col})


# --------------------------------------------------------------------------------------
# Logging routed through Rich (won't break spinners, safe for background threads)
# --------------------------------------------------------------------------------------
class _QueueListener(logging.handlers.QueueListener):
    pass


def configure_logging(console: Console, level: int = logging.INFO) -> _QueueListener:
    """Configure root logging via QueueHandler → RichHandler.

    Returns the QueueListener so we can stop it cleanly on exit.
    """
    log_queue: queue.Queue = queue.Queue(-1)

    # QueueHandler routes all records (including from background threads)
    qh = logging.handlers.QueueHandler(log_queue)
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers[:] = [qh]

    # RichHandler actually renders to the console, but is called by the listener thread
    rich_handler = RichHandler(
        console=console,
        rich_tracebacks=True,
        show_time=True,
        markup=True,
        enable_link_path=True,
    )
    rich_handler.setLevel(level)

    listener = _QueueListener(log_queue, rich_handler, respect_handler_level=True)
    listener.start()

    # Ensure clean shutdown
    atexit.register(listener.stop)
    return listener


# --------------------------------------------------------------------------------------
# UI helpers
# --------------------------------------------------------------------------------------


def _render_properties(props: Optional[Mapping[str, str]]) -> str:
    if not props:
        return "—"
    # show a compact key=value list
    items = [f"'{k}'='{v}'" for k, v in list(props.items())[:6]]
    suffix = " …" if props and len(props) > 6 else ""
    return ", ".join(items) + suffix


def _render_labels(labels: Optional[Sequence[str]]) -> str:
    if not labels:
        return "—"
    # show a compact key=value list
    items = [f"'{label}'" for label in labels[:6]]
    suffix = " …" if labels and len(labels) > 6 else ""
    return ", ".join(items) + suffix


def _render_table(table: Table, console: Console, max_unpaged: int = 20) -> None:
    if table.row_count > max_unpaged:
        with console.pager():
            console.print(table)
    else:
        console.print(table)


def _select_asset(console: Console, client: nominal.NominalClient) -> nominal.Asset | None:
    with console.status("Loading assets…", spinner="dots"):
        assets = client.search_assets()
        if not assets:
            console.print("No assets available!", style=Style(color="red"))
            return None

    # Sort by last updated timestamps
    raw_assets = client._clients.assets.get_assets(client._clients.auth_header, [asset.rid for asset in assets])
    sorted_assets = sorted(assets, key=lambda asset: pd.to_datetime(raw_assets[asset.rid].updated_at), reverse=True)

    table = Table(
        Column("#", style=Style(color="white", bold=True), ratio=1, overflow="fold"),
        Column("Name", style=Style(color="white", bold=True), ratio=2, overflow="fold"),
        Column("Description", style=Style(color="cyan"), ratio=5, overflow="fold"),
        Column("Labels", style=Style(color="green"), ratio=3, overflow="fold"),
        Column("Properties", style=Style(color="magenta"), ratio=4, overflow="fold"),
        title=f"Available Assets ({len(sorted_assets)})",
        expand=True,
    )
    for idx, asset in enumerate(sorted_assets):
        table.add_row(
            str(idx),
            asset.name,
            asset.description or "-",
            _render_labels(asset.labels),
            _render_properties(asset.properties),
        )

    _render_table(table, console)

    while True:
        idx = IntPrompt.ask("Select an asset #")
        if 0 <= idx < len(sorted_assets):
            asset = sorted_assets[idx]
            console.print(f"Selected asset: {asset.name} ({asset.rid})", style=Style(color="magenta"))
            return asset
        else:
            console.print(f"Please enter a number between 0 and {len(sorted_assets) - 1}.", style=Style(color="red"))

        if Confirm.ask("See asset table again?", default=False, show_default=True):
            _render_table(table, console)


def _select_dataset(console: Console, asset: nominal.Asset) -> tuple[str | None, nominal.Dataset | None]:
    with console.status("Loading datasets...", spinner="dots"):
        datasets_by_ref = {refname: dataset for refname, dataset in asset.list_datasets()}
        if not datasets_by_ref:
            console.print("No datasets for this asset.", style=Style(color="red"))
            return None, None

    if len(datasets_by_ref) == 1:
        refname = list(datasets_by_ref.keys())[0]
        dataset = datasets_by_ref[refname]
        console.print(f"[cyan]Defaulting to dataset {dataset.name} ([magenta]{dataset.rid}[cyan])[/cyan]")
        return refname, dataset

    table = Table(
        Column("#", style=Style(color="white", bold=True), ratio=1, overflow="fold"),
        Column("Name", style=Style(color="white", bold=True), ratio=2, overflow="fold"),
        Column("Refname", style=Style(italic=True, dim=True), ratio=2, overflow="fold"),
        Column("Description", style=Style(color="cyan"), ratio=5, overflow="fold"),
        Column("Labels", style=Style(color="green"), ratio=3, overflow="fold"),
        Column("Properties", style=Style(color="magenta"), ratio=4, overflow="fold"),
        title=f"Available Datasets ({len(datasets_by_ref)})",
        expand=True,
    )
    dataset_pairs = []
    for idx, refname in enumerate(datasets_by_ref):
        dataset = datasets_by_ref[refname]
        dataset_pairs.append((refname, dataset))
        table.add_row(
            str(idx),
            dataset.name,
            refname,
            dataset.description or "-",
            _render_labels(dataset.labels),
            _render_properties(dataset.properties),
        )

    _render_table(table, console)

    while True:
        idx = IntPrompt.ask("Select a dataset #")
        if 0 <= idx < len(dataset_pairs):
            refname, dataset = dataset_pairs[idx]
            console.print(f"Selected dataset: '{dataset.name}' ({dataset.rid})", style=Style(color="magenta"))
            return refname, dataset
        else:
            console.print(f"Please enter a number between 0 and {len(dataset_pairs) - 1}.", style=Style(color="red"))

        if Confirm.ask("See dataset table again?", default=False, show_default=True):
            _render_table(table, console)


def _select_channels(console: Console, dataset: nominal.Dataset) -> list[nominal.Channel]:
    with console.status("Fetching channels…", spinner="dots"):
        all_channels = list(dataset.search_channels())
        console.print(
            f"[dim]There are {len(all_channels)} total channels in dataset[/dim] [bold]{dataset.name}[/bold]."
        )
        console.print(
            "See the dataset page to see available channels",
            style=Style(link=dataset.nominal_url, color="blue"),
        )

    while True:
        substrings = Prompt.ask("Enter exact channel substrings, separated by a comma [* for all]").strip()
        subqueries = [subquery.strip() for subquery in substrings.split(",")]
        new_channels = []
        if not subqueries:
            console.print(
                "No patterns provided! Try again...",
                style=Style(bold=True, color="yellow"),
            )
            continue
        elif "*" in subqueries:
            console.print(
                "'*' provided-- selecting all channels!",
                style=Style(color="magenta"),
            )
            new_channels = all_channels
        else:
            with console.status("Searching channels…", spinner="dots"):
                new_channels = [
                    channel for subquery in subqueries for channel in dataset.search_channels(exact_match=[subquery])
                ]

        if not new_channels:
            console.print(
                "No channels found matching query...",
                style=Style(bold=True, color="red"),
            )
            continue

        if Confirm.ask(f"{len(new_channels)} channel(s) selected! View channels?", default=False, show_default=True):
            _display_channels(new_channels, console)

        if Confirm.ask("Would you like to edit the list of channels?", default=False, show_default=True):
            edited_lines = click.edit(text="\n".join(sorted([ch.name for ch in new_channels])))
            if edited_lines is None:
                console.print("No channels selected... restarting", style=Style(color="yellow"))
                continue

            edited_channels = set([line for line in edited_lines.splitlines() if line])
            new_channels = [channel for channel in new_channels if channel.name in edited_channels]

        if len(new_channels) > 500:
            console.print(
                f"{len(new_channels)} channels selected! Too many (>500) channels results in slow exports!",
                style=Style(bold=True, color="red"),
            )

        if Confirm.ask(f"Are you sure you want to proceed with {len(new_channels)} channel(s)?"):
            return new_channels


def _display_channels(channels: List[nominal.Channel], console: Console) -> None:
    if not channels:
        console.print("No channels...", style=Style(color="yellow"))
        return

    table = Table(
        Column("Name", style=Style(color="white", bold=True), ratio=4, overflow="fold"),
        Column("Description", style=Style(color="cyan"), ratio=3, overflow="fold"),
        Column("Data Type", style=Style(color="green"), ratio=2, overflow="fold"),
        Column("Unit", style=Style(color="magenta"), ratio=2, overflow="fold"),
        title=f"Channels ({len(channels)})",
        expand=True,
    )
    for ch in sorted(channels, key=lambda ch: ch.name):
        table.add_row(ch.name, ch.description or "-", ch.data_type.value if ch.data_type else "-", ch.unit or "-")

    _render_table(table, console)


def _select_bounds(console: Console, client: nominal.NominalClient) -> tuple[datetime.datetime, datetime.datetime]:
    time_option = Prompt.ask(
        "Choose an option for providing time bounds for download",
        choices=["event", "run", "custom"],
        show_choices=True,
    ).strip()
    if time_option == "event":
        start, end = _select_bounds_for_event(console, client)
    elif time_option == "run":
        start, end = _select_bounds_for_run(console, client)
    else:
        start, end = _select_custom_bounds(console)

    start, end = _edit_window_loop(console, start, end)
    return start, end


def _select_bounds_for_event(
    console: Console, client: nominal.NominalClient
) -> tuple[datetime.datetime, datetime.datetime]:
    # Request event from user
    event = _ask_event_by_rid(console, client)

    # If event has no duration, ask for a duration
    event_duration = datetime.timedelta(seconds=event.duration / 1e9)
    if event_duration.total_seconds() == 0:
        event_duration = _ask_duration(console, "Selected event has no duration! Enter duration")

    start = datetime.datetime.fromtimestamp(event.start / 1e9, tz=datetime.timezone.utc)
    return start, start + event_duration


def _ask_event_by_rid(console: Console, client: nominal.NominalClient) -> Event:
    while True:
        # Get event rid
        event_rid = Prompt.ask("Enter event rid (copy + paste from nominal)").strip()

        try:
            # Silence warnings about picosecond resolution
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=UserWarning)
                return client.get_event(event_rid)
        except Exception as ex:
            console.print(f"Failed to get event... try again!\n{ex}", style=Style(color="red"))
            continue


def _ask_run_by_rid(console: Console, client: nominal.NominalClient) -> nominal.Run:
    while True:
        # Get run rid
        run_rid = Prompt.ask("Enter run rid (copy + paste from nominal)").strip()

        try:
            return client.get_run(run_rid)
        except Exception as ex:
            console.print(f"Failed to get run... try again!\n{ex}", style=Style(color="red"))
            continue


def _select_bounds_for_run(
    console: Console, client: nominal.NominalClient
) -> tuple[datetime.datetime, datetime.datetime]:
    # Request run from user
    run = _ask_run_by_rid(console, client)

    start_ns = run.start
    start_timestamp = datetime.datetime.fromtimestamp(start_ns / 1e9, tz=datetime.timezone.utc)

    # If run has no end time, ask for a duration
    end_ns = run.end
    end_timestamp = None
    if end_ns is None:
        end_timestamp = _ask_utc_timestamp(
            console, "Run has no end! Provide end timestamp (UTC)", default=datetime.datetime.now().isoformat()
        )
    else:
        end_timestamp = datetime.datetime.fromtimestamp(end_ns / 1e9, tz=datetime.timezone.utc)

    return start_timestamp, end_timestamp


def _select_custom_bounds(console: Console) -> tuple[datetime.datetime, datetime.datetime]:
    start = _ask_utc_timestamp(console, "Enter start timestamp (UTC), e.g. 2025-09-03T10:15:00Z")
    end = _ask_utc_timestamp(console, "Enter end timestamp (UTC)")
    return start, end


def _edit_window_loop(
    console: Console, start: datetime.datetime, end: datetime.datetime
) -> tuple[datetime.datetime, datetime.datetime]:
    """Show current start/end (UTC), ask if user wants to edit.
    If yes: prompt for new start then end; blank keeps current.
    Loop until user is satisfied and end > start.
    """
    while True:
        seconds = (end - start).total_seconds()
        console.print(
            f"Bounds preview ({(seconds)} seconds):\n  Start (UTC): {start.isoformat()}\n  End   (UTC): {end.isoformat()}"
        )

        if start > end:
            console.print("End must be after start. Please adjust!", style=Style(color="red"))
        elif not Confirm.ask("Edit timestamps?", default=False, show_default=True):
            return start, end

        # Start edit (blank keeps)
        start = _ask_utc_timestamp(console, "Enter new START (UTC)", default=start.isoformat())
        end = _ask_utc_timestamp(console, "Enter new END (UTC)", default=end.isoformat())

        if start > end:
            continue
        elif not Confirm.ask(
            f"Parsed bounds as: [{start.isoformat()}, {end.isoformat()}]. Edit bounds?",
            default=False,
            show_default=True,
        ):
            return start, end


def _ask_utc_timestamp(console: Console, prompt: str, default: Any = ...) -> datetime.datetime:
    while True:
        raw_ts = Prompt.ask(prompt, default=default, show_default=True)
        parsed_ts = _parse_utc_ts(raw_ts)
        if parsed_ts:
            return parsed_ts

        console.print(
            "Invalid timestamp. Try values like '2025-09-03T10:15:00Z' or '2025-09-03 10:15:00' (UTC).",
            style=Style(color="red"),
        )


def _parse_utc_ts(ts: str) -> datetime.datetime | None:
    dt = pd.to_datetime(ts, utc=True)
    if pd.isna(dt):
        return None
    return dt.to_pydatetime()


def _normalize_duration_text(s: str) -> str:
    s = s.strip()
    if not s:
        return s
    s = s.replace("and", " ")
    s = s.replace("mins", "minutes").replace("secs", "seconds")
    return " ".join(s.split())


def _ask_duration(console: Console, prompt: str, default: Any = ...) -> datetime.timedelta:
    while True:
        raw_duration = Prompt.ask(prompt, default=default, show_default=True)
        normalized_duration = _normalize_duration_text(raw_duration)
        td = pd.to_timedelta(normalized_duration, errors="coerce")
        if not pd.isna(td):
            return datetime.timedelta(seconds=float(td.total_seconds()))

        console.print(
            "Invalid duration. Try '5m', '2 hours 30 minutes', '90s', or '00:05:00'.",
            style=Style(color="red"),
        )


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------
@click.group()
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False),
    default="INFO",
    show_default=True,
    help="Logging verbosity.",
)
@click.pass_context
def cli(ctx: click.Context, log_level: str) -> None:
    """Browse assets, pick a dataset, filter channels by exact name, and download."""
    console = Console()
    listener = configure_logging(console, getattr(logging, log_level.upper()))
    ctx.obj = {"listener": listener, "console": console}


@cli.command("browse")
@client_options
@click.pass_context
def browse(ctx: click.Context, client: nominal.NominalClient) -> None:
    """Interactive flow: pick asset → dataset → exact-match channels → output dir → download."""
    _listener: _QueueListener = ctx.obj.get("listener")  # kept alive for duration
    console: Console = ctx.obj.get("console")

    # 1) Assets
    asset = _select_asset(console, client)
    if not asset:
        logger.error("No asset selected! Exiting...")
        return

    # 2) Datasets (mapping refname -> Dataset)
    refname, dataset = _select_dataset(console, asset)
    if dataset is None or refname is None:
        logger.error("No dataset selected! Exiting...")
        return

    # get tags from dataset & asset combo
    scope_tags = None
    raw_asset = client._clients.assets.get_assets(client._clients.auth_header, [asset.rid])[asset.rid]
    for raw_datascope in raw_asset.data_scopes:
        if raw_datascope.data_scope_name == refname:
            scope_tags = raw_datascope.series_tags
            break
    if scope_tags is None:
        logger.error("Failed to retrieve datascope details for refname %s", refname)
        return

    # 3) Channels (exact-name matching with iterative queries)
    channels = _select_channels(console, dataset)
    if not channels:
        logger.error("No channels selected! Exiting...")
        return

    # 4) Output directory
    out_dir: pathlib.Path = click.prompt(
        "Enter download directory:",
        type=click.Path(file_okay=False, dir_okay=True, resolve_path=True, path_type=pathlib.Path),
        default="./out",
        show_default=True,
    )
    if not out_dir.exists():
        console.print(f"Creating output directory '{out_dir}'")
        out_dir.mkdir(parents=True, exist_ok=True)

    # 5) Get time bounds
    start, end = _select_bounds(console, client)

    # 6) Make sure user is fully aware of what they are about to download
    seconds_to_dl = (end - start).total_seconds()
    if not Confirm.ask(
        f"About to download {seconds_to_dl} seconds of data from {len(channels)} channels. Are you sure?"
    ):
        console.print("OK! Exiting...")
        return

    # 7) Download
    dataset_prefix = dataset.rid.split(".")[-1]
    exporter = PolarsExportHandler(client, points_per_dataframe=25_000_000, channels_per_request=10)
    with console.status("Downloading...", spinner="bouncingBar"):
        for idx, df in enumerate(
            exporter.export(
                channels, int(start.timestamp() * 1e9), int(end.timestamp() * 1e9), scope_tags, join_batches=True
            )
        ):
            out_path = out_dir / f"{dataset_prefix}-part_{idx}.parquet"
            df.write_parquet(out_path, compression="snappy")

    # 8) Tell users how to use!
    new_code = f'''
data_dir = fullfile( ...
    "{str(out_dir)}", ...
    "{dataset_prefix}-part_*.parquet" ...
);
nominal_data = sortrows(readall(parquetDatastore(data_dir)), "timestamp");'''.strip()
    new_instructions = Panel(
        Syntax(
            new_code,
            "matlab",
            theme="monokai",
            padding=1,
        ),
        title="Load downloaded data into matlab 2023b+",
        padding=1,
        box=HORIZONTALS,
    )
    console.print(new_instructions)

    old_code = f'''
files=dir(fullfile(
    "{str(out_dir)}", ...
    "{dataset_prefix}-part_*.parquet" ...
));
nominal_data = sortrows( ...
    feval( ...
        @(c) vertcat (c{{:}}), ...
        cellfun( ...
            @parquetread, ...
            fullfile({{files.folder}}, {{files.name}}), ...
            "uni", ...
            0 ...
        ) ...
    ), ...
    "timestamp" ...
);'''.strip()
    old_instructions = Panel(
        Syntax(old_code, "matlab", theme="monokai", padding=1, word_wrap=True),
        title="Load downloaded data into matlab 2019b+",
        padding=1,
        box=HORIZONTALS,
    )
    console.print(old_instructions)


if __name__ == "__main__":
    cli()
