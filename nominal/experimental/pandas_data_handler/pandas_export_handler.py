from __future__ import annotations

import collections
import concurrent.futures
import dataclasses
import datetime
import logging
import multiprocessing
from multiprocessing.managers import ValueProxy
from typing import Any, Iterator, Mapping, Sequence

import pandas as pd
from nominal_api import api, scout_compute_api, scout_dataexport_api
from typing_extensions import Self

from nominal._utils import LogTiming
from nominal.core.channel import Channel, ChannelDataType
from nominal.experimental.compute._buckets import _create_compute_request_buckets
from nominal.experimental.compute.dsl import exprs
from nominal.experimental.pandas_data_handler._utils import group_channels_by_datatype, to_pandas_unit
from nominal.ts import (
    Epoch,
    IntegralNanosecondsDuration,
    IntegralNanosecondsUTC,
    Iso8601,
    Relative,
    _AnyNativeTimestampType,
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
DEFAULT_POINTS_PER_DATAFRAME = 10_000_000

# Maximum number of channels to get data for within a single request to Nominal
DEFAULT_CHANNELS_PER_REQUEST = 25

DEFAULT_EXPORTED_TIMESTAMP_COL_NAME = "timestamp"

logger = logging.getLogger(__name__)


def _batch_channel_points_per_second(
    channels: Sequence[Channel],
    start: str | datetime.datetime | IntegralNanosecondsUTC,
    end: str | datetime.datetime | IntegralNanosecondsUTC,
    tags: Mapping[str, str] | None = None,
    window: datetime.timedelta = datetime.timedelta(seconds=1),
) -> Mapping[str, float]:
    """For each provided channel, determine the maximum number of points per second in the given range.

    Args:
        channels: Channels to query data rates for
        start: Start of the time range to query over
        end: End of the time range to query over
        tags: Key-value pairs of tags to filter data with
        window: Duration of window to use when computing rolling counts of points
            NOTE: the closer this gets to a second, the more accurate, but at the cost of taking longer.

    Returns:
        Mapping of channel name to maximum points/second for the respective channels
    """
    if not channels:
        return {}
    elif len(channels) > 300:
        raise ValueError(f"Can only compute points per second on batches up to 300, provided: {len(channels)}")

    requests = []
    for channel in channels:
        if channel.data_type is not ChannelDataType.DOUBLE:
            raise ValueError(
                f"Can only compute points per second on float channels, "
                f"but {channel.name} has type: {channel.data_type}"
            )

        raw_channel = exprs.NumericExpr.channel(channel, tags=tags)
        computed_channel = raw_channel.rolling(window=int(window.total_seconds() * 1e9), operator="count")
        request = _create_compute_request_buckets(
            computed_channel._to_conjure(),
            context={},
            start=_SecondsNanos.from_flexible(start).to_api(),
            end=_SecondsNanos.from_flexible(end).to_api(),
            buckets=1,
        )
        requests.append(request)

    batch_resp = channels[0]._clients.compute.batch_compute_with_units(
        auth_header=channels[0]._clients.auth_header,
        request=scout_compute_api.BatchComputeWithUnitsRequest(requests=requests),
    )

    results = {}
    for channel, resp in zip(channels, batch_resp.results):
        channel_resp = resp.compute_result
        if channel_resp.success:
            if channel_resp.success.bucketed_numeric:
                print(channel.name, channel_resp.success.bucketed_numeric.buckets)
                max_points = max([bucket.max for bucket in channel_resp.success.bucketed_numeric.buckets])
                # print(channel, max_points)
                results[channel.name] = max_points / window.total_seconds()
            else:
                logger.warning("No points found in range for channel '%s'", channel.name)
                results[channel.name] = 0
        elif channel_resp.error:
            logger.error(
                "Failed to compute rate for '%s'-- excluding from results: %s", channel.name, channel_resp.error
            )
        else:
            logger.error("Unexpected channel response: %s", channel_resp)

    return results


def channel_points_per_second(
    channels: Sequence[Channel],
    start: str | datetime.datetime | IntegralNanosecondsUTC,
    end: str | datetime.datetime | IntegralNanosecondsUTC,
    tags: Mapping[str, str] | None = None,
    window: datetime.timedelta = datetime.timedelta(seconds=1),
    batch_size: int = 25,
    num_workers: int = DEFAULT_NUM_WORKERS,
) -> Mapping[str, float]:
    """For each provided channel, determine the maximum number of points per second in the given range.

    This method will take the list of channels provided, and group them into batches (as determined by
    `batch_size`) and perform queries in parallel using the provided `Executor`.

    Args:
        channels: Channels to query data rates for
        start: Start of the time range to query over
        end: End of the time range to query over
        tags: Key-value pairs of tags to filter data with
        window: Duration of window to use when computing rolling counts of points
            NOTE: the closer this gets to a second, the more accurate, but at the cost of taking longer.
        batch_size: Channels to request metadata for in a single batch
            NOTE: setting this too high may result in 429 errors for too many concurrent requests
        num_workers: Number of background threads to use when making API requests

    Returns:
        Mapping of channel name to maximum points/second for the respective channels
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = {}
        for idx in range(0, len(channels), batch_size):
            channel_batch = channels[idx : idx + batch_size]
            fut = pool.submit(
                _batch_channel_points_per_second,
                channel_batch,
                start=start,
                end=end,
                tags=tags,
                window=window,
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
                logger.error("Failed to extract %d channel sample rates", len(channel_batch), exc_info=ex)
                continue

            res = fut.result()
            for channel, rate in res.items():
                results[channel] = rate

        return results


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

    def duration(self) -> datetime.timedelta:
        return datetime.timedelta(seconds=(self.end_time - self.start_time) / 1e9)

    def subdivide(self, duration: datetime.timedelta) -> Sequence[Self]:
        """Subdivides the duration into chunks with at most the given duration."""
        duration_ns = int(duration.total_seconds() * 1e9)
        return [
            self.__class__(curr_ns, min(curr_ns + duration_ns, self.end_time))
            for curr_ns in range(self.start_time, self.end_time, duration_ns)
        ]


@dataclasses.dataclass(frozen=True, unsafe_hash=True)
class ExportJob:
    """Represents an individual export task suitable for giving to subprocesses."""

    channels: Sequence[Channel]

    # Time bounds to export
    time_slice: TimeRange

    # Key-value pairs to filter channels by
    tags: Mapping[str, str] = dataclasses.field(default_factory=dict)

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

    def export_channels(self) -> scout_dataexport_api.ExportChannels:
        """Construct data export channels for the configured channels and export options."""
        return scout_dataexport_api.ExportChannels(
            time_domain=scout_dataexport_api.ExportTimeDomainChannels(
                channels=[channel._to_time_domain_channel(tags=self.tags) for channel in self.channels],
                merge_timestamp_strategy=scout_dataexport_api.MergeTimestampStrategy(
                    none=scout_dataexport_api.NoneStrategy()
                ),
                output_timestamp_format=_to_export_timestamp_format(self.timestamp_type),
            )
        )

    def export_request(self) -> scout_dataexport_api.ExportDataRequest:
        """Construct conjure export request given the provided configuration options."""
        return scout_dataexport_api.ExportDataRequest(
            channels=self.export_channels(),
            context=scout_compute_api.Context(function_variables={}, variables={}),
            end_time=self.time_slice.end_api,
            start_time=self.time_slice.start_api,
            resolution=self.resolution_options(),
            compression=scout_dataexport_api.CompressionFormat.GZIP,
            format=scout_dataexport_api.ExportFormat(
                csv=scout_dataexport_api.Csv(),
            ),
        )


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


def _format_time_col(time_column: pd.Series[Any], job: ExportJob) -> pd.Series[pd.Timestamp] | pd.Series[float]:
    typed_timestamp_type = _to_typed_timestamp_type(job.timestamp_type)

    if isinstance(typed_timestamp_type, (Relative, Epoch)):
        # If the timestamp type is relative, formatting is already handled by the export service
        return time_column
    elif isinstance(typed_timestamp_type, Iso8601):
        # Do nothing-- already in iso format
        return pd.to_datetime(time_column, format="ISO8601", utc=True)
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


def _export_job(job_proxy: ValueProxy[ExportJob]) -> pd.DataFrame:
    job = job_proxy.value
    if not job.channels:
        raise ValueError("No channels to extract")

    dataexport = job.channels[0]._clients.dataexport
    auth_header = job.channels[0]._clients.auth_header

    # Warn user about renamed channels, handle data with channel names of "timestamp"
    channel_names = [ch.name for ch in job.channels]
    time_col = _get_exported_timestamp_channel(channel_names)

    resp = dataexport.export_channel_data(auth_header, job.export_request())
    batch_df = pd.read_csv(resp, compression="gzip")
    if batch_df.empty:
        logger.warning("No data found for export for channels %s", channel_names)
        batch_df = pd.DataFrame({col: [] for col in [*channel_names, time_col]})
    else:
        batch_df[time_col] = _format_time_col(batch_df[time_col], job)

    return batch_df.set_index(time_col)


def _merge_dfs(dfs: Sequence[pd.DataFrame]) -> pd.DataFrame:
    if not dfs:
        return pd.DataFrame()

    # First, vertically concatenate exports that have the same set of columns
    df_idx_by_channel_set: Mapping[frozenset[str], set[int]] = collections.defaultdict(set)
    for idx, df in enumerate(dfs):
        df_idx_by_channel_set[frozenset(df.columns)].add(idx)

    # List of dataframes containing a full copy of each of their columns (no shared columns)
    full_dfs = []
    for columns, idxs in df_idx_by_channel_set.items():
        if len(idxs) == 1:
            # If the set of channels has only been shown once, then it must contain
            # full channels
            full_dfs.append(dfs[list(idxs)[0]])
        else:
            # Vertically combine all dataframes with matching columns
            full_dfs.append(pd.concat([dfs[idx] for idx in idxs], sort=True))

    if len(full_dfs) == 1:
        return full_dfs[0]

    # Finally, horizontally concatenate dataframes and join using the index (which should be timestamps)
    return full_dfs[0].join(list(full_dfs[1:]), how="outer", sort=True)


class PandasExportHandler:
    """Manages streaming data out of Nominal using pandas dataframes.

    Happens in a few steps:
    * If the user has not provided us with a bucket duration, compute the max allowable duration for
      any channel given other parameters and use that
    * Compute read schedule-- which channels will be read in which groups and for which durations
    * For each time slice:
        * in parallel, fetch each channel group
        * join channel groups back together with a combination of horizontal and vertical stitching
    * Yield joined dataframe batches
    """

    def __init__(
        self,
        points_per_request: int = DEFAULT_POINTS_PER_REQUEST,
        points_per_dataframe: int = DEFAULT_POINTS_PER_DATAFRAME,
        channels_per_request: int = DEFAULT_CHANNELS_PER_REQUEST,
        num_workers: int = DEFAULT_NUM_WORKERS,
    ):
        """Initialize export handler"""
        self._points_per_request = points_per_request
        self._points_per_dataframe = points_per_dataframe
        self._channels_per_request = channels_per_request

        self._num_workers = num_workers

    def _compute_export_jobs(
        self,
        channels: Sequence[Channel],
        time_range: TimeRange,
        timestamp_type: _AnyNativeTimestampType,
        tags: Mapping[str, str] | None = None,
        buckets: int | None = None,
        resolution: IntegralNanosecondsDuration | None = None,
        batch_duration: datetime.timedelta | None = None,
    ) -> Mapping[TimeRange, Sequence[ExportJob]]:
        """Compute the mapping of export time slices to the sequence of export jobs to produce data for that range."""
        if buckets is not None and resolution is not None:
            raise ValueError("Cannot provide `buckets` and `resolution`")

        partitioned_channels = group_channels_by_datatype(channels)
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
        points_per_second = channel_points_per_second(
            channels=numeric_channels,
            start=time_range.start_time,
            end=time_range.end_time,
            tags=tags,
            num_workers=self._num_workers,
            window=datetime.timedelta(seconds=1),
        )

        # If the user has not given us a specific batch duration (expected), compute the duration
        # that would support the provided batch size parameters (i.e. max points per request)
        if batch_duration is None:
            if enum_channels:
                logger.warning(
                    "No `batch_duration` provided, but exporting %d enum channels. "
                    "These will not be accounted for in the computed `batch_duration`",
                    len(enum_channels),
                )

            # Compute the theoretical max data rate in an second within the export time range
            total_point_rate = sum(points_per_second.values())
            print("total point rate", total_point_rate, "points per dataframe", self._points_per_dataframe)
            computed_duration = datetime.timedelta(seconds=self._points_per_dataframe / total_point_rate)

            # If the computed max batch duration is greater than the requested export duration, truncate
            batch_duration = min(computed_duration, time_range.duration())
            print(
                "Computed batch duration:",
                batch_duration.total_seconds(),
                "from computed:",
                computed_duration.total_seconds(),
                "and given:",
                time_range.duration().total_seconds(),
            )

        channel_groups, large_channels = _build_channel_groups(
            points_per_second,
            channels_by_name,
            self._points_per_request,
            self._channels_per_request,
            batch_duration,
        )

        # Enum channels cannot have their data rate estimated, so we assume the worst and have them
        # each in their own channel groups
        for channel in enum_channels:
            channel_groups.append([channel])

        jobs = {}
        for slice in time_range.subdivide(batch_duration):
            # Add basic groups of channels that can be read in a single export request
            jobs[slice] = [
                ExportJob(
                    channels=channel_group,
                    time_slice=slice,
                    tags=tags if tags else {},
                    buckets=buckets,
                    resolution=resolution,
                    timestamp_type=timestamp_type,
                )
                for channel_group in channel_groups
            ]

            # Add subdivided slices for large channels that cannot be read in a single export request
            # For large channels, we need to subdivide the time range based on their data rates
            for channel in large_channels:
                channel_rate = points_per_second[channel.name]
                sub_offset = datetime.timedelta(seconds=self._points_per_request / channel_rate)
                for sub_slice in slice.subdivide(sub_offset):
                    jobs[slice].append(
                        ExportJob(
                            channels=[channel],
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
    ) -> Iterator[pd.DataFrame]:
        """Yield dataframe slices"""
        # Ensure user has selected channels to export
        if not channels:
            logger.warning("No channels requested for export-- returning")
            return

        # Ensure user has not selected incompatible decimation options
        if None not in (buckets, resolution):
            raise ValueError("Cannot export data decimated with both buckets and resolution")

        # Determine download schedule
        export_jobs = self._compute_export_jobs(
            channels, TimeRange(start, end), timestamp_type, tags, buckets, resolution, batch_duration
        )

        # Kick off downloads
        with (
            LogTiming(f"Downloaded {len(export_jobs)} batches"),
            concurrent.futures.ThreadPoolExecutor(max_workers=self._num_workers) as pool,
            multiprocessing.Manager() as manager,
        ):
            futures: list[concurrent.futures.Future[pd.DataFrame]] = []

            # For each dataframe batch, concurrently fetch all constituent dataframes
            time_slices = sorted(export_jobs.keys())
            for idx, time_slice in enumerate(time_slices):
                logger.info(
                    "Starting to download data for slice %s (%d / %d)",
                    time_slice,
                    idx + 1,
                    len(time_slices),
                )
                with LogTiming(f"Downloaded data for slice {slice} ({idx + 1} / {len(time_slices)})"):
                    if not futures:
                        futures = [
                            pool.submit(_export_job, manager.Value("o", task)) for task in export_jobs[time_slice]
                        ]

                    results = []
                    for future_idx, future in enumerate(concurrent.futures.as_completed(futures)):
                        ex = future.exception()
                        if ex is not None:
                            logger.error("Failed to extract batch", exc_info=ex)
                            continue

                        res = future.result()
                        logger.info("Finished extracting batch %d/%d", future_idx + 1, len(futures))
                        results.append(res)

                    # Schedule next batch of downloads before starting merge
                    if idx < len(time_slices) - 1:
                        futures = [
                            pool.submit(_export_job, manager.Value("o", task))
                            for task in export_jobs[time_slices[idx + 1]]
                        ]

                with LogTiming(f"Merged {len(results)} exports"):
                    yield _merge_dfs(results)
