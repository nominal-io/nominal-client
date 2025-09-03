from __future__ import annotations

import collections
import concurrent.futures
import dataclasses
import datetime
import logging
from typing import Any, Iterator, Mapping, Sequence

import pandas as pd
from nominal_api import api, datasource_api, scout_compute_api, scout_dataexport_api, scout_run_api
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
DEFAULT_POINTS_PER_DATAFRAME = 100_000_000

# Maximum number of channels to get data for within a single request to Nominal
DEFAULT_CHANNELS_PER_REQUEST = 25

DEFAULT_EXPORTED_TIMESTAMP_COL_NAME = "timestamp"

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
    client: NominalClient, channel: Channel, tags: dict[str, str], start_ns: int, end_ns: int
) -> bool:
    resp = client._clients.datasource.get_available_tags_for_channel(
        client._clients.auth_header,
        datasource_api.GetAvailableTagsForChannelRequest(
            datasource_api.ChannelWithTagFilters(
                channel=channel.name,
                data_source_rid=channel.data_source,
                tag_filters=tags,
            ),
            start_time=scout_run_api.UtcTimestamp(int(start_ns / 1e9), int(start_ns % 1e9)),
            end_time=scout_run_api.UtcTimestamp(int(end_ns / 1e9), int(end_ns % 1e9)),
        ),
    )
    return len(resp.available_tags.available_tags) > 0


def _batch_channel_points_per_second(
    client: NominalClient,
    channels: Sequence[Channel],
    start: str | datetime.datetime | IntegralNanosecondsUTC,
    end: str | datetime.datetime | IntegralNanosecondsUTC,
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
        return {}
    elif num_buckets > 1000:
        raise ValueError("num_buckets must be <=1000")

    start_ns = _SecondsNanos.from_flexible(start).to_nanoseconds()
    end_ns = _SecondsNanos.from_flexible(end).to_nanoseconds()

    # For each channel that has data with the given tags within the provided time range, add a
    # compute expression to later retrieve decimated bucket stats
    expressions = []
    results: dict[str, float | None] = {}
    channels_in_expressions = []
    for channel in channels:
        if channel.data_type is not ChannelDataType.DOUBLE:
            logger.warning(
                "Can only compute points per second on float channels, but %s has type: %s",
                channel.name,
                channel.data_type,
            )
            results[channel.name] = None
        elif not _has_data_with_tags(client, channel, tags, start_ns, end_ns):
            logger.warning("No points found in range for channel '%s'", channel.name)
            results[channel.name] = None
        else:
            expressions.append(exprs.NumericExpr.datasource_channel(channel.data_source, channel.name, tags))
            channels_in_expressions.append(channel)

    # For each channel, compute the number of points across the desired number of buckets.
    # Compute the approximate average points/second in each bucket, and use the largest
    # across all buckets as the points per second for that channel.
    for channel, buckets in zip(
        channels_in_expressions,
        batch_compute_buckets(
            client,
            expressions,
            start_ns,
            end_ns,
            buckets=num_buckets,
        ),
    ):
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


def channel_points_per_second(
    client: NominalClient,
    channels: Sequence[Channel],
    start: str | datetime.datetime | IntegralNanosecondsUTC,
    end: str | datetime.datetime | IntegralNanosecondsUTC,
    tags: Mapping[str, str] | None = None,
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

    Returns:
        Mapping of channel name to maximum points/second for the respective channels
    """
    NUM_WORKERS = 8
    BATCH_SIZE = 25

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as pool:
        futures = {}
        for idx in range(0, len(channels), BATCH_SIZE):
            channel_batch = channels[idx : idx + BATCH_SIZE]
            fut = pool.submit(
                _batch_channel_points_per_second,
                client,
                channel_batch,
                start=start,
                end=end,
                tags=dict(tags or {}),
                num_buckets=100,
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


def _export_job(job: ExportJob, auth_header: str, base_url: str, workspace_rid: str | None) -> pd.DataFrame:
    client = NominalClient.from_token(token=auth_header, base_url=base_url, workspace_rid=workspace_rid)
    if not job.channel_names:
        raise ValueError("No channels to extract")

    # Warn user about renamed channels, handle data with channel names of "timestamp"
    # channel_names = [ch.name for ch in job.channels]
    time_col = _get_exported_timestamp_channel(job.channel_names)

    req = job.export_request(client)
    resp = client._clients.dataexport.export_channel_data(auth_header, req)
    batch_df = pd.read_csv(resp, compression="gzip")
    if batch_df.empty:
        logger.warning("No data found for export for channels %s", job.channel_names)
        batch_df = pd.DataFrame({col: [] for col in [*job.channel_names, time_col]})
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
    for idxs in df_idx_by_channel_set.values():
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
        all_points_per_second = channel_points_per_second(
            client=self._client,
            channels=numeric_channels,
            start=time_range.start_time,
            end=time_range.end_time,
            tags=tags,
        )
        points_per_second = {channel: rate for channel, rate in all_points_per_second.items() if rate}

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
            computed_duration = datetime.timedelta(seconds=self._points_per_dataframe / total_point_rate)

            # If the computed max batch duration is greater than the requested export duration, truncate
            batch_duration = min(computed_duration, time_range.duration())

        # group channels by dataset
        channel_names_by_dataset = collections.defaultdict(set)
        for channel_group in (numeric_channels, enum_channels):
            for channel in channel_group:
                channel_names_by_dataset[channel.data_source].add(channel.name)

        jobs = collections.defaultdict(list)
        time_slices = time_range.subdivide(batch_duration)
        for dataset_rid, channel_names in channel_names_by_dataset.items():
            channel_groups, large_channels = _build_channel_groups(
                {k: v for k, v in points_per_second.items() if k in channel_names},
                {k: v for k, v in channels_by_name.items() if k in channel_names},
                self._points_per_request,
                self._channels_per_request,
                batch_duration,
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
            channels, TimeRange(start, end), timestamp_type, dict(tags or {}), buckets, resolution, batch_duration
        )

        # Kick off downloads
        with (
            LogTiming(f"Downloaded {len(export_jobs)} batches"),
            concurrent.futures.ThreadPoolExecutor(max_workers=self._num_workers) as pool,
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
                            pool.submit(
                                _export_job,
                                task,
                                self._client._clients.auth_header.split()[-1],
                                self._client._clients.dataexport._uri,
                                self._client._clients.workspace_rid,
                            )
                            for task in export_jobs[time_slice]
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
                            pool.submit(
                                _export_job,
                                task,
                                self._client._clients.auth_header.split()[-1],
                                self._client._clients.dataexport._uri,
                                self._client._clients.workspace_rid,
                            )
                            for task in export_jobs[time_slices[idx + 1]]
                        ]

                with LogTiming(f"Merged {len(results)} exports"):
                    yield _merge_dfs(results)
