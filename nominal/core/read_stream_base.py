from __future__ import annotations

import collections
import concurrent.futures
import dataclasses
import datetime
import logging
from typing import Mapping, Sequence

from nominal_api import api, scout_compute_api, scout_dataexport_api
from typing_extensions import Self

from nominal.core.channel import Channel, ChannelDataType
from nominal.ts import (
    IntegralNanosecondsUTC,
    _AnyTimestampType,
    _SecondsNanos,
    _to_api_duration,
    _to_export_timestamp_format,
)

logger = logging.getLogger(__name__)

# Number of workers to use in thread / process pools to query the API with
DEFAULT_NUM_WORKERS = 8

# Number of channels to get data for (at most) per request to Nominal
DEFAULT_CHANNELS_PER_REQUEST = 25

# Number of points to export at once in a single request from Nominal
# Nominal has a hard limit of 10 million unique timestamps at once for a request,
# but performance is empirically better with a smaller request size
DEFAULT_POINTS_PER_REQUEST = 1_000_000

# Number of points to export at once in a single dataframe from Nominal
DEFAULT_POINTS_PER_SLICE = 10_000_000


def _batch_channel_points_per_second(
    channels: Sequence[Channel],
    start: str | datetime.datetime | IntegralNanosecondsUTC,
    end: str | datetime.datetime | IntegralNanosecondsUTC,
    tags: Mapping[str, str] | None = None,
    window: datetime.timedelta = datetime.timedelta(seconds=0.1),
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

    for channel in channels:
        if channel.data_type is not ChannelDataType.DOUBLE:
            raise ValueError(
                f"Can only compute points per second on float channels, "
                f"but {channel.name} has type: {channel.data_type}"
            )

    series_tags = {k: scout_compute_api.StringConstant(v) for k, v in tags.items()} if tags else {}
    channel_sources = [
        scout_compute_api.DataSourceChannel(
            channel=scout_compute_api.StringConstant(literal=channel.name),
            data_source_rid=scout_compute_api.StringConstant(literal=channel.data_source),
            tags=series_tags,
            tags_to_group_by=[],
        )
        for channel in channels
    ]
    batch_resp = channels[0]._clients.compute.batch_compute_with_units(
        auth_header=channels[0]._clients.auth_header,
        request=scout_compute_api.BatchComputeWithUnitsRequest(
            requests=[
                scout_compute_api.ComputeNodeRequest(
                    context=scout_compute_api.Context(
                        function_variables={},
                        variables={},
                    ),
                    start=_SecondsNanos.from_flexible(start).to_api(),
                    end=_SecondsNanos.from_flexible(end).to_api(),
                    node=scout_compute_api.ComputableNode(
                        series=scout_compute_api.SummarizeSeries(
                            input=scout_compute_api.Series(
                                numeric=scout_compute_api.NumericSeries(
                                    rolling_operation=scout_compute_api.RollingOperationSeries(
                                        input=scout_compute_api.NumericSeries(
                                            channel=scout_compute_api.ChannelSeries(data_source=channel_source),
                                        ),
                                        operator=scout_compute_api.RollingOperator(count=scout_compute_api.Count()),
                                        window=scout_compute_api.Window(
                                            duration=scout_compute_api.DurationConstant(
                                                literal=_to_api_duration(window),
                                            )
                                        ),
                                    )
                                )
                            ),
                            buckets=1,
                        )
                    ),
                )
                for channel_source in channel_sources
            ]
        ),
    )

    results = {}
    for channel, resp in zip(channels, batch_resp.results):
        channel_resp = resp.compute_result
        if channel_resp.success:
            if channel_resp.success.bucketed_numeric:
                results[channel.name] = channel_resp.success.bucketed_numeric.buckets[0].max / window.total_seconds()
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


def _channel_points_per_second(
    executor: concurrent.futures.Executor,
    channels: Sequence[Channel],
    start: str | datetime.datetime | IntegralNanosecondsUTC,
    end: str | datetime.datetime | IntegralNanosecondsUTC,
    tags: Mapping[str, str] | None = None,
    window: datetime.timedelta = datetime.timedelta(seconds=0.1),
    batch_size: int = 25,
) -> Mapping[str, float]:
    """For each provided channel, determine the maximum number of points per second in the given range.

    This method will take the list of channels provided, and group them into batches (as determined by
    `batch_size`) and perform queries in parallel using the provided `Executor`.

    Args:
        executor: Concurrent executor to use for processing requests.
        channels: Channels to query data rates for
        start: Start of the time range to query over
        end: End of the time range to query over
        tags: Key-value pairs of tags to filter data with
        window: Duration of window to use when computing rolling counts of points
            NOTE: the closer this gets to a second, the more accurate, but at the cost of taking longer.
        batch_size: Channels to request metadata for in a single batch
            NOTE: setting this too high may result in 429 errors for too many concurrent requests

    Returns:
        Mapping of channel name to maximum points/second for the respective channels
    """
    futures = {}
    for idx in range(0, len(channels), batch_size):
        channel_batch = channels[idx : idx + batch_size]
        fut = executor.submit(
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
        if channel.data_type:
            channel_groups[channel.data_type].append(channel)
        else:
            channel_groups[ChannelDataType.UNKNOWN].append(channel)
    return {**channel_groups}


@dataclasses.dataclass(frozen=True, unsafe_hash=True, order=True)
class TimeRange:
    start_time: int
    end_time: int

    @property
    def start_api(self) -> api.Timestamp:
        """Gets the start time of the range in conjure API format."""
        return _SecondsNanos.from_nanoseconds(self.start_time).to_api()

    @property
    def end_api(self) -> api.Timestamp:
        """Gets the end time of the range in conjure API format."""
        return _SecondsNanos.from_nanoseconds(self.end_time).to_api()

    def subdivide(self, duration_ns: int) -> Sequence[Self]:
        """Subdivides the duration into chunks with at most the given duration."""
        return [
            self.__class__(curr_ns, min(curr_ns + duration_ns, self.end_time))
            for curr_ns in range(self.start_time, self.end_time, duration_ns)
        ]


@dataclasses.dataclass(frozen=True, unsafe_hash=True)
class ExportJob:
    """Represents an individual export task suitable for giving to subprocesses."""

    # Mapping of channel names to their respective datasource rids
    # channel_sources: Mapping[str, str]
    channels: Sequence[Channel]

    # Time bounds to export
    time_slice: TimeRange

    # Key-value pairs to filter channels by
    tags: Mapping[str, str] = dataclasses.field(default_factory=dict)

    # Decimation settings
    buckets: int | None = None
    resolution_ns: int | None = None

    # Timestamp formatting
    timestamp_type: _AnyTimestampType = "epoch_seconds"

    def resolution_options(self) -> scout_dataexport_api.ResolutionOption:
        """Construct data export resolution options based on bucketing and resolution parameters."""
        if self.buckets is not None and self.resolution_ns is not None:
            raise ValueError("Only one of buckets or resolution_ns may be provided")
        elif self.buckets is None and self.resolution_ns is None:
            return scout_dataexport_api.ResolutionOption(undecimated=scout_dataexport_api.UndecimatedResolution())
        else:
            return scout_dataexport_api.ResolutionOption(nanoseconds=self.resolution_ns, buckets=self.buckets)

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


class ReadStreamBase:
    """Base class for exporting streams of data from Nominal.

    Intended to be subclassed by children that either export chunks of data in-memory or to disk.
    """

    def __init__(
        self,
        num_workers: int = DEFAULT_NUM_WORKERS,
        points_per_request: int = DEFAULT_POINTS_PER_REQUEST,
        points_per_slice: int = DEFAULT_POINTS_PER_SLICE,
        max_channels_per_request: int = DEFAULT_CHANNELS_PER_REQUEST,
    ):
        """Initialize ReadStreamBase.

        Args:
            num_workers: Number of parallel workers to use within internal process pools
            points_per_request: Maximum number of points to request from nominal within a single request
            points_per_slice: Maximum number of points (excluding interpolated NaNs) to retrieve in a single export
            max_channels_per_request: Maximum number of channels to include in a single request to Nominal

        """
        self._num_workers = num_workers
        self._points_per_request = points_per_request
        self._points_per_slice = points_per_slice
        self._max_channels_per_request = max_channels_per_request

    def _build_download_queue(
        self,
        channels: Sequence[Channel],
        time_range: TimeRange,
        timestamp_type: _AnyTimestampType,
        tags: Mapping[str, str] | None = None,
        batch_duration: datetime.timedelta | None = None,
        buckets: int | None = None,
        resolution_ns: int | None = None,
    ) -> Mapping[TimeRange, Sequence[ExportJob]]:
        """Given a list of channels, a time range, and other assorted configuration details, build export jobs.

        Args:
            channels: Channels to export data for
            time_range: Range of time to export data for
            timestamp_type: Timestamp format to export data with
            tags: Key-value pairs to filter data being exported with
            batch_duration: Time duration of each batch of data to return in-memory
                NOTE: if not provided, this is computed based on sampled data rates for each
                      channel and the configured request / batch point maximums.
            buckets: Number of buckets to decimate data into within each exported batch of data
                NOTE: may not be used alongside `resolution_ns`
            resolution_ns: Resolution, in nanoseconds, between decimated points.
                NOTE: may not be used alongside `buckets`

        Returns:
            Mapping of time range to a collection of export jobs that extract the data for that time slice.
        """
        # Split channels into float vs string channels
        partitioned_channels = _group_channels_by_datatype(channels)
        float_channels = partitioned_channels.get(ChannelDataType.DOUBLE, [])
        enum_channels = partitioned_channels.get(ChannelDataType.STRING, [])
        unknown_channels = partitioned_channels.get(ChannelDataType.UNKNOWN, [])
        if unknown_channels:
            logger.warning("Could not determine datatypes of %d channels-- ignoring for export", len(unknown_channels))

        channels_by_name = {channel.name: channel for channel in channels}

        # Validate preconditions
        if batch_duration is None:
            if not float_channels:
                raise ValueError("Must provide either float channels or slice duration")

        # Compute max data rates per float channels
        with concurrent.futures.ThreadPoolExecutor(max_workers=self._num_workers) as pool:
            points_per_second = _channel_points_per_second(
                executor=pool,
                channels=float_channels,
                start=time_range.start_time,
                end=time_range.end_time,
                tags=tags,
            )

        # Compute slice duration using maximum points per second of all exported channels,
        # if the user hasn't provided one
        if batch_duration is None:
            if enum_channels or unknown_channels:
                logger.warning(
                    "No batch_duration provided, but exporting %d enum and %d unknown channels. "
                    "These will not be accounted for the computed duration!",
                    len(enum_channels),
                    len(unknown_channels),
                )

            total_point_rate = sum(points_per_second.values())
            computed_duration = datetime.timedelta(seconds=self._points_per_slice / total_point_rate)
            requested_duration = datetime.timedelta(seconds=(time_range.end_time - time_range.start_time) / 1e9)
            batch_duration = min(computed_duration, requested_duration)
            logger.info(
                "Expecting %d points in %f seconds",
                total_point_rate * requested_duration.total_seconds(),
                requested_duration.total_seconds(),
            )

        # Start by making each enum or unknown channel into its own group, as we cannot determine
        # the data rate. This assumes that the highest rate channel being exported is a float channel,
        # a relatively safe assumption in practice.
        channel_groups = [[channel] for channel in [*enum_channels, *unknown_channels]]

        # Channels that wouldn't fit in a single request
        large_channels = []

        allowed_rate_per_group = self._points_per_request / batch_duration.total_seconds()
        curr_group: list[Channel] = []
        curr_rate = 0.0
        for channel_name, channel_rate in sorted(points_per_second.items(), key=lambda tup: tup[1], reverse=True):
            channel = channels_by_name[channel_name]
            if channel_rate > allowed_rate_per_group:
                large_channels.append(channel)
                continue

            if curr_rate + channel_rate > allowed_rate_per_group or len(curr_group) >= self._max_channels_per_request:
                channel_groups.append(curr_group)
                curr_group = []
                curr_rate = 0.0

            curr_group.append(channel)
            curr_rate += channel_rate

        # Add last group in progress to channel groups
        if curr_group:
            channel_groups.append(curr_group)

        jobs = {}
        offset_ns = int(batch_duration.total_seconds() * 1e9)
        for slice in time_range.subdivide(offset_ns):
            jobs[slice] = [
                ExportJob(
                    channels=channel_group,
                    time_slice=slice,
                    tags=tags if tags else {},
                    buckets=buckets,
                    resolution_ns=resolution_ns,
                    timestamp_type=timestamp_type,
                )
                for channel_group in channel_groups
            ]

            # For large channels, we need to subdivide the time range based on their data rates
            for channel in large_channels:
                channel_rate = points_per_second[channel.name]
                sub_offset = datetime.timedelta(seconds=self._points_per_request / channel_rate)
                sub_offset_ns = int(sub_offset.total_seconds() * 1e9)
                for sub_slice in slice.subdivide(sub_offset_ns):
                    jobs[slice].append(
                        ExportJob(
                            channels=[channel],
                            time_slice=sub_slice,
                            tags=tags if tags else {},
                            buckets=buckets,
                            resolution_ns=resolution_ns,
                            timestamp_type=timestamp_type,
                        )
                    )

        return jobs
