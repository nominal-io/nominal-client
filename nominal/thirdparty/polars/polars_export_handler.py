import collections
import concurrent.futures
import dataclasses
import datetime
import logging
from typing import Iterator, Mapping, Sequence

import polars as pl
from nominal_api import api, scout_compute_api, scout_dataexport_api
from typing_extensions import Self

from nominal._utils import LogTiming
from nominal._utils.iterator_tools import batched
from nominal.core.channel import Channel, ChannelDataType, filter_channels_with_data
from nominal.core.client import NominalClient
from nominal.core.datasource import DataSource
from nominal.ts import (
    Epoch,
    IntegralNanosecondsDuration,
    IntegralNanosecondsUTC,
    Iso8601,
    Relative,
    _AnyExportableTimestampType,
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

# Maximum number of buckets / decimated points exported per compute query.
# TODO(drake) raise 1000 limit once backend limit is raised
MAX_NUM_BUCKETS = 1000

DEFAULT_EXPORTED_TIMESTAMP_COL_NAME = "timestamp"
_INTERNAL_TS_COL = "__nmnl_ts__"  # internal join key, chosen to avoid collision with channel names

logger = logging.getLogger(__name__)


def _timestamp_nanos(ts: api.Timestamp) -> IntegralNanosecondsUTC:
    """Convert a conjure Timestamp to nanoseconds UTC."""
    return ts.seconds * 1_000_000_000 + ts.nanos


def _extract_bucket_counts(
    response: scout_compute_api.ComputeNodeResponse,
) -> Sequence[tuple[IntegralNanosecondsUTC, int]]:
    """Extract (timestamp, point_count) pairs from a compute response.

    Works for both numeric and enum series. Handles bucketed (decimated) responses
    as well as undecimated fallbacks when the data has fewer points than requested buckets.
    """
    # Numeric — decimated into buckets with statistics
    if response.bucketed_numeric is not None:
        return [
            (_timestamp_nanos(ts), bucket.count)
            for ts, bucket in zip(response.bucketed_numeric.timestamps, response.bucketed_numeric.buckets)
        ]

    # Numeric — undecimated (fewer points than requested buckets)
    if response.numeric is not None:
        return [(_timestamp_nanos(ts), 1) for ts in response.numeric.timestamps]

    # Numeric — single point
    if response.numeric_point is not None:
        return [(_timestamp_nanos(response.numeric_point.timestamp), 1)]

    # Enum — decimated into buckets with histograms
    if response.bucketed_enum is not None:
        return [
            (_timestamp_nanos(ts), sum(bucket.histogram.values()))
            for ts, bucket in zip(response.bucketed_enum.timestamps, response.bucketed_enum.buckets)
        ]

    # Enum — undecimated (fewer points than requested buckets)
    if response.enum is not None:
        return [(_timestamp_nanos(ts), 1) for ts in response.enum.timestamps]

    logger.warning("Unrecognized compute response type: %s", response.type)
    return []


def _build_compute_request(
    series: scout_compute_api.Series,
    start: api.Timestamp,
    end: api.Timestamp,
    num_buckets: int,
) -> scout_compute_api.ComputeNodeRequest:
    """Build a decimation compute request for a single series."""
    return scout_compute_api.ComputeNodeRequest(
        context=scout_compute_api.Context(variables={}),
        node=scout_compute_api.ComputableNode(
            series=scout_compute_api.SummarizeSeries(
                input=series,
                numeric_aggregations={},
                summarization_strategy=scout_compute_api.SummarizationStrategy(
                    decimate=scout_compute_api.DecimateStrategy(
                        buckets=scout_compute_api.DecimateWithBuckets(buckets=num_buckets)
                    )
                ),
                buckets=num_buckets,
            )
        ),
        start=start,
        end=end,
    )


def _max_points_per_second(
    bucket_counts: Sequence[tuple[IntegralNanosecondsUTC, int]],
    start_ns: IntegralNanosecondsUTC,
    end_ns: IntegralNanosecondsUTC,
) -> float:
    """Compute the maximum points-per-second from a sequence of (timestamp, count) bucket data.

    For a single bucket, uses the full time range as the duration. For multiple buckets,
    computes PPS between consecutive bucket timestamps and returns the maximum.
    Returns 0.0 if the time range or any bucket interval has zero duration.
    """
    if len(bucket_counts) == 0:
        return 0.0
    elif len(bucket_counts) == 1:
        total_duration = (end_ns - start_ns) / 1e9
        if total_duration <= 0:
            return 0.0
        return bucket_counts[0][1] / total_duration
    else:
        max_pps = 0.0
        for idx in range(1, len(bucket_counts)):
            ts, count = bucket_counts[idx]
            prev_ts = bucket_counts[idx - 1][0]
            duration = (ts - prev_ts) / 1e9
            if duration > 0:
                max_pps = max(max_pps, count / duration)
        return max_pps


def _batch_channel_points_per_second(
    client: NominalClient,
    channels: Sequence[Channel],
    start_ns: IntegralNanosecondsUTC,
    end_ns: IntegralNanosecondsUTC,
    tags: dict[str, str],
    num_buckets: int,
) -> Mapping[str, float | None]:
    """For each provided channel, determine the maximum points per second in the given range.

    Supports all channel data types (DOUBLE, INT, STRING) by building the appropriate
    compute series for each and submitting a single BatchComputeWithUnitsRequest.

    NOTE: Not intended for direct use — see `_channel_points_per_second`.
    NOTE: Do not use with more than 300 channels, or 500 concurrently across all requests.

    Args:
        client: Nominal request client
        channels: Channels to query data rates for
        start_ns: Start of the time range to query over
        end_ns: End of the time range to query over
        tags: Key-value pairs of tags to filter data with
        num_buckets: Number of buckets to use — more typically leads to better results.
            NOTE: max number of buckets allowed is 1000

    Returns:
        Mapping of channel name to maximum points/second for the respective channels
    """
    if not channels:
        logger.warning("No channels given!")
        return {}
    elif num_buckets > MAX_NUM_BUCKETS:
        raise ValueError(f"num_buckets ({num_buckets}) must be <= {MAX_NUM_BUCKETS}")

    # Build a compute Series for each channel. _to_compute_series dispatches to
    # NumericSeries for DOUBLE/INT and EnumSeries for STRING channels.
    results: dict[str, float | None] = {}
    series_list: list[scout_compute_api.Series] = []
    channels_in_request: list[Channel] = []
    for channel in channels:
        try:
            series_list.append(channel._to_compute_series(tags=tags))
            channels_in_request.append(channel)
        except ValueError:
            logger.warning(
                "Cannot compute points per second for channel %s with type: %s",
                channel.name,
                channel.data_type,
            )
            results[channel.name] = None

    if not series_list:
        return results

    api_start = _SecondsNanos.from_nanoseconds(start_ns).to_api()
    api_end = _SecondsNanos.from_nanoseconds(end_ns).to_api()

    try:
        request = scout_compute_api.BatchComputeWithUnitsRequest(
            requests=[_build_compute_request(s, api_start, api_end, num_buckets) for s in series_list]
        )
        resp = client._clients.compute.batch_compute_with_units(
            auth_header=client._clients.auth_header,
            request=request,
        )
    except Exception:
        logger.exception("Failed to compute buckets for channels: %s", [ch.name for ch in channels_in_request])
        return {ch.name: None for ch in channels}

    for channel, result in zip(channels_in_request, resp.results):
        compute_result = result.compute_result
        if compute_result is None or compute_result.error is not None:
            error_msg = compute_result.error if compute_result else "no result"
            logger.warning("Failed to compute point rate for channel '%s': %s", channel.name, error_msg)
            results[channel.name] = None
            continue

        assert compute_result.success is not None
        bucket_counts = _extract_bucket_counts(compute_result.success)
        if not bucket_counts:
            logger.warning("No points found in range for channel '%s'", channel.name)
            results[channel.name] = 0.0
        else:
            results[channel.name] = _max_points_per_second(bucket_counts, start_ns, end_ns)

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
    start_ns = _SecondsNanos.from_flexible(start).to_nanoseconds()
    end_ns = _SecondsNanos.from_flexible(end).to_nanoseconds()
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = {}
        for channel_batch in batched(channels, DEFAULT_CHANNELS_PER_REQUEST):
            fut = pool.submit(
                _batch_channel_points_per_second,
                client,
                channel_batch,
                start_ns,
                end_ns,
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

            try:
                res = fut.result()
            except Exception:
                logger.exception(
                    "Failed to extract %d channel sample rates: %s",
                    len(channel_batch),
                    [ch.name for ch in channel_batch],
                )
                continue
            for channel, rate in res.items():
                results[channel] = rate

        return results


def _is_numeric_type(data_type: ChannelDataType | None) -> bool:
    """True for channel types that can coexist in a single export request (DOUBLE, INT)."""
    return data_type in (ChannelDataType.DOUBLE, ChannelDataType.INT)


def _build_channel_groups(
    points_per_second: Mapping[str, float],
    channels_by_name: Mapping[str, Channel],
    points_per_request: int,
    channels_per_request: int,
    batch_duration: datetime.timedelta,
) -> tuple[list[list[Channel]], list[Channel]]:
    """Build groups of channels to read together, and channels that must be read on their own.

    Channels are first partitioned by type compatibility — the backend currently
    cannot mix numeric (DOUBLE/INT) and string/enum channels in the same export
    request. Within each partition, channels are grouped by data rate to stay
    within the per-request point budget.
    """
    # Channels that can be read entirely in a single export request for a batch
    channel_groups: list[list[Channel]] = []

    # Channels that wouldn't fit in a single export request for a batch
    large_channels: list[Channel] = []

    batch_seconds = batch_duration.total_seconds()
    if batch_seconds <= 0:
        raise ValueError(f"batch_duration must be positive, got {batch_duration}")
    allowed_rate_per_group = points_per_request / batch_seconds

    # Partition channels by type compatibility: numeric (DOUBLE/INT) can share
    # requests, but STRING channels must be in separate requests.
    # TODO: remove this partitioning once the backend supports mixed-type exports.
    numeric_pps: dict[str, float] = {}
    string_pps: dict[str, float] = {}
    for name, rate in points_per_second.items():
        channel = channels_by_name.get(name)
        if channel is not None and _is_numeric_type(channel.data_type):
            numeric_pps[name] = rate
        else:
            string_pps[name] = rate

    for partition_pps in (numeric_pps, string_pps):
        curr_group: list[Channel] = []
        curr_rate = 0.0
        for channel_name, channel_rate in sorted(partition_pps.items(), key=lambda tup: tup[1], reverse=True):
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
class _TimeRange:
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
class _ExportJob:
    """Represents an individual export task suitable for giving to subprocesses."""

    datasource_rid: str
    channel_names: list[str]
    channel_types: dict[str, ChannelDataType | None]

    # Time bounds to export
    time_slice: _TimeRange

    # Key-value pairs to filter channels by
    tags: dict[str, str]

    # Decimation settings
    buckets: int | None = None
    resolution: IntegralNanosecondsDuration | None = None

    # Timestamp formatting
    timestamp_type: _AnyExportableTimestampType = "epoch_seconds"

    def resolution_options(self) -> scout_dataexport_api.ResolutionOption:
        """Construct data export resolution options based on bucketing and resolution parameters."""
        if self.buckets is not None and self.resolution is not None:
            raise ValueError("Only one of buckets or resolution may be provided")
        elif self.buckets is None and self.resolution is None:
            return scout_dataexport_api.ResolutionOption(undecimated=scout_dataexport_api.UndecimatedResolution())
        else:
            return scout_dataexport_api.ResolutionOption(nanoseconds=self.resolution, buckets=self.buckets)

    def export_channels(self, datasource: DataSource) -> scout_dataexport_api.ExportChannels:
        """Construct data export channels for the configured channels and export options."""
        channels = datasource.get_channels(names=self.channel_names)
        return scout_dataexport_api.ExportChannels(
            time_domain=scout_dataexport_api.ExportTimeDomainChannels(
                channels=[channel._to_time_domain_channel(tags=self.tags) for channel in channels],
                merge_timestamp_strategy=scout_dataexport_api.MergeTimestampStrategy(
                    none=scout_dataexport_api.NoneStrategy()
                ),
                output_timestamp_format=_to_export_timestamp_format(self.timestamp_type),
            )
        )

    def export_request(self, datasource: DataSource) -> scout_dataexport_api.ExportDataRequest:
        """Construct conjure export request given the provided configuration options."""
        return scout_dataexport_api.ExportDataRequest(
            channels=self.export_channels(datasource),
            context=scout_compute_api.Context(function_variables={}, variables={}),
            end_time=self.time_slice.end_api,
            start_time=self.time_slice.start_api,
            resolution=self.resolution_options(),
            compression=scout_dataexport_api.CompressionFormat.GZIP,
            format=scout_dataexport_api.ExportFormat(
                csv=scout_dataexport_api.Csv(),
            ),
        )


def _format_time_col(df: pl.DataFrame, time_col: str, job: _ExportJob) -> pl.DataFrame:
    typed_timestamp_type = _to_typed_timestamp_type(job.timestamp_type)

    if isinstance(typed_timestamp_type, (Relative, Epoch)):
        # Already numeric/relative per export service; no transform.
        return df.with_columns(pl.col(time_col).cast(pl.Float64).alias(time_col))
    elif isinstance(typed_timestamp_type, Iso8601):
        # Parse ISO8601 into timezone-aware datetime
        return df.with_columns(pl.col(time_col).str.strptime(pl.Datetime, strict=False, exact=False).alias(time_col))
    else:
        raise ValueError("Expected timestamp type to be a typed timestamp type")


def _get_exported_timestamp_channel(channel_names: list[str]) -> str:
    # skip data channel names, and find the highest numbered "timestamp" channel
    renamed_timestamp_col = DEFAULT_EXPORTED_TIMESTAMP_COL_NAME
    if DEFAULT_EXPORTED_TIMESTAMP_COL_NAME in channel_names:
        idx = 1
        while True:
            other_col_name = f"{DEFAULT_EXPORTED_TIMESTAMP_COL_NAME}.{idx}"
            if other_col_name not in channel_names:
                renamed_timestamp_col = other_col_name
                break
            else:
                idx += 1

    return renamed_timestamp_col


def _export_job(job: _ExportJob, client: NominalClient) -> pl.DataFrame:
    if not job.channel_names:
        raise ValueError("No channels to extract")

    # Warn user about renamed channels, handle data with channel names of "timestamp"
    time_col = _get_exported_timestamp_channel(job.channel_names)

    datasource = client.get_datasource(job.datasource_rid)
    req = job.export_request(datasource)
    resp = client._clients.dataexport.export_channel_data(client._clients.auth_header, req)

    # force schema for export based on known channel types (helps if columns are all nan for a given part to prevent
    # that channel from loading as strings)
    schema: dict[str, pl.DataType] = {}
    for channel_name, data_type in job.channel_types.items():
        match data_type:
            case ChannelDataType.STRING:
                schema[channel_name] = pl.String()
            case ChannelDataType.DOUBLE:
                schema[channel_name] = pl.Float64()
            case ChannelDataType.INT:
                schema[channel_name] = pl.Int64()
            case _:
                logger.warning("Can't add missing channel %s to dataframe-- no known datatype!", channel_name)
                continue

    # Read CSV via Polars
    df = pl.read_csv(resp, schema_overrides=schema)
    if df.is_empty():
        logger.warning("No data found for export for channels %s", job.channel_names)
        return pl.DataFrame({col: [] for col in [*job.channel_names, time_col]})
    elif len(df[time_col].unique()) != len(df[time_col]):
        logger.error("Dataframe has duplicate timestamps! %s", df.head())

    # Convert string-based timestamps into native timestamp objects or floats based on desired export type
    df = _format_time_col(df, time_col, job)

    # Create internal timestamp column for consistent joins; keep original time_col out of the way
    df = df.rename({time_col: _INTERNAL_TS_COL})

    # Place timestamps first, then the data channels, with rows sorted by timestamps
    ordered_cols = [_INTERNAL_TS_COL] + [c for c in df.columns if c not in (_INTERNAL_TS_COL, time_col)]
    df = df.select(ordered_cols).sort(by=pl.col(_INTERNAL_TS_COL))

    # Add columns missing from the data to the dataframe for schema inference
    missing_channels = [channel_name for channel_name in job.channel_names if channel_name not in df.columns]
    if missing_channels:
        logger.warning("Found %d missing channels", len(missing_channels))
        channel_exprs = {}
        for channel_name in missing_channels:
            if channel_name in schema:
                channel_exprs[channel_name] = pl.lit(None).cast(schema[channel_name])
            else:
                logger.warning("Cannot infer type for channel %s, not exporting", channel_name)

        df = df.with_columns(**channel_exprs)

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

    def _compute_batch_duration(
        self,
        batch_duration: datetime.timedelta | None,
        time_range: _TimeRange,
        points_per_second: Mapping[str, float],
    ) -> IntegralNanosecondsDuration:
        """Compute the batch duration for export time slices.

        If no explicit batch_duration is provided, computes one based on the total
        point rate across all channels and the configured points_per_dataframe limit.
        """
        if batch_duration is None:
            total_point_rate = sum(points_per_second.values())
            if total_point_rate == 0.0:
                logger.warning("No data detected in time range, attempting to export in one batch")
                computed_duration = time_range.duration()
            else:
                computed_duration = datetime.timedelta(seconds=self._points_per_dataframe / total_point_rate)

            # If the computed max batch duration is greater than the requested export duration, truncate
            return min(int(computed_duration.total_seconds() * 1e9), time_range.duration_ns())
        else:
            return int(batch_duration.total_seconds() * 1e9)

    def _compute_export_jobs(
        self,
        channels: Sequence[Channel],
        time_range: _TimeRange,
        timestamp_type: _AnyExportableTimestampType,
        tags: Mapping[str, str] | None = None,
        buckets: int | None = None,
        resolution: IntegralNanosecondsDuration | None = None,
        batch_duration: datetime.timedelta | None = None,
    ) -> Mapping[_TimeRange, Sequence[_ExportJob]]:
        """Compute the mapping of export time slices to the sequence of export jobs to produce data for that range."""
        if buckets is not None and resolution is not None:
            raise ValueError("Cannot provide `buckets` and `resolution`")

        # Exclude channels with unsupported data types
        supported_channels = [ch for ch in channels if ch.data_type in (
            ChannelDataType.DOUBLE, ChannelDataType.INT, ChannelDataType.STRING,
        )]
        unsupported = len(channels) - len(supported_channels)
        if unsupported:
            logger.warning("Could not determine datatypes of %d channels -- ignoring for export", unsupported)

        # Fast server-side filter: identify which channels have data in the time range
        supported_channels = list(filter_channels_with_data(
            supported_channels,
            tags=tags,
            start_time=time_range.start_time,
            end_time=time_range.end_time,
        ))

        # Compute point rates for all channel types (DOUBLE, INT, STRING)
        channels_by_name = {channel.name: channel for channel in supported_channels}
        all_pps = _channel_points_per_second(
            client=self._client,
            channels=supported_channels,
            start=time_range.start_time,
            end=time_range.end_time,
            tags=tags,
        )
        points_per_second = {name: rate for name, rate in all_pps.items() if rate}
        batch_duration_ns = self._compute_batch_duration(batch_duration, time_range, points_per_second)

        # Group channels by datasource for export job creation
        channel_names_by_datasource: dict[str, set[str]] = collections.defaultdict(set)
        for channel in supported_channels:
            channel_names_by_datasource[channel.data_source].add(channel.name)

        jobs: dict[_TimeRange, list[_ExportJob]] = collections.defaultdict(list)
        time_slices = time_range.subdivide_ns(batch_duration_ns)
        for datasource_rid, channel_names in channel_names_by_datasource.items():
            channel_groups, large_channels = _build_channel_groups(
                {k: v for k, v in points_per_second.items() if k in channel_names},
                {k: v for k, v in channels_by_name.items() if k in channel_names},
                self._points_per_request,
                self._channels_per_request,
                datetime.timedelta(seconds=batch_duration_ns / 1e9),
            )

            for time_slice in time_slices:
                for channel_group in channel_groups:
                    jobs[time_slice].append(
                        _ExportJob(
                            datasource_rid=datasource_rid,
                            channel_names=[ch.name for ch in channel_group],
                            channel_types={ch.name: ch.data_type for ch in channel_group},
                            time_slice=time_slice,
                            tags=dict(tags or {}),
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
                    for sub_slice in time_slice.subdivide(sub_offset):
                        jobs[time_slice].append(
                            _ExportJob(
                                datasource_rid=datasource_rid,
                                channel_names=[channel.name],
                                channel_types={channel.name: channel.data_type},
                                time_slice=sub_slice,
                                tags=dict(tags or {}),
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
        timestamp_type: _AnyExportableTimestampType = "epoch_seconds",
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

        if resolution is not None:
            # If the batch duration is higher than this number, and data is actually downsampled with the
            # given resolution, then it would error today if the batch duration is any larger than this.
            computed_batch_duration = datetime.timedelta(seconds=(resolution * MAX_NUM_BUCKETS) / 1e9)
            if batch_duration is None:
                logger.info(
                    "Manually setting batch_duration to %fs (resolution=%dns)",
                    computed_batch_duration.total_seconds(),
                    resolution,
                )
                batch_duration = computed_batch_duration
            elif computed_batch_duration < batch_duration:
                logger.warning(
                    "Configured batch_duration of %fs would result in failing exports with resolution=%dns. "
                    "Setting batch_duration to %fs instead.",
                    batch_duration.total_seconds(),
                    resolution,
                    computed_batch_duration.total_seconds(),
                )
                batch_duration = computed_batch_duration

        # Determine download schedule
        export_jobs = self._compute_export_jobs(
            channels, _TimeRange(start, end), timestamp_type, tags or {}, buckets, resolution, batch_duration
        )
        time_column = _get_exported_timestamp_channel([ch.name for ch in channels])
        yield from self._export_dataframes(export_jobs, time_column, join_batches)

    def _export_dataframes(
        self, export_jobs: Mapping[_TimeRange, Sequence[_ExportJob]], time_column: str, join_batches: bool
    ) -> Iterator[pl.DataFrame]:
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
                                self._client,
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
                            yield res.rename({_INTERNAL_TS_COL: time_column})

                    # Schedule next batch of downloads before starting merge
                    if idx < len(time_slices) - 1:
                        futures = [
                            pool.submit(_export_job, task, self._client) for task in export_jobs[time_slices[idx + 1]]
                        ]

                if join_batches:
                    with LogTiming(f"Merged {len(results)} exports"):
                        logger.info("Merging dataframes")
                        merged_df = _merge_dfs(results)
                        if merged_df.is_empty():
                            logger.warning("Dataframe empty after merging...")
                        else:
                            yield merged_df.rename({_INTERNAL_TS_COL: time_column})
