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

# Channel data types that can be exported as dataframe columns.
_EXPORTABLE_DATA_TYPES: frozenset[ChannelDataType] = frozenset(
    [ChannelDataType.DOUBLE, ChannelDataType.INT, ChannelDataType.STRING]
)

DEFAULT_EXPORTED_TIMESTAMP_COL_NAME = "timestamp"
_INTERNAL_TS_COL = "__nmnl_ts__"  # internal join key, chosen to avoid collision with channel names

logger = logging.getLogger(__name__)


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
            (_SecondsNanos.from_api(ts).to_nanoseconds(), bucket.count)
            for ts, bucket in zip(response.bucketed_numeric.timestamps, response.bucketed_numeric.buckets)
        ]

    # Numeric — undecimated (fewer points than requested buckets)
    if response.numeric is not None:
        return [(_SecondsNanos.from_api(ts).to_nanoseconds(), 1) for ts in response.numeric.timestamps]

    # Numeric — single point
    if response.numeric_point is not None:
        return [(_SecondsNanos.from_api(response.numeric_point.timestamp).to_nanoseconds(), 1)]

    # Enum — decimated into buckets with histograms
    if response.bucketed_enum is not None:
        return [
            (_SecondsNanos.from_api(ts).to_nanoseconds(), sum(bucket.histogram.values()))
            for ts, bucket in zip(response.bucketed_enum.timestamps, response.bucketed_enum.buckets)
        ]

    # Enum — undecimated (fewer points than requested buckets)
    if response.enum is not None:
        return [(_SecondsNanos.from_api(ts).to_nanoseconds(), 1) for ts in response.enum.timestamps]

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


def _peak_points_per_second(
    bucket_counts: Sequence[tuple[IntegralNanosecondsUTC, int]],
    start_ns: IntegralNanosecondsUTC,
    end_ns: IntegralNanosecondsUTC,
) -> float:
    """Compute the peak points-per-second from a sequence of (timestamp, count) bucket data.

    For a single bucket, uses the full time range as the duration. For multiple buckets,
    computes PPS between consecutive bucket timestamps and returns the peak.
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
        peak_pps = 0.0
        for idx in range(1, len(bucket_counts)):
            ts, count = bucket_counts[idx]
            prev_ts = bucket_counts[idx - 1][0]
            duration = (ts - prev_ts) / 1e9
            if duration > 0:
                peak_pps = max(peak_pps, count / duration)
        return peak_pps


def _batch_channel_points_per_second(
    client: NominalClient,
    channels: Sequence[Channel],
    start_ns: IntegralNanosecondsUTC,
    end_ns: IntegralNanosecondsUTC,
    tags: dict[str, str],
    num_buckets: int,
) -> Mapping[tuple[str, str], float | None]:
    """For each provided channel, determine the peak points per second in the given range.

    Supports all channel data types (DOUBLE, INT, STRING) by building the appropriate
    compute series for each and submitting a single BatchComputeWithUnitsRequest.

    NOTE: Not intended for direct use — see `_channel_points_per_second`.
    NOTE: Do not use with more than 300 channels, or 500 concurrently across all requests.

    Args:
        client: Nominal request client
        channels: Channels to query data rates for (must be DOUBLE, INT, or STRING)
        start_ns: Start of the time range to query over
        end_ns: End of the time range to query over
        tags: Key-value pairs of tags to filter data with
        num_buckets: Number of buckets to use — more typically leads to better results.
            NOTE: max number of buckets allowed is 1000

    Returns:
        Mapping of (data_source, channel_name) to peak points/second. A value of `None`
        indicates the backend failed to compute a rate for that channel (e.g. transient
        error); `0.0` indicates the compute succeeded but returned no buckets.

    Raises:
        ValueError: If any channel has an unsupported data type (not DOUBLE/INT/STRING).
    """
    if not channels:
        return {}
    elif num_buckets > MAX_NUM_BUCKETS:
        raise ValueError(f"num_buckets ({num_buckets}) must be <= {MAX_NUM_BUCKETS}")

    # _to_compute_series raises ValueError for unsupported types; callers are expected to
    # pre-filter to DOUBLE/INT/STRING channels (see _compute_export_jobs).
    series_list = [channel._to_compute_series(tags=tags) for channel in channels]

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
        logger.exception("Failed to compute buckets for channels: %s", [ch.name for ch in channels])
        return {(ch.data_source, ch.name): None for ch in channels}

    results: dict[tuple[str, str], float | None] = {}
    for channel, result in zip(channels, resp.results):
        key = (channel.data_source, channel.name)
        compute_result = result.compute_result
        if compute_result is None or compute_result.error is not None:
            error_msg = compute_result.error if compute_result else "no result"
            logger.warning("Failed to compute point rate for channel '%s': %s", channel.name, error_msg)
            results[key] = None
            continue

        if compute_result.success is None:
            logger.warning("Compute succeeded for channel '%s' but response is empty", channel.name)
            results[key] = None
            continue

        bucket_counts = _extract_bucket_counts(compute_result.success)
        if not bucket_counts:
            logger.warning("No points found in range for channel '%s'", channel.name)
            results[key] = 0.0
        else:
            results[key] = _peak_points_per_second(bucket_counts, start_ns, end_ns)

    return results


def _channel_points_per_second(
    client: NominalClient,
    channels: Sequence[Channel],
    start: _InferrableTimestampType,
    end: _InferrableTimestampType,
    tags: Mapping[str, str] | None = None,
    num_buckets: int = 100,
    num_workers: int = DEFAULT_NUM_WORKERS,
) -> Mapping[tuple[str, str], float | None]:
    """For each provided channel, determine the peak number of points per second in the given range.

    Splits channels into batches of `DEFAULT_CHANNELS_PER_REQUEST` and queries each batch in parallel
    via an internally-managed thread pool.

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
        Mapping of (data_source, channel_name) to peak points/second, or None when the
        estimation failed. Keying by the tuple keeps same-named channels from different
        datasources independent.
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

        results: dict[tuple[str, str], float | None] = {}
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
            results.update(res)

        return results


def _build_channel_groups(
    points_per_second: Mapping[tuple[str, str], float],
    channels_by_key: Mapping[tuple[str, str], Channel],
    points_per_request: int,
    max_channels_per_group: int,
    batch_duration: datetime.timedelta,
) -> tuple[list[list[Channel]], list[Channel]]:
    """Bin-pack channels into groups that fit the per-request rate and channel-count budgets.

    Channels whose individual rate exceeds the per-group budget are returned separately as
    `large_channels` — the caller handles subdividing their time ranges. Groups are built
    highest-rate-first so any NaN padding from uneven channel lengths sits at the tail of
    each export.
    """
    batch_seconds = batch_duration.total_seconds()
    if batch_seconds <= 0:
        raise ValueError(f"batch_duration must be positive, got {batch_duration}")
    allowed_rate_per_group = points_per_request / batch_seconds

    # Defensive skip: names in points_per_second should also be in channels_by_key, but
    # don't raise if the invariant breaks.
    sorted_pairs = sorted(
        ((channels_by_key[key], rate) for key, rate in points_per_second.items() if key in channels_by_key),
        key=lambda pair: pair[1],
        reverse=True,
    )

    groups: list[list[Channel]] = []
    large: list[Channel] = []
    curr_group: list[Channel] = []
    curr_rate = 0.0
    for channel, rate in sorted_pairs:
        if rate > allowed_rate_per_group:
            large.append(channel)
            continue

        # If the current group is too big to fit the current channel, close it out.
        if curr_rate + rate > allowed_rate_per_group or len(curr_group) >= max_channels_per_group:
            groups.append(curr_group)
            curr_group = []
            curr_rate = 0.0

        curr_group.append(channel)
        curr_rate += rate

    if curr_group:
        groups.append(curr_group)

    return groups, large


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


def _compute_batch_duration(
    batch_duration: datetime.timedelta | None,
    time_range: _TimeRange,
    points_per_second: Mapping[tuple[str, str], float],
    points_per_dataframe: int,
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
            computed_duration = datetime.timedelta(seconds=points_per_dataframe / total_point_rate)

        # If the computed max batch duration is greater than the requested export duration, truncate
        return min(int(computed_duration.total_seconds() * 1e9), time_range.duration_ns())
    else:
        return int(batch_duration.total_seconds() * 1e9)


@dataclasses.dataclass(frozen=True, unsafe_hash=True)
class _ExportJob:
    """Represents a single CSV export request dispatched to a worker thread."""

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
    """Streams data out of Nominal into Polars DataFrames.

    Pipeline:
    * Filter to exportable channel types (DOUBLE/INT/STRING).
    * Confirm each channel has data in the range (via `filter_channels_with_data`) and
      estimate per-channel peak points-per-second.
    * Compute a batch duration such that each DataFrame batch stays under `points_per_dataframe`,
      unless the caller passes an explicit `batch_duration`.
    * Bin-pack channels into per-request groups within the `points_per_request` rate budget;
      channels whose rate exceeds the budget are split across sub-slices of the time range.
    * For each time slice, fetch channel groups in parallel and stitch the results back into a
      single DataFrame (via vertical concat within equal-column groups, outer-join across groups
      on the timestamp column) before yielding.
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

    def _compute_channel_rates(
        self,
        channels: Sequence[Channel],
        time_range: _TimeRange,
        tags: Mapping[str, str] | None,
    ) -> tuple[list[Channel], dict[tuple[str, str], float]]:
        """Filter by data presence, compute PPS, and fill in 0.0 for unknown/missing rates.

        Returns the filtered channel list (confirmed to have data in the range) and a
        mapping from (data_source, channel_name) to estimated peak PPS. Channels with
        failed/degenerate PPS estimation default to 0.0 so they aren't silently dropped
        downstream.
        """
        # Cost gate: PPS compute alone correctly handles empty-data channels, but this filter
        # avoids running the expensive compute on channels with no data in the range.
        supported_channels = list(
            filter_channels_with_data(
                channels,
                tags=tags,
                start_time=time_range.start_time,
                end_time=time_range.end_time,
            )
        )
        all_pps = _channel_points_per_second(
            client=self._client,
            channels=supported_channels,
            start=time_range.start_time,
            end=time_range.end_time,
            tags=tags,
        )
        # Drop channels whose rate estimator returned 0.0 or None — either the channel has no
        # data in range (0.0) or the compute failed (None); either way there's nothing to
        # export, and keeping them would just waste an export request per channel.
        points_per_second = {key: rate for key, rate in all_pps.items() if rate}
        exportable_channels = [ch for ch in supported_channels if (ch.data_source, ch.name) in points_per_second]
        return exportable_channels, points_per_second

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
        """Compute the mapping of export time slices to the sequence of export jobs to produce data for that range.

        `channels` is expected to be already filtered to exportable data types by the caller.
        """
        if buckets is not None and resolution is not None:
            raise ValueError("Cannot provide `buckets` and `resolution`")

        supported_channels, points_per_second = self._compute_channel_rates(channels, time_range, tags)
        batch_duration_ns = _compute_batch_duration(
            batch_duration, time_range, points_per_second, self._points_per_dataframe
        )
        time_slices = time_range.subdivide_ns(batch_duration_ns)
        batch_timedelta = datetime.timedelta(seconds=batch_duration_ns / 1e9)

        channels_by_datasource: dict[str, list[Channel]] = collections.defaultdict(list)
        for channel in supported_channels:
            channels_by_datasource[channel.data_source].append(channel)

        jobs: dict[_TimeRange, list[_ExportJob]] = collections.defaultdict(list)
        for datasource_rid, ds_channels in channels_by_datasource.items():
            ds_jobs = self._build_jobs_for_datasource(
                datasource_rid=datasource_rid,
                channels=ds_channels,
                points_per_second=points_per_second,
                time_slices=time_slices,
                batch_duration=batch_timedelta,
                timestamp_type=timestamp_type,
                tags=tags,
                buckets=buckets,
                resolution=resolution,
            )
            for time_slice, slice_jobs in ds_jobs.items():
                jobs[time_slice].extend(slice_jobs)

        return jobs

    def _build_jobs_for_datasource(
        self,
        *,
        datasource_rid: str,
        channels: Sequence[Channel],
        points_per_second: Mapping[tuple[str, str], float],
        time_slices: Sequence[_TimeRange],
        batch_duration: datetime.timedelta,
        timestamp_type: _AnyExportableTimestampType,
        tags: Mapping[str, str] | None,
        buckets: int | None,
        resolution: IntegralNanosecondsDuration | None,
    ) -> Mapping[_TimeRange, list[_ExportJob]]:
        """Build export jobs for a single datasource's channels, keyed by parent time slice.

        Channels are bin-packed by rate; any channel whose rate exceeds the per-request budget
        is subdivided across sub-slices of each parent time slice, producing one single-channel
        job per sub-slice.
        """
        channels_by_key = {(ch.data_source, ch.name): ch for ch in channels}
        ds_pps = {key: points_per_second[key] for key in channels_by_key}
        channel_groups, large_channels = _build_channel_groups(
            ds_pps, channels_by_key, self._points_per_request, self._channels_per_request, batch_duration
        )

        def make_job(group: Sequence[Channel], slice_: _TimeRange) -> _ExportJob:
            return _ExportJob(
                datasource_rid=datasource_rid,
                channel_names=[ch.name for ch in group],
                channel_types={ch.name: ch.data_type for ch in group},
                time_slice=slice_,
                tags=dict(tags or {}),
                buckets=buckets,
                resolution=resolution,
                timestamp_type=timestamp_type,
            )

        jobs: dict[_TimeRange, list[_ExportJob]] = collections.defaultdict(list)
        for time_slice in time_slices:
            for group in channel_groups:
                jobs[time_slice].append(make_job(group, time_slice))
            # Large channels exceed the per-request rate budget, so subdivide the slice per
            # channel so each sub-slice fits. All sub-slice jobs roll up under the parent slice.
            for channel in large_channels:
                rate = ds_pps[(channel.data_source, channel.name)]
                sub_offset = datetime.timedelta(seconds=self._points_per_request / rate)
                for sub_slice in time_slice.subdivide(sub_offset):
                    jobs[time_slice].append(make_job([channel], sub_slice))
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
        """Yield exported data one DataFrame at a time.

        LOG / UNKNOWN channels are filtered out with a warning; only DOUBLE, INT, and STRING
        channels flow through the export pipeline.

        Args:
            channels: Channels to export.
            start: Start of the export time range (nanoseconds UTC).
            end: End of the export time range (nanoseconds UTC).
            tags: Key-value pairs used to filter channel data server-side.
            batch_duration: Explicit per-batch time window. If omitted, one is computed
                from total channel rate and `points_per_dataframe`.
            timestamp_type: Output timestamp representation (`epoch_seconds`, `iso8601`, etc.).
            buckets: Decimate each channel to at most this many buckets per request. Mutually
                exclusive with `resolution`.
            resolution: Decimate each channel to samples at this interval (nanoseconds).
                Mutually exclusive with `buckets`.
            join_batches: If True (default), each yielded DataFrame is the outer-joined merge of
                all channel groups for a single time slice. If False, yields one DataFrame per
                channel group without merging.

        Yields:
            Polars DataFrames covering successive time slices of the export range.
        """
        # Ensure user has selected channels to export
        if not channels:
            logger.warning("No channels requested for export-- returning")
            return

        # Ensure user has not selected incompatible decimation options
        if None not in (buckets, resolution):
            raise ValueError("Cannot export data decimated with both buckets and resolution")

        # Exclude channels with unsupported data types
        supported_channels = [ch for ch in channels if ch.data_type in _EXPORTABLE_DATA_TYPES]
        unsupported = len(channels) - len(supported_channels)
        if unsupported:
            logger.warning("Could not determine datatypes of %d channels -- ignoring for export", unsupported)

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
            supported_channels, _TimeRange(start, end), timestamp_type, tags or {}, buckets, resolution, batch_duration
        )
        time_column = _get_exported_timestamp_channel([ch.name for ch in supported_channels])
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
