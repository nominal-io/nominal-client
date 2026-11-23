import collections
import concurrent.futures
import dataclasses
import datetime
import logging
from typing import Iterator, Mapping, Self, Sequence

import polars as pl
from nominal._utils.iterator_tools import batched
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core.channel import Channel, ChannelDataType, _channels_with_data_and_cardinality
from nominal.core.client import NominalClient
from nominal.core.dataset import _get_datasets
from nominal.protos.compute.v1 import (
    computation_spec_pb2,
    compute_pb2,
    execution_inputs_pb2,
    execution_options_pb2,
    execution_results_pb2,
    expressions_pb2,
)
from nominal.protos.exportjobs.v1 import export_jobs_pb2 as jobs_api
from nominal.thirdparty.polars._export_jobs import compute_export_request, download_export, raw_export_query
from nominal.ts import (
    Epoch,
    IntegralNanosecondsDuration,
    IntegralNanosecondsUTC,
    Iso8601,
    Relative,
    _AnyExportableTimestampType,
    _InferrableTimestampType,
    _SecondsNanos,
    _to_typed_timestamp_type,
)

# Number of workers to use across thread / processes pools when hitting the api
DEFAULT_NUM_WORKERS = 8

# Output frame sizing and the default maximum job width are independent of compute metadata sampling.
DEFAULT_POINTS_PER_DATAFRAME = 25_000_000
DEFAULT_POINTS_PER_REQUEST = 1_000_000  # Previous per-request target, available for callers that opt into it.
DEFAULT_CHANNELS_PER_REQUEST = 256
_RATE_SAMPLE_CHANNELS = 200

_POLARS_TYPES = {ChannelDataType.INT: pl.Int64, ChannelDataType.DOUBLE: pl.Float64, ChannelDataType.STRING: pl.String}

_RAW_TABLES = {
    ChannelDataType.DOUBLE: "points_double",
    ChannelDataType.INT: "points_int",
    ChannelDataType.STRING: "points_string",
}

# Maximum number of buckets / decimated points exported per compute query.
# TODO(drake) raise 1000 limit once backend limit is raised
MAX_NUM_BUCKETS = 1000

# Channel data types that can be exported as dataframe columns.
_EXPORTABLE_DATA_TYPES: frozenset[ChannelDataType] = frozenset(
    [ChannelDataType.DOUBLE, ChannelDataType.INT, ChannelDataType.STRING]
)

DEFAULT_EXPORTED_TIMESTAMP_COL_NAME = "timestamp"

logger = logging.getLogger(__name__)


def _extract_bucket_counts(
    response: execution_results_pb2.ComputationResult,
) -> Sequence[tuple[IntegralNanosecondsUTC, int]]:
    """Extract (timestamp, point_count) pairs from a compute response.

    Works for both numeric and enum series. Handles bucketed (decimated) responses
    as well as undecimated fallbacks when the data has fewer points than requested buckets.
    """
    if response.HasField("numeric_buckets"):
        return [
            (_SecondsNanos.from_proto(bucket.end).to_nanoseconds(), bucket.statistics.count)
            for bucket in response.numeric_buckets.buckets
        ]
    if response.HasField("numeric_samples"):
        return [
            (_SecondsNanos.from_proto(point.timestamp).to_nanoseconds(), 1)
            for point in response.numeric_samples.samples
        ]
    if response.HasField("numeric_sample") and response.numeric_sample.HasField("sample"):
        return [(_SecondsNanos.from_proto(response.numeric_sample.sample.timestamp).to_nanoseconds(), 1)]
    if response.HasField("categorical_buckets"):
        return [
            (
                _SecondsNanos.from_proto(bucket.end).to_nanoseconds(),
                sum(bucket.statistics.count_by_category_index.values()),
            )
            for bucket in response.categorical_buckets.buckets
        ]
    if response.HasField("categorical_samples"):
        return [
            (_SecondsNanos.from_proto(point.timestamp).to_nanoseconds(), 1)
            for point in response.categorical_samples.samples
        ]
    logger.warning("Unrecognized compute response type: %s", response.WhichOneof("kind"))
    return []


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

    Note:
        Not intended for direct use — see `_channel_points_per_second`.

    Note:
        Do not use with more than 300 channels, or 500 concurrently across all requests.

    Args:
        client: Nominal request client
        channels: Channels to query data rates for (must be DOUBLE, INT, or STRING)
        start_ns: Start of the time range to query over
        end_ns: End of the time range to query over
        tags: Key-value pairs of tags to filter data with
        num_buckets: Number of buckets to use — more typically leads to better results.

            **Note:** Max number of buckets allowed is 1000.

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
    series_list = [channel._to_proto_compute_series(tags=tags) for channel in channels]

    api_start = _SecondsNanos.from_nanoseconds(start_ns).to_proto()
    api_end = _SecondsNanos.from_nanoseconds(end_ns).to_proto()

    try:
        summarization = execution_options_pb2.SeriesSummarization(
            bucket_count=execution_options_pb2.BucketCountSummary(bucket_count=num_buckets)
        )
        request = compute_pb2.BatchComputeWithUnitsRequest(
            inputs=[
                execution_inputs_pb2.ExecutionInput(
                    context=expressions_pb2.ComputeContext(),
                    start=api_start,
                    end=api_end,
                    computation=computation_spec_pb2.ComputationSpec(
                        series_summary=computation_spec_pb2.SeriesSummarySpec(input=series, summarization=summarization)
                    ),
                )
                for series in series_list
            ]
        )
        with translate_grpc_errors():
            resp = client._clients.compute.BatchComputeWithUnits(request)

    except Exception:
        logger.exception("Failed to compute buckets for channels: %s", [ch.name for ch in channels])
        return {(ch.data_source, ch.name): None for ch in channels}

    results: dict[tuple[str, str], float | None] = {}
    for channel, result in zip(channels, resp.results):
        key = (channel.data_source, channel.name)
        compute_result = result.outcome
        if not result.HasField("outcome") or compute_result.HasField("failure"):
            error_msg = compute_result.failure if compute_result.HasField("failure") else "no result"
            logger.warning("Failed to compute point rate for channel '%s': %s", channel.name, error_msg)
            results[key] = None
            continue

        if not compute_result.HasField("success"):
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

    Splits channels into batches of 200 channels and queries each batch in parallel
    via an internally-managed thread pool.

    Note:
        May take a long time for large channel counts. Takes approx. 30s for 1000 channels with good internet,
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
    # Up to 500 channel computations can be outstanding. Two 200-channel batches leave
    # headroom for other requests and avoid planning fan-out proportional to download workers.
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(num_workers, 2)) as pool:
        futures = {}
        for channel_batch in batched(channels, _RATE_SAMPLE_CHANNELS):
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

    def duration_ns(self) -> IntegralNanosecondsDuration:
        return self.end_time - self.start_time

    def subdivide_ns(self, duration: IntegralNanosecondsDuration) -> Sequence[Self]:
        if duration <= 0:
            raise ValueError("Batch duration must be positive")
        if self.start_time == self.end_time:
            return [self]
        # The service includes both endpoints. Only the final window includes the requested end.
        return [
            self.__class__(start, self.end_time if start + duration >= self.end_time else start + duration - 1)
            for start in range(self.start_time, self.end_time, duration)
        ]

    def duration(self) -> datetime.timedelta:
        return datetime.timedelta(seconds=(self.end_time - self.start_time) / 1e9)


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


@dataclasses.dataclass(frozen=True)
class _ExportJob:
    """One independently downloadable job within an output time window."""

    channels: Sequence[Channel]

    # Time bounds to export
    time_slice: _TimeRange

    # Key-value pairs to filter channels by
    tags: Mapping[str, str]

    # Decimation settings
    buckets: int | None = None
    resolution: IntegralNanosecondsDuration | None = None

    raw_table: str | None = None

    def fit_query_limit(self) -> Iterator[Self]:
        """Split only requests whose SQL exceeds the export API's 100,000 character limit."""
        if self.raw_table is None or len(self.sql_query()) <= 100_000:
            yield self
        elif len(self.channels) == 1:
            # A large tag predicate alone can exceed the SQL limit.
            yield dataclasses.replace(self, raw_table=None)
        else:
            midpoint = len(self.channels) // 2
            for group in (self.channels[:midpoint], self.channels[midpoint:]):
                yield from dataclasses.replace(self, channels=group).fit_query_limit()

    def sql_query(self) -> str:
        assert self.raw_table is not None
        return raw_export_query(
            self.raw_table, self.channels, self.time_slice.start_time, self.time_slice.end_time, self.tags
        )


def _format_time_col(df: pl.DataFrame, time_col: str, timestamp_type: _AnyExportableTimestampType) -> pl.DataFrame:
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


def _native_clock_frames(points: pl.DataFrame, names: Sequence[str], time_col: str) -> list[pl.DataFrame]:
    """Group raw columns with identical nanosecond timestamps within this job and window."""
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


def _export_job(job: _ExportJob, client: NominalClient, time_col: str, join_batches: bool) -> list[pl.DataFrame]:
    names = [ch.name for ch in job.channels]
    workspace = client._clients.resolve_default_workspace_rid()
    start, end = job.time_slice.start_time, job.time_slice.end_time
    schema = {ch.name: _POLARS_TYPES[ch.data_type] for ch in job.channels if ch.data_type is not None}
    if job.raw_table is not None:
        query = job.sql_query()
        request = jobs_api.CreateExportJobRequest(
            workspace_rid=workspace,
            sql=jobs_api.SqlExportSource(query=query),
            settings=jobs_api.ExportSettings(file_format=jobs_api.EXPORT_FILE_FORMAT_PARQUET),
        )
        with download_export(client, request) as file:
            points = pl.read_parquet(file)
        if points.is_empty():
            return []
        if not join_batches:
            return _native_clock_frames(points, names, time_col)
        # Pivot on the caller-safe timestamp name so even channels named "ts" and "timestamp" work.
        df = points.with_columns(pl.col("ts").dt.epoch("ns").alias(time_col)).pivot(
            on="channel", index=time_col, values="value"
        )
    else:
        # CSV reserves "timestamp" for the clock. Give a data channel of that name
        # a distinct wire header, then restore it (and any tag-expanded columns).
        timestamp_alias = _get_exported_timestamp_channel(names)
        request = compute_export_request(
            workspace, job.channels, start, end, job.tags, job.buckets, job.resolution, timestamp_alias
        )
        csv_schema = dict(schema)
        if "timestamp" in schema:
            csv_schema[timestamp_alias] = csv_schema.pop("timestamp")
        with download_export(client, request) as file:
            df = pl.read_csv(file, schema_overrides={**csv_schema, "timestamp": pl.Int64}, raise_if_empty=False)
        if df.is_empty():
            return []
        renames = {"timestamp": time_col}
        if "timestamp" in schema:
            renames.update(
                {
                    col: "timestamp" + col[len(timestamp_alias) :]
                    for col in df.columns
                    if col == timestamp_alias or (col.startswith(timestamp_alias + "_") and col not in schema)
                }
            )
        df = df.rename(renames)
    df = df.with_columns(pl.lit(None, dtype=schema[name]).alias(name) for name in names if name not in df.columns)
    known = {time_col, *names}
    return [df.select(time_col, *names, *(col for col in df.columns if col not in known)).sort(time_col)]


def _merge_dfs(dfs: Sequence[pl.DataFrame], time_col: str) -> pl.DataFrame:
    if len(dfs) == 1:
        return dfs[0]
    # Temporal subdivisions stack vertically. Independent channel groups align on timestamp.
    by_columns: dict[frozenset[str], list[pl.DataFrame]] = collections.defaultdict(list)
    for df in dfs:
        by_columns[frozenset(df.columns)].append(df)
    groups = iter(by_columns.values())
    first = next(groups)
    merged = pl.concat([df.lazy().select(first[0].columns) for df in first])
    for group in groups:
        other = pl.concat([df.lazy().select(group[0].columns) for df in group])
        merged = merged.join(other, on=time_col, how="full", coalesce=True)
    return merged.sort(time_col).collect()


class PolarsExportHandler:
    """Streams data out of Nominal into Polars DataFrames.

    Single-series raw channels use SQL jobs that write typed Parquet. Derived, external,
    multi-series, and decimated channels use compute jobs. The default joins each output
    window on integer nanosecond timestamps before conversion to the requested time format.
    """

    def __init__(
        self,
        client: NominalClient,
        points_per_request: int | None = None,
        points_per_dataframe: int = DEFAULT_POINTS_PER_DATAFRAME,
        channels_per_request: int = DEFAULT_CHANNELS_PER_REQUEST,
        num_workers: int = DEFAULT_NUM_WORKERS,
    ):
        """Configure export batching and concurrency.

        Args:
            client: Client whose selected workspace owns the export.
            points_per_request: Optional sample budget per job. Setting it enables rate estimation
                even when the export specifies a batch duration.
            points_per_dataframe: Target number of samples per yielded window when sizing it automatically.
            channels_per_request: Maximum number of channels in one job.
            num_workers: Maximum number of jobs processed concurrently.
        """
        if min(points_per_dataframe, channels_per_request, num_workers) < 1:
            raise ValueError("Frame size, channel count, and worker count must be positive")
        if points_per_request is not None and points_per_request < 1:
            raise ValueError("points_per_request must be positive")
        self._client = client
        self._points_per_request = points_per_request
        self._points_per_dataframe = points_per_dataframe
        self._channels_per_request = channels_per_request

        self._num_workers = num_workers

    def _compute_export_jobs(
        self,
        channels: Sequence[Channel],
        time_range: _TimeRange,
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

        matches = list(
            _channels_with_data_and_cardinality(
                channels,
                tags=tags,
                start_time=time_range.start_time,
                end_time=time_range.end_time,
                num_workers=min(self._num_workers, 4),
                batch_size=40,
            )
        )
        if not matches:
            return {}
        need_rates = time_range.duration_ns() > 0 and (batch_duration is None or self._points_per_request is not None)
        points_per_second: dict[tuple[str, str], float] = {}
        if need_rates:
            rates = _channel_points_per_second(
                self._client,
                [ch for ch, _ in matches],
                time_range.start_time,
                time_range.end_time,
                tags,
                num_workers=self._num_workers,
            )
            points_per_second = {key: rate for key, rate in rates.items() if rate}
            unknown = [(ch.data_source, ch.name) for ch, _ in matches if rates.get((ch.data_source, ch.name)) is None]
            if unknown:
                if not points_per_second or self._points_per_request is not None:
                    raise RuntimeError(
                        f"Could not size export jobs for {len(unknown)} channels. "
                        "Specify batch_duration and omit points_per_request to export without rate estimates."
                    )
                # Sizing is approximate. Keep confirmed channels whose estimate failed, using
                # the highest observed rate instead of silently omitting part of the export.
                peak = max(points_per_second.values())
                points_per_second.update(dict.fromkeys(unknown, peak))

        raw_datasets: set[str] = set()
        if buckets is None and resolution is None:
            dataset_rids = {ch.data_source for ch, single in matches if single}
            clients = self._client._clients
            raw_datasets = {
                ds.rid
                for ds in _get_datasets(clients.auth_header, clients.catalog, dataset_rids)
                if ds.derived_definition is None
                and ds.external_connection_metadata is None
                and ds.default_group_by_and_aggregation_config is None
            }

        batch_duration_ns = _compute_batch_duration(
            batch_duration, time_range, points_per_second, self._points_per_dataframe
        )
        time_slices = time_range.subdivide_ns(max(1, batch_duration_ns))
        batch_timedelta = datetime.timedelta(seconds=batch_duration_ns / 1e9)

        channels_by_datasource: dict[tuple[str, str | None], list[Channel]] = collections.defaultdict(list)
        for channel, single_series in matches:
            if need_rates and (channel.data_source, channel.name) not in points_per_second:
                continue
            table = (
                _RAW_TABLES.get(channel.data_type)
                if channel.data_type and single_series and channel.data_source in raw_datasets
                else None
            )
            channels_by_datasource[channel.data_source, table].append(channel)

        jobs: dict[_TimeRange, list[_ExportJob]] = collections.defaultdict(list)
        for (_, raw_table), ds_channels in channels_by_datasource.items():
            for group, step_ns in self._channel_groups(ds_channels, points_per_second, batch_timedelta):
                for window in time_slices:
                    subdivisions = window.subdivide_ns(step_ns) if step_ns is not None else (window,)
                    for bounds in subdivisions:
                        job = _ExportJob(
                            channels=group,
                            raw_table=raw_table,
                            time_slice=bounds,
                            tags=tags or {},
                            buckets=buckets,
                            resolution=resolution,
                        )
                        jobs[window].extend(job.fit_query_limit())
        return jobs

    def _channel_groups(
        self,
        channels: Sequence[Channel],
        points_per_second: Mapping[tuple[str, str], float],
        batch_duration: datetime.timedelta,
    ) -> Iterator[tuple[Sequence[Channel], int | None]]:
        """Yield channel groups and any shorter window required by an explicit per-job budget."""
        if self._points_per_request is None or not points_per_second:
            for group in batched(channels, self._channels_per_request):
                yield group, None
            return
        channels_by_key = {(ch.data_source, ch.name): ch for ch in channels}
        rates = {key: points_per_second[key] for key in channels_by_key}
        groups, large_channels = _build_channel_groups(
            rates, channels_by_key, self._points_per_request, self._channels_per_request, batch_duration
        )
        for grouped_channels in groups:
            yield grouped_channels, None
        for channel in large_channels:
            rate = rates[channel.data_source, channel.name]
            yield [channel], max(1, int(1e9 * self._points_per_request / rate))

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

        Only DOUBLE, INT, and STRING channels flow through the export pipeline; channels of any
        other data type are filtered out with a warning. Each batch requires export jobs and
        object storage in the client’s selected workspace. Job and download failures propagate
        during iteration, including after earlier batches have been yielded.

        Args:
            channels: Channels to export.
            start: Start of the export time range (nanoseconds UTC).
            end: End of the export time range (nanoseconds UTC).
            tags: Exact tags to match. Export sizing uses the :ref:`compute-filter-limits`.
            batch_duration: Explicit per-batch time window. If omitted, one is computed
                from total channel rate and `points_per_dataframe`.
            timestamp_type: Output timestamp representation (`epoch_seconds`, `iso8601`, etc.).
            buckets: Decimate each channel to at most this many buckets per request. Mutually
                exclusive with `resolution`.
            resolution: Decimate each channel to samples at this interval (nanoseconds).
                Mutually exclusive with `buckets`.
            join_batches: If True (default), each yielded DataFrame is the outer-joined merge of
                all channel groups for a single time slice. If False, raw SQL jobs yield frames
                of channels with identical sample timestamps within that job and time slice.
                Only sampled channels appear. Compute jobs yield their original channel groups.

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

        if batch_duration is not None and batch_duration <= datetime.timedelta(0):
            raise ValueError("batch_duration must be positive")
        if end < start:
            raise ValueError("end must be at or after start")
        if (resolution is not None and resolution < 1) or (buckets is not None and buckets < 1):
            raise ValueError("Decimation resolution and bucket count must be positive")

        # Determine download schedule
        export_jobs = self._compute_export_jobs(
            supported_channels, _TimeRange(start, end), tags or {}, buckets, resolution, batch_duration
        )
        time_column = _get_exported_timestamp_channel([ch.name for ch in supported_channels])
        yield from self._export_dataframes(export_jobs, time_column, timestamp_type, join_batches)

    def _export_dataframes(
        self,
        export_jobs: Mapping[_TimeRange, Sequence[_ExportJob]],
        time_column: str,
        timestamp_type: _AnyExportableTimestampType,
        join_batches: bool,
    ) -> Iterator[pl.DataFrame]:
        # One window of lookahead overlaps server execution and download with the current
        # frame's assembly and consumption. Results are still yielded in time-window order.
        windows = iter(sorted(export_jobs))
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=self._num_workers)
        try:
            pending: collections.deque[list[concurrent.futures.Future[list[pl.DataFrame]]]] = collections.deque()
            while True:
                while len(pending) < 2 and (window := next(windows, None)) is not None:
                    pending.append(
                        [
                            pool.submit(_export_job, job, self._client, time_column, join_batches)
                            for job in export_jobs[window]
                        ]
                    )
                if not pending:
                    return
                results: list[pl.DataFrame] = []
                futures = pending.popleft()
                for future in futures if join_batches else concurrent.futures.as_completed(futures):
                    if join_batches:
                        results.extend(future.result())
                    else:
                        for frame in future.result():
                            yield _format_time_col(frame, time_column, timestamp_type)
                if results:
                    yield _format_time_col(_merge_dfs(results, time_column), time_column, timestamp_type)
        finally:
            pool.shutdown(cancel_futures=True)
