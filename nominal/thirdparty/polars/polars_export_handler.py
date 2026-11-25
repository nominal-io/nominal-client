import collections
import concurrent.futures
import dataclasses
import datetime
import functools
import logging
from typing import Iterable, Iterator, Mapping, Self, Sequence

import polars as pl
from nominal._utils.iterator_tools import batched
from nominal.core._utils.api_tools import build_compute_tag_filter
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core.channel import (
    Channel,
    ChannelDataType,
    _batch_check_channels_have_data,
    _channels_with_data_and_cardinality,
)
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
_SERIES_COUNT_BATCH_SIZE = 40

_POLARS_TYPES = {ChannelDataType.INT: pl.Int64, ChannelDataType.DOUBLE: pl.Float64, ChannelDataType.STRING: pl.String}

_RAW_TABLES = {
    ChannelDataType.DOUBLE: "points_double",
    ChannelDataType.INT: "points_int",
    ChannelDataType.STRING: "points_string",
}

# Maximum number of buckets / decimated points exported per compute query.
# TODO(drake) raise 1000 limit once backend limit is raised
MAX_NUM_BUCKETS = 1000

DEFAULT_EXPORTED_TIMESTAMP_COL_NAME = "timestamp"

logger = logging.getLogger(__name__)


def _extract_bucket_counts(
    response: execution_results_pb2.ComputationResult,
) -> Sequence[tuple[IntegralNanosecondsUTC, int]] | None:
    """Extract (timestamp, point_count) pairs from a compute response.

    Works for both numeric and enum series. Handles bucketed (decimated) responses
    as well as undecimated fallbacks when the data has fewer points than requested buckets.
    """
    if response.HasField("numeric_buckets"):
        return [
            (_SecondsNanos.from_proto(bucket.end).to_nanoseconds(), bucket.statistics.count)
            for bucket in response.numeric_buckets.buckets
        ]
    if response.HasField("categorical_buckets"):
        return [
            (
                _SecondsNanos.from_proto(bucket.end).to_nanoseconds(),
                sum(bucket.statistics.count_by_category_index.values()),
            )
            for bucket in response.categorical_buckets.buckets
        ]
    if response.HasField("numeric_samples"):
        timestamps = [point.timestamp for point in response.numeric_samples.samples]
    elif response.HasField("categorical_samples"):
        timestamps = [point.timestamp for point in response.categorical_samples.samples]
    elif response.HasField("numeric_sample") or response.HasField("categorical_sample"):
        single = response.numeric_sample if response.HasField("numeric_sample") else response.categorical_sample
        timestamps = [single.sample.timestamp] if single.HasField("sample") else []
    else:
        logger.warning("Unrecognized compute response type: %s", response.WhichOneof("kind"))
        return None
    return [(_SecondsNanos.from_proto(ts).to_nanoseconds(), 1) for ts in timestamps]


def _peak_points_per_second(
    bucket_counts: Sequence[tuple[IntegralNanosecondsUTC, int]],
    start_ns: IntegralNanosecondsUTC,
    end_ns: IntegralNanosecondsUTC,
) -> float:
    """Estimate peak rate from bucket ends, using the range average for coincident samples."""
    if not bucket_counts or end_ns <= start_ns:
        return 0.0
    peak = sum(count for _, count in bucket_counts) * 1e9 / (end_ns - start_ns)
    if len(bucket_counts) > 1:
        previous = start_ns
        for timestamp, count in bucket_counts:
            if timestamp > previous:
                peak = max(peak, count * 1e9 / (timestamp - previous))
            previous = timestamp
    return peak


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
        Not intended for direct use — see `_estimate_export_rates`.

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
        results[key] = None if bucket_counts is None else _peak_points_per_second(bucket_counts, start_ns, end_ns)

    return results


def _estimate_export_rates(
    client: NominalClient,
    channels: Sequence[Channel],
    start_ns: int,
    end_ns: int,
    tags: Mapping[str, str] | None,
    num_workers: int,
    require_all: bool,
) -> dict[tuple[str, str], float]:
    """Use the highest observed rate for unknown channels when sizing a frame approximately."""
    # The service allows 500 channel computations in flight. Two batches of 200 leave headroom.
    sample = functools.partial(
        _batch_channel_points_per_second,
        client,
        start_ns=start_ns,
        end_ns=end_ns,
        tags=dict(tags or {}),
        num_buckets=100,
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(num_workers, 2)) as pool:
        rates = {
            key: rate
            for batch in pool.map(sample, batched(channels, _RATE_SAMPLE_CHANNELS))
            for key, rate in batch.items()
        }

    positive = {key: rate for key, rate in rates.items() if rate}
    unknown = [(ch.data_source, ch.name) for ch in channels if rates.get((ch.data_source, ch.name)) is None]
    if unknown:
        if require_all or not positive:
            raise RuntimeError(
                f"Could not size export jobs for {len(unknown)} channels. "
                "Specify batch_duration and omit points_per_request to export without rate estimates."
            )
        positive.update(dict.fromkeys(unknown, max(positive.values())))
    return positive


@dataclasses.dataclass(frozen=True)
class _TimeRange:
    start_time: IntegralNanosecondsUTC
    end_time: IntegralNanosecondsUTC

    def duration_ns(self) -> IntegralNanosecondsDuration:
        return self.end_time - self.start_time

    def batch_duration_ns(self, explicit: datetime.timedelta | None, rate: float, target_points: int) -> int:
        if explicit is not None:
            return (explicit // datetime.timedelta(microseconds=1)) * 1000
        span = max(1, self.duration_ns())
        return min(span, max(1, int(target_points * 1e9 / rate))) if rate else span

    def subdivide_ns(self, duration: IntegralNanosecondsDuration) -> Iterator[Self]:
        # Both bounds are inclusive. The final window includes the requested end.
        for start in range(self.start_time, max(self.start_time + 1, self.end_time), duration):
            end = self.end_time if start + duration >= self.end_time else start + duration - 1
            yield self.__class__(start, end)


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
    ambiguous_channels: frozenset[str] = frozenset()

    def resolve_sources(self) -> Iterator[Self]:
        """Include ambiguous channels in a raw job when this window selects exactly one series."""
        if not self.raw_table or not self.ambiguous_channels:
            yield self
            return

        candidates = [ch for ch in self.channels if ch.name in self.ambiguous_channels]
        compute_names = {ch.name for ch in candidates}
        start = _SecondsNanos.from_nanoseconds(self.time_slice.start_time).to_api()
        end = _SecondsNanos.from_nanoseconds(self.time_slice.end_time).to_api()
        filters = build_compute_tag_filter(self.tags)
        for group in batched(candidates, _SERIES_COUNT_BATCH_SIZE):
            try:
                matches = _batch_check_channels_have_data(group[0]._clients, group, filters, start, end)
            except Exception:
                logger.debug("Using compute after a window series check failed", exc_info=True)
                continue
            compute_names.difference_update(ch.name for ch, single in matches if single)
        if not compute_names:
            yield self
            return
        raw = [ch for ch in self.channels if ch.name not in compute_names]
        if raw:
            yield dataclasses.replace(self, channels=raw, ambiguous_channels=frozenset())
        yield dataclasses.replace(
            self,
            channels=[ch for ch in candidates if ch.name in compute_names],
            raw_table=None,
            ambiguous_channels=frozenset(),
        )

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
    name = DEFAULT_EXPORTED_TIMESTAMP_COL_NAME
    index = 0
    while name in channel_names:
        index += 1
        name = f"{DEFAULT_EXPORTED_TIMESTAMP_COL_NAME}.{index}"
    return name


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
    workspace = client._clients.resolve_default_workspace_rid()
    frames = []
    for batch in job.resolve_sources():
        frames.extend(_download_frames(batch, client, workspace, time_col, join_batches))
    return frames


def _download_frames(
    job: _ExportJob, client: NominalClient, workspace: str, time_col: str, join_batches: bool
) -> list[pl.DataFrame]:
    names = [ch.name for ch in job.channels]
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
            return [pl.DataFrame(schema={time_col: pl.Int64, **schema})] if join_batches else []
        if not join_batches:
            return _native_clock_frames(points, names, time_col)
        # Pivot on the caller-safe timestamp name so even channels named "ts" and "timestamp" work.
        df = points.with_columns(pl.col("ts").dt.epoch("ns").alias(time_col)).pivot(
            on="channel", index=time_col, values="value"
        )
    else:
        # Compute appends "_tag_value" to grouped columns. Private column IDs let us
        # recover the declared type even if channel names themselves contain underscores.
        wire_channels = {str(i): ch for i, ch in enumerate(job.channels)}
        request = compute_export_request(workspace, wire_channels, start, end, job.tags, job.buckets, job.resolution)
        with download_export(client, request) as file:
            csv = pl.read_csv(
                file, infer_schema_length=0, schema_overrides={"timestamp": pl.Int64}, raise_if_empty=False
            )
        if csv.is_empty():
            return [pl.DataFrame(schema={time_col: pl.Int64, **schema})] if join_batches else []
        columns = [pl.col("timestamp").alias(time_col)]
        for column in csv.columns:
            if column == "timestamp":
                continue
            wire_name, separator, tags = column.partition("_")
            name = wire_channels[wire_name].name
            output_name = name + separator + tags
            if separator and output_name in schema:
                raise ValueError(f"Tag-expanded column {output_name!r} matches a requested channel name")
            columns.append(pl.col(column).cast(schema[name]).alias(output_name))
        df = csv.select(columns)
    df = df.with_columns(pl.lit(None, dtype=schema[name]).alias(name) for name in names if name not in df.columns)
    known = {time_col, *names}
    return [df.select(time_col, *names, *(col for col in df.columns if col not in known)).sort(time_col)]


def _join_window(results: Sequence[tuple[_ExportJob, list[pl.DataFrame]]], time_col: str) -> pl.DataFrame:
    # A single job can return separate SQL and compute frames. Only single-channel
    # jobs can be subdivided in time, and tag-expanded columns can vary by subdivision.
    by_group: dict[tuple[str, ...], list[list[pl.DataFrame]]] = collections.defaultdict(list)
    for job, frames in results:
        by_group[tuple(ch.name for ch in job.channels)].append(frames)
    groups: list[pl.DataFrame] = []
    for slices in by_group.values():
        if len(slices) == 1:
            groups.extend(slices[0])
        else:
            groups.append(pl.concat([frame for batch in slices for frame in batch], how="diagonal"))
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


class PolarsExportHandler:
    """Streams data out of Nominal into Polars DataFrames.

    Raw channels can share a typed Parquet SQL job when each matches one series within
    its time window. Derived, external, decimated, and unresolved multi-series channels
    use compute jobs. The default joins each output window on integer nanosecond timestamps
    before conversion to the requested time format.
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
    ) -> Iterator[Iterator[_ExportJob]]:
        """Plan channel groups once and yield jobs for successive output windows.

        `channels` is expected to be already filtered to exportable data types by the caller.
        """
        matches = list(
            _channels_with_data_and_cardinality(
                channels,
                tags=tags,
                start_time=time_range.start_time,
                end_time=time_range.end_time,
                num_workers=min(self._num_workers, 4),
                batch_size=_SERIES_COUNT_BATCH_SIZE,
            )
        )
        if not matches:
            return
        need_rates = time_range.duration_ns() > 0 and (batch_duration is None or self._points_per_request is not None)
        points_per_second: dict[tuple[str, str], float] = {}
        if need_rates:
            points_per_second = _estimate_export_rates(
                self._client,
                [ch for ch, _ in matches],
                time_range.start_time,
                time_range.end_time,
                tags,
                num_workers=self._num_workers,
                require_all=self._points_per_request is not None,
            )

        duration_ns = time_range.batch_duration_ns(
            batch_duration, sum(points_per_second.values()), self._points_per_dataframe
        )
        multiple_windows = time_range.duration_ns() > duration_ns
        # A channel with several matching series may still select one in a shorter window.
        sql_candidates = (
            {
                (ch.data_source, ch.name): single
                for ch, single in matches
                if single is not None and (single or multiple_windows)
            }
            if buckets is None and resolution is None
            else {}
        )
        clients = self._client._clients
        raw_datasets = {
            ds.rid
            for ds in _get_datasets(clients.auth_header, clients.catalog, {rid for rid, _ in sql_candidates})
            if ds.derived_definition is None
            and ds.external_connection_metadata is None
            and ds.default_group_by_and_aggregation_config is None
        }

        channels_by_datasource: dict[tuple[str, str | None], list[Channel]] = collections.defaultdict(list)
        for channel, _ in matches:
            key = (channel.data_source, channel.name)
            if need_rates and key not in points_per_second:
                continue
            table = None
            if key in sql_candidates and channel.data_source in raw_datasets and channel.data_type is not None:
                table = _RAW_TABLES[channel.data_type]
            channels_by_datasource[channel.data_source, table].append(channel)

        groups = []
        for (_, table), channels_in_source in channels_by_datasource.items():
            for group, step_ns in self._channel_groups(channels_in_source, points_per_second, duration_ns):
                to_check = frozenset(ch.name for ch in group if sql_candidates.get((ch.data_source, ch.name)) is False)
                template = _ExportJob(
                    channels=group,
                    time_slice=time_range,
                    tags=tags or {},
                    buckets=buckets,
                    resolution=resolution,
                    raw_table=table,
                    ambiguous_channels=to_check,
                )
                groups.append((template, step_ns))
        if not groups:
            return

        def jobs_for_window(window: _TimeRange) -> Iterator[_ExportJob]:
            for template, step in groups:
                subdivisions = window.subdivide_ns(step) if step is not None else (window,)
                for bounds in subdivisions:
                    yield from dataclasses.replace(template, time_slice=bounds).fit_query_limit()

        for window in time_range.subdivide_ns(duration_ns):
            yield jobs_for_window(window)

    def _channel_groups(
        self,
        channels: Sequence[Channel],
        rates: Mapping[tuple[str, str], float],
        duration_ns: int,
    ) -> Iterator[tuple[Sequence[Channel], int | None]]:
        """Pack channels by estimated rate, splitting oversized channels into shorter jobs."""
        if self._points_per_request is None or not rates:
            for batch in batched(channels, self._channels_per_request):
                yield batch, None
            return

        allowed_rate = self._points_per_request * 1e9 / duration_ns
        group: list[Channel] = []
        group_rate = 0.0
        for channel in sorted(channels, key=lambda ch: rates[ch.data_source, ch.name], reverse=True):
            rate = rates[channel.data_source, channel.name]
            if rate > allowed_rate:
                yield [channel], max(1, int(self._points_per_request * 1e9 / rate))
                continue
            if group and (group_rate + rate > allowed_rate or len(group) == self._channels_per_request):
                yield group, None
                group = []
                group_rate = 0.0
            group.append(channel)
            group_rate += rate
        if group:
            yield group, None

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
            channels: Channels to export. Frames carry channel names without a source dataset RID.
                Keep each input channel’s ``data_source`` to identify its dataset.
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
                Within a window, unjoined jobs can yield frames in any order.

        Yields:
            Polars DataFrames covering successive time slices of the export range.

        Raises:
            ValueError: On first iteration if ``join_batches=True`` and two exportable input channels
                have the same name, even across datasets. This check runs before any export job starts.
        """
        # Ensure user has selected channels to export
        if not channels:
            logger.warning("No channels requested for export-- returning")
            return

        # Ensure user has not selected incompatible decimation options
        if None not in (buckets, resolution):
            raise ValueError("Cannot export data decimated with both buckets and resolution")

        # Exclude channels with unsupported data types
        supported_channels = [ch for ch in channels if ch.data_type in _POLARS_TYPES]
        unsupported = len(channels) - len(supported_channels)
        if unsupported:
            logger.warning("Could not determine datatypes of %d channels -- ignoring for export", unsupported)

        if batch_duration is not None and batch_duration <= datetime.timedelta(0):
            raise ValueError("batch_duration must be positive")
        if end < start:
            raise ValueError("end must be at or after start")
        if (resolution is not None and resolution < 1) or (buckets is not None and buckets < 1):
            raise ValueError("Decimation resolution and bucket count must be positive")

        names = [channel.name for channel in supported_channels]
        if join_batches:
            duplicates = [name for name, count in collections.Counter(names).items() if count > 1]
            if duplicates:
                raise ValueError(f"Duplicate channel names {duplicates!r} in joined export. Use join_batches=False.")

        # Determine download schedule
        export_jobs = self._compute_export_jobs(
            supported_channels, _TimeRange(start, end), tags or {}, buckets, resolution, batch_duration
        )
        time_column = _get_exported_timestamp_channel(names)
        yield from self._export_dataframes(export_jobs, time_column, timestamp_type, join_batches)

    def _export_dataframes(
        self,
        export_jobs: Iterator[Iterable[_ExportJob]],
        time_column: str,
        timestamp_type: _AnyExportableTimestampType,
        join_batches: bool,
    ) -> Iterator[pl.DataFrame]:
        jobs = ((window, job) for window, group in enumerate(export_jobs) for job in group)
        upcoming = next(jobs, None)
        pending: dict[concurrent.futures.Future[list[pl.DataFrame]], tuple[int, _ExportJob]] = {}
        results: list[tuple[_ExportJob, list[pl.DataFrame]]] = []
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=self._num_workers)
        try:
            while upcoming is not None or pending:
                while upcoming is not None and len(pending) < self._num_workers:
                    window, job = upcoming
                    # Joined frames can be wide. Prefetch only the next output window.
                    if join_batches and pending and window > next(iter(pending.values()))[0] + 1:
                        break
                    future = pool.submit(_export_job, job, self._client, time_column, join_batches)
                    pending[future] = upcoming
                    upcoming = next(jobs, None)

                first = next(iter(pending))
                window = pending[first][0]
                if join_batches:
                    # Submission order keeps a channel's temporal subdivisions sorted.
                    _, job = pending.pop(first)
                    results.append((job, first.result()))
                    if (not pending or next(iter(pending.values()))[0] != window) and (
                        upcoming is None or upcoming[0] != window
                    ):
                        joined = _join_window(results, time_column)
                        results.clear()
                        if not joined.is_empty():
                            yield _format_time_col(joined, time_column, timestamp_type)
                else:
                    active = (future for future, (index, _) in pending.items() if index == window)
                    done = next(concurrent.futures.as_completed(active))
                    del pending[done]
                    for frame in done.result():
                        yield _format_time_col(frame, time_column, timestamp_type)
        finally:
            pool.shutdown(cancel_futures=True)
