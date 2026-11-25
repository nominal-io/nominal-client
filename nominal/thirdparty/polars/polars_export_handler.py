import collections
import concurrent.futures
import dataclasses
import datetime
import functools
import logging
import time
from typing import Iterable, Iterator, Mapping, Self, Sequence

import polars as pl
from nominal._utils.iterator_tools import batched
from nominal.core._utils.api_tools import build_compute_tag_filter
from nominal.core.bounds import Bounds
from nominal.core.channel import (
    Channel,
    _batch_series_counts,
    _channels_with_series_counts,
)
from nominal.core.client import NominalClient
from nominal.protos.exportjobs.v1 import export_jobs_pb2 as jobs_api
from nominal.thirdparty.polars._export_jobs import (
    MAX_SQL_QUERY_LENGTH,
    RawSource,
    compute_export_request,
    download_export,
    raw_export_query,
    raw_sources,
)
from nominal.thirdparty.polars._frames import (
    POLARS_TYPES,
    format_time_col,
    join_window,
    read_compute_frames,
    read_raw_frames,
)
from nominal.thirdparty.polars._point_counts import SAMPLE_BATCH_SIZE, estimate_export_rates, sample_counts
from nominal.ts import (
    IntegralNanosecondsDuration,
    IntegralNanosecondsUTC,
    _AnyExportableTimestampType,
    _SecondsNanos,
)

DEFAULT_NUM_WORKERS = 8
DEFAULT_POINTS_PER_DATAFRAME = 25_000_000
DEFAULT_POINTS_PER_REQUEST = 1_000_000  # Previous per-request target, available for callers that opt into it.
DEFAULT_CHANNELS_PER_REQUEST = 256
_NATIVE_WINDOW_NS = 120_000_000_000
_SMALL_NUMERIC_EXPORT_POINTS = 1_000_000
_SERIES_COUNT_BATCH_SIZE = 40

MAX_NUM_BUCKETS = 1000

DEFAULT_EXPORTED_TIMESTAMP_COL_NAME = "timestamp"

logger = logging.getLogger(__name__)


def _windows(bounds: Bounds, step: int) -> Iterator[Bounds]:
    """Split an inclusive interval without duplicating a sample at the next window's start."""
    for start in range(bounds.start, max(bounds.start + 1, bounds.end), step):
        end = bounds.end if start + step >= bounds.end else start + step - 1
        yield Bounds(start, end)


@dataclasses.dataclass(frozen=True)
class _ExportJob:
    """One independently downloadable job within an output time window."""

    channels: Sequence[Channel]
    bounds: Bounds
    tags: Mapping[str, str]
    buckets: int | None = None
    resolution: IntegralNanosecondsDuration | None = None

    raw_kind: RawSource | None = None
    ambiguous_channels: frozenset[str] = frozenset()

    def resolve_sources(self) -> Iterator[Self]:
        """Use raw points when this window matches at most one series for the channel."""
        if not self.raw_kind or not self.ambiguous_channels:
            yield self
            return

        candidates = [ch for ch in self.channels if ch.name in self.ambiguous_channels]
        compute_names: set[str] = set()
        start = _SecondsNanos.from_nanoseconds(self.bounds.start).to_api()
        end = _SecondsNanos.from_nanoseconds(self.bounds.end).to_api()
        filters = build_compute_tag_filter(self.tags)
        for group in batched(candidates, _SERIES_COUNT_BATCH_SIZE):
            try:
                matches = _batch_series_counts(group[0]._clients, group, filters, start, end)
            except Exception:
                logger.debug("Using compute after a window series check failed", exc_info=True)
                compute_names.update(ch.name for ch in group)
                continue
            # Confirmed zeros are omitted from matches. They can share the raw job with single-series channels.
            compute_names.update(ch.name for ch, count in matches if count != 1)
        if not compute_names:
            yield self
            return
        raw = [ch for ch in self.channels if ch.name not in compute_names]
        if raw:
            yield dataclasses.replace(self, channels=raw)
        yield dataclasses.replace(
            self,
            channels=[ch for ch in candidates if ch.name in compute_names],
            raw_kind=None,
        )

    def fit_query_limit(self) -> Iterator[Self]:
        """Split only requests whose SQL exceeds the export API's 100,000 character limit."""
        if self.raw_kind is None or len(self.sql_query) <= MAX_SQL_QUERY_LENGTH:
            yield self
        elif len(self.channels) == 1:
            # A large tag predicate alone can exceed the SQL limit.
            yield dataclasses.replace(self, raw_kind=None)
        else:
            midpoint = len(self.channels) // 2
            for group in (self.channels[:midpoint], self.channels[midpoint:]):
                yield from dataclasses.replace(self, channels=group).fit_query_limit()

    @functools.cached_property
    def sql_query(self) -> str:
        assert self.raw_kind is not None
        return raw_export_query(self.raw_kind, self.channels, self.bounds, self.tags)


def _get_exported_timestamp_channel(channel_names: list[str]) -> str:
    name = DEFAULT_EXPORTED_TIMESTAMP_COL_NAME
    index = 0
    while name in channel_names:
        index += 1
        name = f"{DEFAULT_EXPORTED_TIMESTAMP_COL_NAME}.{index}"
    return name


def _export_job(job: _ExportJob, client: NominalClient, time_col: str, join_batches: bool) -> list[pl.DataFrame]:
    workspace = client._clients.resolve_default_workspace_rid()
    frames = []
    for batch in job.resolve_sources():
        if batch.raw_kind is not None:
            request = jobs_api.CreateExportJobRequest(
                workspace_rid=workspace,
                sql=jobs_api.SqlExportSource(query=batch.sql_query),
                settings=jobs_api.ExportSettings(file_format=jobs_api.EXPORT_FILE_FORMAT_PARQUET),
            )
            with download_export(client, request) as file:
                frames.extend(
                    read_raw_frames(file, batch.channels, time_col, join_batches, batch.raw_kind == "mixed_numeric")
                )
        else:
            wire_channels = {str(i): ch for i, ch in enumerate(batch.channels)}
            request = compute_export_request(
                workspace,
                wire_channels,
                batch.bounds,
                batch.tags,
                batch.buckets,
                batch.resolution,
            )
            with download_export(client, request) as file:
                frames.extend(read_compute_frames(file, wire_channels, time_col, join_batches))
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "Export window [%d, %d], %d channels returned %d frames, %d bytes before joining",
            job.bounds.start,
            job.bounds.end,
            len(job.channels),
            len(frames),
            sum(frame.estimated_size() for frame in frames),
        )
    return frames


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
        points_per_dataframe: int | None = None,
        channels_per_request: int = DEFAULT_CHANNELS_PER_REQUEST,
        num_workers: int = DEFAULT_NUM_WORKERS,
    ):
        """Configure export batching and concurrency.

        Args:
            client: Client whose selected workspace owns the export.
            points_per_request: Optional sample budget per job. Setting it enables rate estimation
                even when the export specifies a batch duration.
            points_per_dataframe: Optional target sample count per window. Setting it requests
                rate-based sizing when ``batch_duration`` is omitted.
            channels_per_request: Maximum number of channels in one job.
            num_workers: Maximum number of jobs processed concurrently.
        """
        if min(channels_per_request, num_workers) < 1 or (
            points_per_dataframe is not None and points_per_dataframe < 1
        ):
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
        time_range: Bounds,
        tags: Mapping[str, str] | None = None,
        buckets: int | None = None,
        resolution: IntegralNanosecondsDuration | None = None,
        batch_duration: datetime.timedelta | None = None,
        *,
        join_batches: bool = True,
    ) -> Iterator[Iterator[_ExportJob]]:
        """Plan channel groups once and yield jobs for successive output windows.

        `channels` is expected to be already filtered to exportable data types by the caller.
        """
        started = time.monotonic()
        matches = list(
            _channels_with_series_counts(
                channels,
                tags=tags,
                start_time=time_range.start,
                end_time=time_range.end,
                num_workers=min(self._num_workers, 4),
                batch_size=_SERIES_COUNT_BATCH_SIZE,
            )
        )
        if not matches:
            return
        sources = (
            raw_sources(self._client, [ch for ch, count in matches if count is not None], tags or {})
            if buckets is None and resolution is None
            else {}
        )
        multi_series = {(ch.data_source, ch.name) for ch, count in matches if count is not None and count > 1}
        channels = [ch for ch, _ in matches]
        duration_ns, rates = self._window_size(
            channels,
            time_range,
            tags,
            batch_duration,
            sources,
            native=not join_batches,
            multi_series=bool(multi_series),
        )
        if rates is not None:
            channels = [ch for ch in channels if (ch.data_source, ch.name) in rates]
        if not channels:
            return
        multiple_windows = time_range.duration > duration_ns
        channels_by_datasource: dict[tuple[str, RawSource | None, str | None], list[Channel]] = collections.defaultdict(
            list
        )
        for channel in channels:
            key = (channel.data_source, channel.name)
            kind = sources.get(key) if multiple_windows or key not in multi_series else None
            # A channel with historical type changes needs its own lossless dtype decision per window.
            separate = channel.name if kind == "mixed_numeric" else None
            channels_by_datasource[channel.data_source, kind, separate].append(channel)

        groups = []
        for (_, kind, _), channels_in_source in channels_by_datasource.items():
            for group, step_ns in self._channel_groups(channels_in_source, rates or {}, duration_ns):
                to_check = frozenset(ch.name for ch in group if (ch.data_source, ch.name) in multi_series)
                template = _ExportJob(
                    channels=group,
                    bounds=time_range,
                    tags=tags or {},
                    buckets=buckets,
                    resolution=resolution,
                    raw_kind=kind,
                    ambiguous_channels=to_check,
                )
                groups.append((template, step_ns))
        logger.debug(
            "Export plan: %d channels, window %.3fs, %d channel groups, %d workers, planning %.3fs",
            len(channels),
            duration_ns / 1e9,
            len(groups),
            self._num_workers,
            time.monotonic() - started,
        )

        def jobs_for_window(window: Bounds) -> Iterator[_ExportJob]:
            for template, step in groups:
                subdivisions = _windows(window, step) if step is not None else (window,)
                for bounds in subdivisions:
                    yield from dataclasses.replace(template, bounds=bounds).fit_query_limit()

        for window in _windows(time_range, duration_ns):
            yield jobs_for_window(window)

    def _window_size(
        self,
        channels: Sequence[Channel],
        bounds: Bounds,
        tags: Mapping[str, str] | None,
        duration: datetime.timedelta | None,
        sources: Mapping[tuple[str, str], RawSource],
        *,
        native: bool,
        multi_series: bool,
    ) -> tuple[int, dict[tuple[str, str], float] | None]:
        """Return a window duration and, only when sampled for a point budget, the channel rates."""
        span = bounds.duration
        fixed = duration // datetime.timedelta(microseconds=1) * 1000 if duration is not None else None
        if fixed is not None and self._points_per_request is None:
            return fixed, None
        if span == 0:
            return fixed or 1, None

        if (
            native
            and fixed is None
            and self._points_per_request is None
            and self._points_per_dataframe is None
            and all((ch.data_source, ch.name) in sources for ch in channels)
        ):
            # One cheap count can replace many waves of small fixed-width numeric jobs.
            if (
                span > 2 * self._num_workers * _NATIVE_WINDOW_NS
                and len(channels) <= SAMPLE_BATCH_SIZE
                and not multi_series
                and all(source in {"points_int", "points_double"} for source in sources.values())
            ):
                counts = sample_counts(self._client, channels, bounds, tags, bucket_count=1)
                if (
                    len(counts) == len(channels)
                    and sum(n for buckets in counts.values() for _, n in buckets) <= _SMALL_NUMERIC_EXPORT_POINTS
                ):
                    return span, None
            return _NATIVE_WINDOW_NS, None

        rates = estimate_export_rates(
            self._client,
            channels,
            bounds,
            tags,
            self._num_workers,
            require_all=self._points_per_request is not None,
        )
        if fixed is not None:
            return fixed, rates
        rate = sum(rates.values())
        target = self._points_per_dataframe or DEFAULT_POINTS_PER_DATAFRAME
        step = max(1, int(target * 1e9 / rate)) if rate else span
        return min(span, step), rates

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
            batch_duration: Time covered by each output window. With no explicit size options,
                unjoined full-resolution exports use two-minute windows when all channels have
                known series counts in stored datasets without a default aggregation. A long numeric
                selection can use one window when a count confirms a small result. Other exports
                estimate the window from the channel rates, targeting 25 million samples by default.
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
                have the same name, even across datasets. Iteration also raises for unsupported
                physical storage types or when a raw window mixes floats and integers that cannot
                be represented together without losing precision.
        """
        if not channels:
            logger.warning("No channels requested for export-- returning")
            return

        if None not in (buckets, resolution):
            raise ValueError("Cannot export data decimated with both buckets and resolution")

        supported_channels = [ch for ch in channels if ch.data_type in POLARS_TYPES]
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

        export_jobs = self._compute_export_jobs(
            supported_channels,
            Bounds(start, end),
            tags or {},
            buckets,
            resolution,
            batch_duration,
            join_batches=join_batches,
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
        results: list[tuple[str | None, list[pl.DataFrame]]] = []
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
                    single = job.channels[0].name if len(job.channels) == 1 else None
                    results.append((single, first.result()))
                    if (not pending or next(iter(pending.values()))[0] != window) and (
                        upcoming is None or upcoming[0] != window
                    ):
                        joined = join_window(results, time_column)
                        results.clear()
                        if not joined.is_empty():
                            yield format_time_col(joined, time_column, timestamp_type)
                else:
                    active = (future for future, (index, _) in pending.items() if index == window)
                    done = next(concurrent.futures.as_completed(active))
                    del pending[done]
                    for frame in done.result():
                        yield format_time_col(frame, time_column, timestamp_type)
        finally:
            pool.shutdown(cancel_futures=True)
