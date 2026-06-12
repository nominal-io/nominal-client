"""Shared, transport-agnostic planning for channel-data exports.

This module owns the *planning* phase common to every export handler: estimating per-channel
point rates, packing channels into request-sized groups, subdividing the time range into batches,
and building the conjure :class:`ExportDataRequest` for each job. It is deliberately free of any
download/parse logic so concrete handlers (e.g. the streaming
:class:`~nominal.thirdparty.polars.polars_export_handler.PolarsExportHandler` and the presigned-link
``PresignedExportHandler``) can inherit :class:`ExportHandler` and supply only their own transport.
"""

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
from nominal.core.client import NominalClient
from nominal.core.datasource import DataSource
from nominal.experimental.compute import Bucket, batch_compute_buckets
from nominal.experimental.compute._buckets import EnumBucket, batch_compute_enum_buckets
from nominal.experimental.compute.dsl import exprs
from nominal.ts import (
    IntegralNanosecondsDuration,
    IntegralNanosecondsUTC,
    _AnyExportableTimestampType,
    _InferrableTimestampType,
    _SecondsNanos,
    _to_export_timestamp_format,
)

# Number of workers to use across thread / processes pools when hitting the api
DEFAULT_NUM_WORKERS = 8

# Maximum number of buckets / decimated points exported per compute query.
# TODO(drake) raise 1000 limit once backend limit is raised
MAX_NUM_BUCKETS = 1000

DEFAULT_EXPORTED_TIMESTAMP_COL_NAME = "timestamp"

# Number of channels queried together when estimating per-channel point rates. Internal to planning
# (kept small to stay within compute concurrency limits); unrelated to a handler's export batch size.
_POINT_RATE_QUERY_BATCH_SIZE = 25

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


def _has_data_with_tags(channel: Channel, tags: Mapping[str, str], start_ns: int, end_ns: int) -> bool:
    available_tags = channel.get_available_tags(start_ns, end_ns, tags)

    # No data matches the given tags
    if not available_tags:
        return False

    bad_tag_items = {name: values for name, values in available_tags.items() if len(values) > 1}
    if bad_tag_items:
        logger.warning(
            "Channel %s has underconstrained tags-- results may have duplicate rows: %s", channel.name, bad_tag_items
        )

    return True


def _build_point_rate_expressions(
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
        elif tags and not _has_data_with_tags(channel, tags, start_ns, end_ns):
            logger.warning("No points found in range for channel '%s'", channel.name)
            expressions.append((channel, None))
        else:
            expressions.append((channel, exprs.NumericExpr.datasource_channel(channel.data_source, channel.name, tags)))

    return expressions


def _batch_channel_points_per_second(
    client: NominalClient,
    channels: Sequence[Channel],
    start_ns: IntegralNanosecondsUTC,
    end_ns: IntegralNanosecondsUTC,
    tags: dict[str, str],
    num_buckets: int,
) -> Mapping[str, float | None]:
    """For each provided channel, determine the maximum number of points per second in the given range.

    NOTE: Not intended for direct use-- see `_channel_points_per_second`
    NOTE: do not use with more than 300 channels, or 500 concurrently across all requests, or concurrency limits
          will be breached and the request will fail.

    Args:
        client: Nominal request client
        channels: Channels to query data rates for
        start_ns: Start of the time range to query over
        end_ns: End of the time range to query over
        tags: Key-value pairs of tags to filter data with
        num_buckets: Number of buckets to use-- more typically leads to better results.
            NOTE: max number of buckets allowed is 1000

    Returns:
        Mapping of channel name to maximum points/second for the respective channels
    """
    if not channels:
        logger.warning("No channels given!")
        return {}
    elif num_buckets > MAX_NUM_BUCKETS:
        raise ValueError(f"num_buckets ({num_buckets}) must be <= {MAX_NUM_BUCKETS}")

    # For each channel that has data with the given tags within the provided time range, add a
    # compute expression to later retrieve decimated bucket stats
    results: dict[str, float | None] = {}
    expressions = []
    channels_in_expressions = []
    for channel, expression in _build_point_rate_expressions(list(channels), start_ns, end_ns, tags):
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
    start_ns = _SecondsNanos.from_flexible(start).to_nanoseconds()
    end_ns = _SecondsNanos.from_flexible(end).to_nanoseconds()
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = {}
        for idx in range(0, len(channels), _POINT_RATE_QUERY_BATCH_SIZE):
            channel_batch = channels[idx : idx + _POINT_RATE_QUERY_BATCH_SIZE]
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


def _batch_channel_buckets(
    client: NominalClient,
    channels: Sequence[Channel],
    start_ns: IntegralNanosecondsUTC,
    end_ns: IntegralNanosecondsUTC,
    tags: dict[str, str],
    num_buckets: int,
) -> Mapping[str, list[Bucket]]:
    """Compute decimated buckets for the channels and return the raw per-bucket stats.

    Unlike :func:`_batch_channel_points_per_second` (which reduces buckets to a single rate), this
    keeps the per-bucket ``(timestamp, count)`` stats so callers can tell *when* a channel has data.
    Channels with no compute expression (non-numeric / no data with tags) are omitted.

    NOTE: Not intended for direct use-- see :func:`_channel_data_buckets`.
    """
    if not channels:
        return {}
    elif num_buckets > MAX_NUM_BUCKETS:
        raise ValueError(f"num_buckets ({num_buckets}) must be <= {MAX_NUM_BUCKETS}")

    expressions = []
    channels_in_expressions = []
    for channel, expression in _build_point_rate_expressions(list(channels), start_ns, end_ns, tags):
        if expression is not None:
            expressions.append(expression)
            channels_in_expressions.append(channel)

    if not expressions:
        return {}

    batch_buckets = batch_compute_buckets(client, expressions, start_ns, end_ns, buckets=num_buckets)
    return {channel.name: list(buckets) for channel, buckets in zip(channels_in_expressions, batch_buckets)}


def _max_rate_from_buckets(buckets: Sequence[Bucket], start_ns: int, end_ns: int) -> float:
    """Reduce a channel's decimated buckets to its maximum points-per-second (0.0 if no data)."""
    if not buckets:
        return 0.0
    if len(buckets) == 1:
        return buckets[0].count / ((end_ns - start_ns) / 1e9)
    max_points_per_second = 0.0
    for idx in range(1, len(buckets)):
        bucket = buckets[idx]
        last_bucket = buckets[idx - 1]
        bucket_duration = (bucket.timestamp - last_bucket.timestamp) / 1e9
        if bucket_duration > 0:
            max_points_per_second = max(max_points_per_second, bucket.count / bucket_duration)
    return max_points_per_second


def _channel_data_buckets(
    client: NominalClient,
    channels: Sequence[Channel],
    start: _InferrableTimestampType,
    end: _InferrableTimestampType,
    tags: Mapping[str, str] | None = None,
    num_buckets: int = 100,
    num_workers: int = DEFAULT_NUM_WORKERS,
) -> Mapping[str, list[Bucket]]:
    """Single bucketed-compute pass returning ``{channel_name: [buckets]}`` for channels with data.

    Reuses the same compute as rate estimation but preserves the per-bucket counts, so a caller can
    derive *both* the per-channel rate (via :func:`_max_rate_from_buckets`) and *when* each channel
    has data (non-empty buckets) from one pass. Channels with no data in range are omitted.
    """
    start_ns = _SecondsNanos.from_flexible(start).to_nanoseconds()
    end_ns = _SecondsNanos.from_flexible(end).to_nanoseconds()
    tags = dict(tags or {})
    results: dict[str, list[Bucket]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = {}
        for idx in range(0, len(channels), _POINT_RATE_QUERY_BATCH_SIZE):
            channel_batch = channels[idx : idx + _POINT_RATE_QUERY_BATCH_SIZE]
            fut = pool.submit(_batch_channel_buckets, client, channel_batch, start_ns, end_ns, tags, num_buckets)
            futures[fut] = channel_batch

        for fut in concurrent.futures.as_completed(futures):
            channel_batch = futures[fut]
            ex = fut.exception()
            if ex is not None:
                logger.error("Failed to compute buckets for %d channels", len(channel_batch), exc_info=ex)
                continue
            for name, buckets in fut.result().items():
                if buckets:
                    results[name] = buckets
    return results


def _batch_channel_enum_buckets(
    client: NominalClient,
    channels: Sequence[Channel],
    start_ns: IntegralNanosecondsUTC,
    end_ns: IntegralNanosecondsUTC,
    tags: dict[str, str],
    num_buckets: int,
) -> tuple[dict[str, list[EnumBucket]], set[str]]:
    """Compute decimated buckets for enum (string) channels; isolate channels that can't be bucketed.

    The enum counterpart to :func:`_batch_channel_buckets` (numeric bucketing only handles DOUBLE).
    Enum bucketing computes per-category frequencies and rejects high-cardinality channels
    (``Compute:TooManyCategories``), failing the whole batch. On failure we retry per channel so the
    determinable ones still yield presence; channels that cannot be bucketed are returned as an
    "undetermined" set (the caller exports them for all slices rather than dropping them).

    Returns ``({channel_name: [buckets]}, {undetermined_channel_names})``. NOTE: not for direct use.
    """
    if not channels:
        return {}, set()
    elif num_buckets > MAX_NUM_BUCKETS:
        raise ValueError(f"num_buckets ({num_buckets}) must be <= {MAX_NUM_BUCKETS}")

    expressions = []
    channels_in_expressions = []
    for channel in channels:
        if tags and not _has_data_with_tags(channel, tags, start_ns, end_ns):
            continue
        expressions.append(exprs.EnumExpr.datasource_channel(channel.data_source, channel.name, tags))
        channels_in_expressions.append(channel)

    if not expressions:
        return {}, set()

    try:
        batch_buckets = batch_compute_enum_buckets(client, expressions, start_ns, end_ns, buckets=num_buckets)
        return {ch.name: list(buckets) for ch, buckets in zip(channels_in_expressions, batch_buckets)}, set()
    except Exception:
        # A single high-cardinality channel fails the whole batch; isolate per channel so we keep
        # presence for the determinable ones and flag the rest as undetermined (never dropped).
        results: dict[str, list[EnumBucket]] = {}
        undetermined: set[str] = set()
        for channel, expression in zip(channels_in_expressions, expressions):
            try:
                single = batch_compute_enum_buckets(client, [expression], start_ns, end_ns, buckets=num_buckets)
                results[channel.name] = list(single[0]) if single else []
            except Exception:
                undetermined.add(channel.name)
        if undetermined:
            logger.warning(
                "Could not determine data presence for %d enum channel(s) (e.g. too many categories); "
                "they will be exported for all time slices",
                len(undetermined),
            )
        return results, undetermined


def _channel_enum_buckets(
    client: NominalClient,
    channels: Sequence[Channel],
    start: _InferrableTimestampType,
    end: _InferrableTimestampType,
    tags: Mapping[str, str] | None = None,
    num_buckets: int = 100,
    num_workers: int = DEFAULT_NUM_WORKERS,
) -> tuple[Mapping[str, list[EnumBucket]], set[str]]:
    """Like :func:`_channel_data_buckets` but for enum (string) channels (uses enum bucketing).

    Returns ``({channel_name: [buckets]}, {undetermined_channel_names})``: buckets for enum channels
    with determinable data (channels with no data are omitted), and the set of channels whose
    presence could not be computed (the caller should export those for all slices).
    """
    start_ns = _SecondsNanos.from_flexible(start).to_nanoseconds()
    end_ns = _SecondsNanos.from_flexible(end).to_nanoseconds()
    tags = dict(tags or {})
    results: dict[str, list[EnumBucket]] = {}
    undetermined: set[str] = set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = {}
        for idx in range(0, len(channels), _POINT_RATE_QUERY_BATCH_SIZE):
            channel_batch = channels[idx : idx + _POINT_RATE_QUERY_BATCH_SIZE]
            fut = pool.submit(_batch_channel_enum_buckets, client, channel_batch, start_ns, end_ns, tags, num_buckets)
            futures[fut] = channel_batch

        for fut in concurrent.futures.as_completed(futures):
            channel_batch = futures[fut]
            ex = fut.exception()
            if ex is not None:
                # Unexpected failure (the per-channel fallback is inside the worker): be conservative
                # and export these channels for all slices rather than dropping them.
                logger.error("Failed to compute enum buckets for %d channels", len(channel_batch), exc_info=ex)
                undetermined.update(channel.name for channel in channel_batch)
                continue
            batch_results, batch_undetermined = fut.result()
            for name, buckets in batch_results.items():
                if buckets:
                    results[name] = buckets
            undetermined |= batch_undetermined
    return results, undetermined


def _build_channel_groups(
    points_per_second: Mapping[str, float],
    channels_by_name: Mapping[str, Channel],
    points_per_request: int,
    channels_per_request: int,
    batch_duration: datetime.timedelta,
) -> tuple[list[list[Channel]], list[Channel]]:
    """Build a tuple of groups of channels to read together, and a list of channels that must be read on their own."""
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

    # Compression applied to the exported file (None for uncompressed)
    compression: scout_dataexport_api.CompressionFormat | None = scout_dataexport_api.CompressionFormat.GZIP

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
            context=scout_compute_api.Context(frame_references={}, variables={}, function_variables={}),
            end_time=self.time_slice.end_api,
            start_time=self.time_slice.start_api,
            resolution=self.resolution_options(),
            compression=self.compression,
            format=scout_dataexport_api.ExportFormat(
                csv=scout_dataexport_api.Csv(),
            ),
        )


class ExportHandler:
    """Base class owning the transport-agnostic planning shared by all export handlers.

    Concrete subclasses inherit the planning (point-rate estimation, channel grouping, time
    slicing, and request construction) and add their own transport: how each planned
    :class:`_ExportJob` is fetched and how results are delivered.
    """

    def __init__(
        self,
        client: NominalClient,
        points_per_request: int,
        points_per_dataframe: int,
        channels_per_request: int,
        num_workers: int,
        compression: scout_dataexport_api.CompressionFormat | None = scout_dataexport_api.CompressionFormat.GZIP,
    ):
        """Initialize the shared export planning configuration.

        Args:
            client: Nominal client for communicating with the API.
            points_per_request: Target maximum number of points within a single export request.
            points_per_dataframe: Target maximum number of points within each delivered batch/file;
                drives how the time range is subdivided.
            channels_per_request: Maximum number of channels packed into a single export request.
            num_workers: Number of parallel workers used when estimating point rates.
            compression: Compression applied to each export request (None for uncompressed).
        """
        self._client = client
        self._points_per_request = points_per_request
        self._points_per_dataframe = points_per_dataframe
        self._channels_per_request = channels_per_request
        self._num_workers = num_workers
        self._compression = compression

    def _compute_channel_points_per_second(
        self, numeric_channels: Sequence[Channel], time_range: _TimeRange, tags: Mapping[str, str] | None = None
    ) -> dict[str, float]:
        all_points_per_second = _channel_points_per_second(
            client=self._client,
            channels=numeric_channels,
            start=time_range.start_time,
            end=time_range.end_time,
            tags=tags,
        )
        return {channel: rate for channel, rate in all_points_per_second.items() if rate}

    def _compute_batch_duration(
        self,
        batch_duration: datetime.timedelta | None,
        enum_channels: Sequence[Channel],
        time_range: _TimeRange,
        points_per_second: Mapping[str, float],
    ) -> IntegralNanosecondsDuration:
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
        points_per_second = self._compute_channel_points_per_second(numeric_channels, time_range, tags)
        batch_duration_ns = self._compute_batch_duration(batch_duration, enum_channels, time_range, points_per_second)

        # group channels by datasource
        channel_names_by_datasource = collections.defaultdict(set)
        for channel_group in (numeric_channels, enum_channels):
            for channel in channel_group:
                channel_names_by_datasource[channel.data_source].add(channel.name)

        jobs = collections.defaultdict(list)
        time_slices = time_range.subdivide_ns(batch_duration_ns)
        for datasource_rid, channel_names in channel_names_by_datasource.items():
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
                        _ExportJob(
                            datasource_rid=datasource_rid,
                            channel_names=[ch.name for ch in channel_group],
                            channel_types={ch.name: ch.data_type for ch in channel_group},
                            time_slice=slice,
                            tags=dict(tags or {}),
                            buckets=buckets,
                            resolution=resolution,
                            timestamp_type=timestamp_type,
                            compression=self._compression,
                        )
                    )

                # Add subdivided slices for large channels that cannot be read in a single export request
                # For large channels, we need to subdivide the time range based on their data rates
                for channel in large_channels:
                    channel_rate = points_per_second[channel.name]
                    sub_offset = datetime.timedelta(seconds=self._points_per_request / channel_rate)
                    for sub_slice in slice.subdivide(sub_offset):
                        jobs[slice].append(
                            _ExportJob(
                                datasource_rid=datasource_rid,
                                channel_names=[channel.name],
                                channel_types={ch.name: ch.data_type for ch in channel_group},
                                time_slice=sub_slice,
                                tags=dict(tags or {}),
                                buckets=buckets,
                                resolution=resolution,
                                timestamp_type=timestamp_type,
                                compression=self._compression,
                            )
                        )

        return jobs
