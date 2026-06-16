"""Per-channel, per-bucket detection of data the destination is missing over a window.

Detection answers one question for every channel in scope: *for each time bucket in the window,
does the destination have at least as much data as the source?* A bucket where the destination
falls short (``src_count > dest_count``, "any shortfall") is a sync target. A channel absent in the
destination reads as all-zero counts, so it is handled identically to a channel that exists but is
empty over the window.

Counts are obtained server-side and **in batch**: :func:`count_channels` summarizes many channels
in one ``batch_compute_with_units`` request (chunked, with chunks issued across a thread pool), so a
dataset with thousands of channels costs a few hundred requests rather than one per channel.

* **Numeric** (``DOUBLE`` / ``INT``) channels yield exact per-bucket counts from numeric decimation.
* **String** (``STRING``) channels yield exact per-bucket counts from enum decimation (the per-bucket
  histogram frequencies sum to the count).
* Any channel whose batched compute result errored falls back to a whole-window **presence** probe
  via :meth:`Channel.get_available_tags` (1 if any data is present in the window, else 0, uniform
  across buckets). Within a tag-filtered scope this is reliable, but it loses per-bucket granularity.

Adapted from the unmerged ``migration/backfill`` PR-stack (``backfill/detect.py``).
"""

from __future__ import annotations

import concurrent.futures
import logging
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from nominal_api import api, scout_compute_api

from nominal.core.channel import Channel, ChannelDataType
from nominal.experimental.compute._buckets import (
    _enum_buckets_from_compute_response,
    _numeric_buckets_from_compute_response,
)
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

logger = logging.getLogger(__name__)

_NUMERIC_TYPES = frozenset({ChannelDataType.DOUBLE, ChannelDataType.INT})
_BATCHABLE_TYPES = frozenset({ChannelDataType.DOUBLE, ChannelDataType.INT, ChannelDataType.STRING})
DEFAULT_DETECT_CHANNELS_PER_REQUEST = 100
DEFAULT_DETECT_WORKERS = 8


def iter_bucket_starts(
    start: IntegralNanosecondsUTC,
    end: IntegralNanosecondsUTC,
    bucket: IntegralNanosecondsUTC,
) -> list[int]:
    """Return the start (ns) of every bucket covering ``[start, end)``.

    Buckets are ``bucket`` nanoseconds wide and tile forward from ``start``. The final bucket may
    extend past ``end``; it is included as long as its start is strictly before ``end``.
    """
    if bucket <= 0:
        raise ValueError(f"bucket must be positive, got {bucket}")
    if end <= start:
        raise ValueError(f"end ({end}) must be after start ({start})")
    starts: list[int] = []
    current = int(start)
    while current < end:
        starts.append(current)
        current += int(bucket)
    return starts


def merge_bucket_ranges(bucket_starts: list[int], bucket: IntegralNanosecondsUTC) -> list[tuple[int, int]]:
    """Merge contiguous bucket-starts into ``[start, end)`` ranges.

    Adjacent buckets (where one bucket's start plus ``bucket`` equals the next start) coalesce into
    a single range so the export issues one request per contiguous span instead of one per bucket.
    """
    ordered = sorted(set(bucket_starts))
    ranges: list[tuple[int, int]] = []
    for start in ordered:
        end = start + int(bucket)
        if ranges and start == ranges[-1][1]:
            ranges[-1] = (ranges[-1][0], end)
        else:
            ranges.append((start, end))
    return ranges


@dataclass(frozen=True)
class ChannelBucketCounts:
    """Per-bucket data counts for a single channel.

    ``counts`` maps each bucket-start (ns) to a count. For numeric/string channels this is an exact
    point count; for the presence fallback it is 1 if the channel has any data in the window (else
    0), uniform across buckets. ``precise`` distinguishes the two so callers can report the coarser
    fallback granularity if desired.
    """

    channel: str
    counts: Mapping[int, int]
    precise: bool


def shortfall_buckets(source: ChannelBucketCounts, destination: ChannelBucketCounts) -> list[int]:
    """Return the bucket-starts (sorted) where the destination has fewer points than the source.

    "Any shortfall" rule: a bucket is a sync target when ``src_count > dest_count``.
    """
    return sorted(
        bucket_start
        for bucket_start, src_count in source.counts.items()
        if src_count > destination.counts.get(bucket_start, 0)
    )


def count_channels(
    channels: Sequence[Channel],
    start: IntegralNanosecondsUTC,
    end: IntegralNanosecondsUTC,
    bucket: IntegralNanosecondsUTC,
    tags: Mapping[str, str] | None = None,
    *,
    channels_per_request: int = DEFAULT_DETECT_CHANNELS_PER_REQUEST,
    workers: int = DEFAULT_DETECT_WORKERS,
    request_delay: float = 0.0,
    on_advance: Callable[[int], None] | None = None,
) -> dict[str, ChannelBucketCounts]:
    """Count per-bucket data for many channels using batched, parallel server-side compute.

    Numeric and string channels are summarized in ``batch_compute_with_units`` requests, chunked by
    ``channels_per_request`` with chunks issued across ``workers`` threads. Any channel whose batch
    result errored falls back to a whole-window presence probe.

    ``on_advance`` (when given) is called with the number of channels resolved each step, summing to
    ``len(channels)`` over the call -- a progress hook for the caller's detection bar.

    Returns a mapping of channel name to zero-filled :class:`ChannelBucketCounts` for every input
    channel. Channels are assumed to have unique names and to share a single client (e.g. all from
    one dataset).
    """
    starts = iter_bucket_starts(start, end, bucket)
    batchable = [c for c in channels if c.data_type in _BATCHABLE_TYPES]
    fallback = [c for c in channels if c.data_type not in _BATCHABLE_TYPES]

    results: dict[str, ChannelBucketCounts] = {}
    errored: list[Channel] = []
    chunks = [batchable[i : i + channels_per_request] for i in range(0, len(batchable), channels_per_request)]

    if chunks:
        logger.info(
            "Counting %d channel(s) over %d bucket(s) in %d batched request(s) across %d worker(s)",
            len(batchable),
            len(starts),
            len(chunks),
            workers,
        )
        done = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(workers, len(chunks)))) as pool:
            futures = []
            for i, chunk in enumerate(chunks):
                if request_delay > 0 and i > 0:
                    time.sleep(request_delay)
                futures.append(pool.submit(_count_chunk, chunk, start, end, bucket, starts, tags))
            for future in concurrent.futures.as_completed(futures):
                chunk_counts, chunk_errored = future.result()
                results.update(chunk_counts)
                errored.extend(chunk_errored)
                done += len(chunk_counts) + len(chunk_errored)
                logger.info("Detection progress: %d/%d channels counted", done, len(batchable))
                # Advance only by channels resolved in this batch; errored ones advance below in the
                # presence pass, so the total over the whole call sums to len(channels).
                if on_advance is not None:
                    on_advance(len(chunk_counts))

    presence_channels = fallback + errored
    if presence_channels:
        logger.info("Presence-probing %d channel(s) (non-batchable or errored)", len(presence_channels))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(workers, len(presence_channels)))) as pool:
            for channel, counts in pool.map(
                lambda c: (c, _presence_counts(c, start, end, starts, tags)), presence_channels
            ):
                results[channel.name] = ChannelBucketCounts(channel.name, counts, precise=False)
                if on_advance is not None:
                    on_advance(1)

    return results


def count_per_bucket(
    channel: Channel,
    start: IntegralNanosecondsUTC,
    end: IntegralNanosecondsUTC,
    bucket: IntegralNanosecondsUTC,
    tags: Mapping[str, str] | None = None,
) -> ChannelBucketCounts:
    """Count data per bucket for a single ``channel`` over ``[start, end)`` filtered by ``tags``.

    Exact per-bucket counts for numeric channels via decimation, whole-window presence for everything
    else. All buckets are zero-filled, so a missing channel reads as all-zero rather than empty. Kept
    for single-channel use; :func:`count_channels` is the batched path used for whole-dataset scans.
    """
    starts = iter_bucket_starts(start, end, bucket)
    if channel.data_type in _NUMERIC_TYPES:
        return ChannelBucketCounts(channel.name, _numeric_counts(channel, start, end, bucket, starts, tags), True)
    return ChannelBucketCounts(channel.name, _presence_counts(channel, start, end, starts, tags), False)


def _count_chunk(
    chunk: Sequence[Channel],
    start: IntegralNanosecondsUTC,
    end: IntegralNanosecondsUTC,
    bucket: IntegralNanosecondsUTC,
    starts: list[int],
    tags: Mapping[str, str] | None,
) -> tuple[dict[str, ChannelBucketCounts], list[Channel]]:
    """Run one batched compute request for a chunk of channels; return counts and any that errored."""
    clients = chunk[0]._clients
    request = scout_compute_api.BatchComputeWithUnitsRequest(
        requests=[_bucket_request(channel, tags, start, end, len(starts)) for channel in chunk]
    )
    response = clients.compute.batch_compute_with_units(clients.auth_header, request)

    counts: dict[str, ChannelBucketCounts] = {}
    errored: list[Channel] = []
    for channel, result in zip(chunk, response.results, strict=False):
        compute_result = result.compute_result
        if compute_result is None or compute_result.success is None:
            errored.append(channel)
            continue
        binned = _counts_from_response(compute_result.success, starts, bucket, start)
        if binned is None:
            errored.append(channel)
        else:
            counts[channel.name] = ChannelBucketCounts(channel.name, binned, precise=True)
    return counts, errored


def _bucket_request(
    channel: Channel,
    tags: Mapping[str, str] | None,
    start: IntegralNanosecondsUTC,
    end: IntegralNanosecondsUTC,
    buckets: int,
) -> scout_compute_api.ComputeNodeRequest:
    """Build a bucketed SummarizeSeries request for one channel (tag-filtered)."""
    return scout_compute_api.ComputeNodeRequest(
        context=scout_compute_api.Context(dataset_references={}, variables={}, function_variables={}),
        node=scout_compute_api.ComputableNode(
            series=scout_compute_api.SummarizeSeries(
                input=channel._to_compute_series(tags=tags),
                numeric_aggregations={},
                summarization_strategy=scout_compute_api.SummarizationStrategy(
                    decimate=scout_compute_api.DecimateStrategy(
                        buckets=scout_compute_api.DecimateWithBuckets(buckets=buckets)
                    )
                ),
                buckets=buckets,
            )
        ),
        start=_SecondsNanos.from_nanoseconds(start).to_api(),
        end=_SecondsNanos.from_nanoseconds(end).to_api(),
    )


def _counts_from_response(
    response: scout_compute_api.ComputeNodeResponse,
    starts: list[int],
    bucket: IntegralNanosecondsUTC,
    start: IntegralNanosecondsUTC,
) -> dict[int, int] | None:
    """Bin a single channel's bucketed compute response into per-bucket counts, or None if untyped.

    Decimated responses (``bucketed_numeric`` / ``bucketed_enum``) carry each bucket's **right-edge**
    timestamp, so they are shifted left by one ``bucket`` before binning -- otherwise every count lands
    one bucket too late and a channel's first data bucket reads 0 (it would then never be flagged
    missing or synced). Undecimated/raw responses carry real point timestamps and bin as-is.
    """
    counts = dict.fromkeys(starts, 0)
    step = int(bucket)
    if response.bucketed_numeric is not None:
        for timestamp, numeric_bucket in _numeric_buckets_from_compute_response(response):
            _add_to_bucket(counts, starts, _ts_to_nanos(timestamp) - step, bucket, start, numeric_bucket.count or 0)
        return counts
    if response.numeric is not None or response.numeric_point is not None:
        for timestamp, numeric_bucket in _numeric_buckets_from_compute_response(response):
            _add_to_bucket(counts, starts, _ts_to_nanos(timestamp), bucket, start, numeric_bucket.count or 0)
        return counts
    if response.bucketed_enum is not None:
        for enum_bucket in _enum_buckets_from_compute_response(response):
            total = sum(enum_bucket.frequencies.values())
            _add_to_bucket(counts, starts, enum_bucket.timestamp - step, bucket, start, total)
        return counts
    if response.enum is not None:
        for enum_bucket in _enum_buckets_from_compute_response(response):
            _add_to_bucket(counts, starts, enum_bucket.timestamp, bucket, start, sum(enum_bucket.frequencies.values()))
        return counts
    return None


def _numeric_counts(
    channel: Channel,
    start: IntegralNanosecondsUTC,
    end: IntegralNanosecondsUTC,
    bucket: IntegralNanosecondsUTC,
    starts: list[int],
    tags: Mapping[str, str] | None,
) -> dict[int, int]:
    counts = dict.fromkeys(starts, 0)
    step = int(bucket)
    response = channel._decimate_request(start, end, tags=tags, resolution=int(bucket))

    if response.bucketed_numeric is not None:
        # Decimation timestamps are bucket right edges; shift left one bucket to bin correctly.
        for timestamp, point in zip(
            response.bucketed_numeric.timestamps, response.bucketed_numeric.buckets, strict=False
        ):
            _add_to_bucket(counts, starts, _ts_to_nanos(timestamp) - step, bucket, start, point.count or 0)
    elif response.numeric is not None:
        # Server returns raw points instead of buckets when the range holds few points; bin them as-is.
        for timestamp in response.numeric.timestamps:
            _add_to_bucket(counts, starts, _ts_to_nanos(timestamp), bucket, start, 1)
    else:
        logger.warning("Decimation of numeric channel %s returned neither buckets nor points", channel.name)
    return counts


def _presence_counts(
    channel: Channel,
    start: IntegralNanosecondsUTC,
    end: IntegralNanosecondsUTC,
    starts: list[int],
    tags: Mapping[str, str] | None,
) -> dict[int, int]:
    available = channel.get_available_tags(start, end, initial_tags=tags)
    present = 1 if available else 0
    return dict.fromkeys(starts, present)


def _add_to_bucket(
    counts: dict[int, int],
    starts: list[int],
    point_ns: int,
    bucket: IntegralNanosecondsUTC,
    start: IntegralNanosecondsUTC,
    amount: int,
) -> None:
    index = (point_ns - int(start)) // int(bucket)
    if 0 <= index < len(starts):
        counts[starts[index]] += amount


def _ts_to_nanos(timestamp: api.Timestamp) -> int:
    # Compute responses carry api.Timestamp (seconds + nanos); reuse the SDK's converter.
    return _SecondsNanos.from_api(timestamp).to_nanoseconds()
