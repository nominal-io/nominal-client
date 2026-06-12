"""Per-channel, per-bucket detection of data the destination is missing over a window.

Detection answers one question for every channel in scope: *for each time bucket in the window,
does the destination have at least as much data as the source?* A bucket where the destination
falls short is a sync target.

There is no single server-side primitive that counts points for every channel type, so the
detector is type-aware while keeping all work server-side (minimize download):

* **Numeric** channels (``DOUBLE`` / ``INT``) are decimated by *resolution = bucket width*, so the
  returned buckets align one-to-one with our buckets and each carries an exact ``count``. One
  request covers the whole window.
* **Non-numeric** channels (``STRING``) have no cheap per-bucket count, so we fall back to a
  whole-window **presence** probe via :meth:`Channel.get_available_tags`. Within a tag-filtered
  scope this is reliable: any data matching the filter necessarily carries the filter's tags, so a
  non-empty result means "data present". Presence maps to a uniform count of 1 across every bucket
  (0 if absent). This loses per-bucket granularity for non-numeric series.

The comparison rule is the same for both: a bucket needs syncing when ``src_count > dest_count``
("any shortfall"). A channel that does not exist in the destination reads as all-zero counts, so it
is handled identically to a channel that exists but is empty over the window.

Adapted from the unmerged ``migration/backfill`` PR-stack (``backfill/detect.py``).
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass

from nominal_api import api

from nominal.core.channel import Channel, ChannelDataType
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

logger = logging.getLogger(__name__)

_NUMERIC_TYPES = frozenset({ChannelDataType.DOUBLE, ChannelDataType.INT})


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

    ``counts`` maps each bucket-start (ns) to a count. For numeric channels this is an exact point
    count; for non-numeric channels it is a presence proxy (1 if the channel has any data in the
    window, else 0), uniform across buckets. ``precise`` distinguishes the two so callers can report
    the coarser non-numeric granularity if desired.
    """

    channel: str
    counts: Mapping[int, int]
    precise: bool


def count_per_bucket(
    channel: Channel,
    start: IntegralNanosecondsUTC,
    end: IntegralNanosecondsUTC,
    bucket: IntegralNanosecondsUTC,
    tags: Mapping[str, str] | None = None,
) -> ChannelBucketCounts:
    """Count data per bucket for ``channel`` over ``[start, end)`` filtered by ``tags``.

    Dispatches on channel data type: exact per-bucket counts for numeric channels, whole-window
    presence for everything else. All buckets in the window are always present in the result
    (zero-filled), so a missing channel reads as all-zero rather than an empty mapping.
    """
    starts = iter_bucket_starts(start, end, bucket)
    if channel.data_type in _NUMERIC_TYPES:
        return ChannelBucketCounts(channel.name, _numeric_counts(channel, start, end, bucket, starts, tags), True)
    return ChannelBucketCounts(channel.name, _presence_counts(channel, start, end, starts, tags), False)


def shortfall_buckets(source: ChannelBucketCounts, destination: ChannelBucketCounts) -> list[int]:
    """Return the bucket-starts (sorted) where the destination has fewer points than the source.

    "Any shortfall" rule: a bucket is a sync target when ``src_count > dest_count``.
    """
    return sorted(
        bucket_start
        for bucket_start, src_count in source.counts.items()
        if src_count > destination.counts.get(bucket_start, 0)
    )


def _numeric_counts(
    channel: Channel,
    start: IntegralNanosecondsUTC,
    end: IntegralNanosecondsUTC,
    bucket: IntegralNanosecondsUTC,
    starts: list[int],
    tags: Mapping[str, str] | None,
) -> dict[int, int]:
    counts = dict.fromkeys(starts, 0)
    response = channel._decimate_request(start, end, tags=tags, resolution=int(bucket))

    if response.bucketed_numeric is not None:
        for timestamp, point in zip(
            response.bucketed_numeric.timestamps, response.bucketed_numeric.buckets, strict=False
        ):
            _add_to_bucket(counts, starts, _ts_to_nanos(timestamp), bucket, start, point.count or 0)
    elif response.numeric is not None:
        # Server returns raw points instead of buckets when the range holds few points; bin them.
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
