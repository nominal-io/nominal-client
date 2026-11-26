"""Compute point counts used for optional export sizing."""

import concurrent.futures
import functools
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from itertools import pairwise

from nominal._utils.iterator_tools import batched
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core.bounds import Bounds
from nominal.core.channel import Channel
from nominal.core.client import NominalClient
from nominal.protos.compute.v1 import compute_pb2, execution_results_pb2
from nominal.ts import _SecondsNanos

logger = logging.getLogger(__name__)

SAMPLE_BATCH_SIZE = 200


@dataclass(frozen=True)
class PointEstimate:
    """Peak samples per second and the full-range count when available."""

    peak_rate: float
    total: int | None


def _estimate_response(
    series: execution_results_pb2.ComputationResult, span_ns: int, bucket_count: int
) -> PointEstimate | None:
    if series.HasField("numeric_buckets"):
        if any(not b.statistics.HasField("count") for b in series.numeric_buckets.buckets):
            return None
        counts = [b.statistics.count for b in series.numeric_buckets.buckets]
    elif series.HasField("categorical_buckets"):
        counts = [sum(b.statistics.count_by_category_index.values()) for b in series.categorical_buckets.buckets]
    else:
        paged = False
        if series.HasField("numeric_samples"):
            timestamps = [p.timestamp for p in series.numeric_samples.samples]
            paged = bool(series.numeric_samples.next_page_token)
        elif series.HasField("categorical_samples"):
            timestamps = [p.timestamp for p in series.categorical_samples.samples]
            paged = bool(series.categorical_samples.next_page_token)
        elif series.HasField("numeric_sample") or series.HasField("categorical_sample"):
            single = series.numeric_sample if series.HasField("numeric_sample") else series.categorical_sample
            timestamps = [single.sample.timestamp] if single.HasField("sample") else []
        else:
            logger.warning("Unrecognized compute response type: %s", series.WhichOneof("kind"))
            return None
        if paged and not timestamps:
            return None
        nanos = (_SecondsNanos.from_proto(ts).to_nanoseconds() for ts in timestamps)
        shortest = min((b - a for a, b in pairwise(nanos) if b > a), default=0)
        rate = len(timestamps) * 1e9 / span_ns if span_ns else 0.0
        if shortest:
            rate = max(rate, 1e9 / shortest)
        return PointEstimate(rate, None if paged else len(timestamps))

    total = sum(counts)
    rate = total * 1e9 / span_ns if span_ns else 0.0
    if len(counts) > 1 and span_ns:
        # Scout uses a fixed time grid. The first bucket label can fall anywhere in the requested range.
        width_ns = max(1, span_ns // bucket_count)
        rate = max(rate, max(counts) * 1e9 / width_ns)
    return PointEstimate(rate, total)


def sample_sizes(
    client: NominalClient,
    channels: Sequence[Channel],
    bounds: Bounds,
    tags: Mapping[str, str] | None,
    bucket_count: int,
) -> dict[tuple[str, str], PointEstimate]:
    """Sample channel rates and retain complete totals when the response includes them."""
    request = compute_pb2.BatchComputeWithUnitsRequest(
        inputs=[ch._series_summary_input(bounds.start, bounds.end, tags, buckets=bucket_count) for ch in channels]
    )
    try:
        with translate_grpc_errors():
            response = client._clients.compute.BatchComputeWithUnits(request)
    except Exception:
        logger.warning("Could not sample %d channels for export sizing", len(channels), exc_info=True)
        return {}

    sizes = {}
    for channel, result in zip(channels, response.results):
        if not result.outcome.HasField("success"):
            logger.warning("No point count for channel %r: %s", channel.name, result.outcome.failure)
            continue
        size = _estimate_response(result.outcome.success, bounds.duration, bucket_count)
        if size is not None:
            sizes[channel.data_source, channel.name] = size
    return sizes


def estimate_export_sizes(
    client: NominalClient,
    channels: Sequence[Channel],
    bounds: Bounds,
    tags: Mapping[str, str] | None,
    num_workers: int,
    require_all: bool,
) -> dict[tuple[str, str], PointEstimate]:
    """Use the highest observed rate for unknown channels when sizing a frame approximately."""
    # The service allows 500 channel computations in flight. Two batches of 200 leave headroom.
    sample = functools.partial(
        sample_sizes,
        client,
        bounds=bounds,
        tags=tags,
        bucket_count=100,
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(num_workers, 2)) as pool:
        sizes = {
            key: size for batch in pool.map(sample, batched(channels, SAMPLE_BATCH_SIZE)) for key, size in batch.items()
        }

    positive = {key: size for key, size in sizes.items() if size.peak_rate}
    unknown = [(ch.data_source, ch.name) for ch in channels if (ch.data_source, ch.name) not in sizes]
    if unknown:
        if require_all or not positive:
            raise RuntimeError(
                f"Could not size export jobs for {len(unknown)} channels. "
                "Specify batch_duration and omit points_per_request to export without rate estimates."
            )
        fallback = PointEstimate(max(size.peak_rate for size in positive.values()), None)
        positive.update(dict.fromkeys(unknown, fallback))
    return positive
