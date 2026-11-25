"""Compute point counts used for optional export sizing."""

import concurrent.futures
import functools
import logging
from collections.abc import Mapping, Sequence

from nominal._utils.iterator_tools import batched
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core.bounds import Bounds
from nominal.core.channel import Channel
from nominal.core.client import NominalClient
from nominal.protos.compute.v1 import compute_pb2, execution_results_pb2
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

logger = logging.getLogger(__name__)

SAMPLE_BATCH_SIZE = 200


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


def sample_counts(
    client: NominalClient,
    channels: Sequence[Channel],
    bounds: Bounds,
    tags: Mapping[str, str] | None,
    bucket_count: int,
) -> dict[tuple[str, str], Sequence[tuple[int, int]]]:
    """Return successful counts by dataset and channel. An empty sequence is a confirmed zero."""
    request = compute_pb2.BatchComputeWithUnitsRequest(
        inputs=[ch._series_summary_input(bounds.start, bounds.end, tags, buckets=bucket_count) for ch in channels]
    )
    try:
        with translate_grpc_errors():
            response = client._clients.compute.BatchComputeWithUnits(request)
    except Exception:
        logger.warning("Could not sample %d channels for export sizing", len(channels), exc_info=True)
        return {}

    counts = {}
    for channel, result in zip(channels, response.results):
        if result.outcome.HasField("success"):
            buckets = _extract_bucket_counts(result.outcome.success)
            if buckets is not None:
                counts[channel.data_source, channel.name] = buckets
        else:
            logger.warning("No point count for channel %r: %s", channel.name, result.outcome.failure)
    return counts


def estimate_export_rates(
    client: NominalClient,
    channels: Sequence[Channel],
    bounds: Bounds,
    tags: Mapping[str, str] | None,
    num_workers: int,
    require_all: bool,
) -> dict[tuple[str, str], float]:
    """Use the highest observed rate for unknown channels when sizing a frame approximately."""
    # The service allows 500 channel computations in flight. Two batches of 200 leave headroom.
    sample = functools.partial(
        sample_counts,
        client,
        bounds=bounds,
        tags=tags,
        bucket_count=100,
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(num_workers, 2)) as pool:
        rates = {
            key: _peak_points_per_second(counts, bounds.start, bounds.end)
            for batch in pool.map(sample, batched(channels, SAMPLE_BATCH_SIZE))
            for key, counts in batch.items()
        }

    positive = {key: rate for key, rate in rates.items() if rate}
    unknown = [(ch.data_source, ch.name) for ch in channels if (ch.data_source, ch.name) not in rates]
    if unknown:
        if require_all or not positive:
            raise RuntimeError(
                f"Could not size export jobs for {len(unknown)} channels. "
                "Specify batch_duration and omit points_per_request to export without rate estimates."
            )
        positive.update(dict.fromkeys(unknown, max(positive.values())))
    return positive
