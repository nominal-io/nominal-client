from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

from nominal_api import api, scout_compute_api

from nominal.core import NominalClient
from nominal.experimental.compute.dsl import exprs, params


class ExceptionGroup(Exception):
    def __init__(self, message: str, exceptions: Sequence[Exception]):
        exc_str = "\n".join([str(ex) for ex in exceptions])
        super().__init__(f"{message}:\n{exc_str}")

        self.message = message
        self.exceptions = exceptions


@dataclass(frozen=True)
class Bucket:
    timestamp: params.NanosecondsUTC
    min: float
    max: float
    mean: float
    variance: float
    count: int

    @classmethod
    def _from_conjure(cls, timestamp: api.Timestamp, bucket: scout_compute_api.NumericBucket) -> Bucket:
        return cls(
            _timestamp_from_conjure(timestamp), bucket.min, bucket.max, bucket.mean, bucket.variance, bucket.count
        )


def batch_compute_buckets(
    client: NominalClient,
    numeric_exprs: Iterable[exprs.NumericExpr],
    start: params.NanosecondsUTC,
    end: params.NanosecondsUTC,
    buckets: int = 1000,
) -> Sequence[Sequence[Bucket]]:
    """Computed bucketed summaries for a batch of expressions

    Args:
        client: Nominal client to make requests with
        numeric_exprs: Expressions to compute buckets for
        start: Starting timestamp to summarize data from
        end: Ending timestamp to summarize data until
        buckets: Number of buckets to return per expression

    Returns:
        A Sequence of sequences of buckets. The top level sequence corresponds to the input expressions, whereas the
        inner sequences correspond to the individual buckets for each input expression. The order of buckets returned
        matches the order of expressions provided.
        NOTE: it is not a safe guarantee that the number of buckets returned is the same as the number requested
    """
    context: dict[str, exprs.NumericExpr] = {}

    # Create request
    api_start = _timestamp_to_conjure(start)
    api_end = _timestamp_to_conjure(end)
    api_context = {k: v._to_conjure() for k, v in context.items()}
    request = scout_compute_api.BatchComputeWithUnitsRequest(
        requests=[
            _create_compute_request_buckets(node._to_conjure(), api_context, api_start, api_end, buckets)
            for node in numeric_exprs
        ]
    )

    # Make request
    resp = client._clients.compute.batch_compute_with_units(
        auth_header=client._clients.auth_header,
        request=request,
    )

    # Parse response
    results: list[list[Bucket]] = []
    errors: list[Exception] = []
    for result in resp.results:
        compute_result = result.compute_result
        assert compute_result is not None

        compute_error = compute_result.error
        compute_response = compute_result.success
        if compute_error is not None:
            errors.append(RuntimeError(f"Failed to compute: {compute_error.error_type} ({compute_error.code})"))
        elif compute_response is not None:
            results.append(
                [Bucket._from_conjure(ts, bucket) for ts, bucket in _buckets_from_compute_response(compute_response)]
            )

    if errors:
        raise ExceptionGroup("Failed to compute batches", errors)

    return results


def compute_buckets(
    client: NominalClient,
    expr: exprs.NumericExpr,
    start: params.NanosecondsUTC,
    end: params.NanosecondsUTC,
    buckets: int = 1000,
) -> Sequence[Bucket]:
    """Compute a bucketed summary of the requested expression.

    Args:
        client: Nominal client to make requests with
        expr: Expression to compute buckets for
        start: Starting timestamp to summarize data from
        end: Ending timestamp to summarize data until
        buckets: Number of buckets to return

    Returns:
        Decimated data representing the provided numerical expression computed over the provided time range
        NOTE: it is not a safe guarantee that the number of buckets returned is the same as the number requested

    """
    # TODO: expose context parameterization
    context: dict[str, exprs.NumericExpr] = {}
    return [
        Bucket._from_conjure(ts, bucket)
        for ts, bucket in _compute_buckets(
            client._clients.compute,
            client._clients.auth_header,
            expr._to_conjure(),
            {k: v._to_conjure() for k, v in context.items()},
            _timestamp_to_conjure(start),
            _timestamp_to_conjure(end),
            buckets,
        )
    ]


def _compute_buckets(
    client: scout_compute_api.ComputeService,
    auth_token: str,
    node: scout_compute_api.NumericSeries,
    context: dict[str, scout_compute_api.NumericSeries],
    start: api.Timestamp,
    end: api.Timestamp,
    buckets: int,
) -> Iterable[tuple[api.Timestamp, scout_compute_api.NumericBucket]]:
    request = _create_compute_request_buckets(node, context, start, end, buckets)
    response = client.compute(auth_token, request)
    yield from _buckets_from_compute_response(response)


def _buckets_from_compute_response(
    response: scout_compute_api.ComputeNodeResponse,
) -> Iterable[tuple[api.Timestamp, scout_compute_api.NumericBucket]]:
    if response.type != "bucketedNumeric" or response.bucketed_numeric is None:
        return

    yield from zip(response.bucketed_numeric.timestamps, response.bucketed_numeric.buckets)


def _timestamp_from_conjure(timestamp: api.Timestamp) -> params.NanosecondsUTC:
    """Convert conjure Timestamp to nanoseconds UTC."""
    return timestamp.seconds * 1_000_000_000 + timestamp.nanos


def _timestamp_to_conjure(nanoseconds: params.NanosecondsUTC) -> api.Timestamp:
    """Convert nanoseconds UTC to conjure Timestamp."""
    seconds = nanoseconds // 1_000_000_000
    nanos = nanoseconds % 1_000_000_000
    return api.Timestamp(seconds=int(seconds), nanos=int(nanos))


def _create_compute_request_buckets(
    node: scout_compute_api.NumericSeries,
    context: dict[str, scout_compute_api.NumericSeries],
    start: api.Timestamp,
    end: api.Timestamp,
    buckets: int,
) -> scout_compute_api.ComputeNodeRequest:
    return scout_compute_api.ComputeNodeRequest(
        context=scout_compute_api.Context(
            variables={
                k: scout_compute_api.VariableValue(
                    compute_node=scout_compute_api.ComputeNodeWithContext(
                        context=scout_compute_api.Context(variables={}),
                        series_node=scout_compute_api.ComputeNode(numeric=v),
                    )
                )
                for k, v in context.items()
            }
        ),
        node=scout_compute_api.ComputableNode(
            series=scout_compute_api.SummarizeSeries(
                input=scout_compute_api.Series(
                    numeric=node,
                ),
                summarization_strategy=scout_compute_api.SummarizationStrategy(
                    decimate=scout_compute_api.DecimateStrategy(
                        buckets=scout_compute_api.DecimateWithBuckets(buckets=buckets)
                    )
                ),
            )
        ),
        start=start,
        end=end,
    )
