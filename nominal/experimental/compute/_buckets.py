from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from nominal_api import api, scout_compute_api

from nominal.core import NominalClient
from nominal.experimental.compute.dsl import exprs, params


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


def compute_buckets(
    client: NominalClient,
    node: exprs.NumericExpr,
    start: params.NanosecondsUTC,
    end: params.NanosecondsUTC,
    buckets: int = 1000,
) -> Iterable[Bucket]:
    # TODO: expose context parameterization
    context: dict[str, exprs.NumericExpr] = {}
    for ts, bucket in _compute_buckets(
        client._clients.compute,
        client._clients.auth_header,
        node._to_conjure(),
        {k: v._to_conjure() for k, v in context.items()},
        _timestamp_to_conjure(start),
        _timestamp_to_conjure(end),
        buckets,
    ):
        yield Bucket._from_conjure(ts, bucket)


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
    assert response.type == "bucketedNumeric"
    assert response.bucketed_numeric is not None
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
