from __future__ import annotations

import typing
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence, Union

from nominal_api import api, scout_compute_api
from typing_extensions import TypeAlias

from nominal.core import NominalClient
from nominal.experimental.compute.dsl import exprs as _exprs
from nominal.experimental.compute.dsl import params
from nominal.ts import _SecondsNanos


class ExceptionGroup(Exception):
    def __init__(self, message: str, exceptions: Sequence[Exception]):
        exc_str = "\n".join([str(ex) for ex in exceptions])
        super().__init__(f"{message}:\n{exc_str}")

        self.message = message
        self.exceptions = exceptions


@dataclass(frozen=True)
class Bucket:
    """Time-bucketed data for a numeric series within Nominal"""

    timestamp: params.NanosecondsUTC
    """Last timestamp of data within the bucket"""

    min: float
    """Minimum value of data within the bucket"""

    max: float
    """Maximum value of data within the bucket"""

    mean: float
    """Average value of data within the bucket"""

    variance: float
    """Variance of data within the bucket"""

    count: int
    """Number of samples decimated into this bucket"""

    @classmethod
    def _from_conjure(cls, timestamp: api.Timestamp, bucket: scout_compute_api.NumericBucket) -> Bucket:
        return cls(
            _timestamp_from_conjure(timestamp), bucket.min, bucket.max, bucket.mean, bucket.variance, bucket.count
        )


@dataclass(frozen=True)
class EnumPoint:
    """Timestamped data point for a enum series within Nominal"""

    timestamp: params.NanosecondsUTC
    value: str

    @classmethod
    def _from_conjure(
        cls,
        enum_point: scout_compute_api.CompactEnumPoint,
        enum_categories: typing.Sequence[str],
    ) -> EnumPoint:
        enum_value = enum_categories[enum_point.value]
        return cls(
            value=enum_value,
            timestamp=_SecondsNanos.from_api(enum_point.timestamp).to_nanoseconds(),
        )


@dataclass(frozen=True)
class EnumBucket:
    """Time-bucketed data for a enum series within Nominal"""

    timestamp: params.NanosecondsUTC
    """Last timestamp of data within the bucket"""

    frequencies: Mapping[str, int]
    """Unique enum values and their occurrence count within the bucket"""

    first_point: EnumPoint
    """First data point decimated into this bucket"""

    last_point: EnumPoint | None
    """Last data point decimated into this bucket"""

    @classmethod
    def _from_conjure(
        cls, timestamp: api.Timestamp, bucket: scout_compute_api.EnumBucket, categories: list[str]
    ) -> EnumBucket:
        return cls(
            _timestamp_from_conjure(timestamp),
            {categories[k]: v for k, v in bucket.histogram.items()},
            EnumPoint._from_conjure(bucket.first_point, categories),
            EnumPoint._from_conjure(bucket.last_point, categories) if bucket.last_point else None,
        )


def compute_buckets(
    client: NominalClient,
    expr: _exprs.NumericExpr,
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
    context: dict[str, _exprs.NumericExpr] = {}
    return [
        Bucket._from_conjure(ts, bucket)
        for ts, bucket in _compute_buckets(
            client=client._clients.compute,
            auth_token=client._clients.auth_header,
            node=expr._to_conjure(),
            context={k: v._to_conjure() for k, v in context.items()},
            start=_timestamp_to_conjure(start),
            end=_timestamp_to_conjure(end),
            buckets=buckets,
        )
    ]


def compute_enum_buckets(
    client: NominalClient,
    expr: _exprs.EnumExpr,
    start: params.NanosecondsUTC,
    end: params.NanosecondsUTC,
    buckets: int = 1000,
) -> Sequence[EnumBucket]:
    """Compute a bucketed summary of the requested enum expression.

    Args:
        client: Nominal client to make requests with
        expr: Expression to compute buckets for
        start: Starting timestamp to sumarize data from
        end: Ending timestamp to summarize data until
        buckets: Number of buckets to return

    Returns:
        Decimated data representing the provided numerical expression computed over the provided time range
        NOTE: it is not a safe guarantee that the number of buckets returned is the same as the number requested
    """
    request = _create_compute_request_buckets(
        expr._to_conjure(), {}, _timestamp_to_conjure(start), _timestamp_to_conjure(end), buckets
    )
    response = client._clients.compute.compute(client._clients.auth_header, request)
    return _enum_buckets_from_compute_response(response)


def _enum_buckets_from_compute_response(response: scout_compute_api.ComputeNodeResponse) -> Sequence[EnumBucket]:
    if response.bucketed_enum is not None:
        return [
            EnumBucket._from_conjure(ts, bucket, response.bucketed_enum.categories)
            for ts, bucket in zip(response.bucketed_enum.timestamps, response.bucketed_enum.buckets)
        ]
    elif response.enum is not None:
        # If the number of data points in range is smaller than the requested number of buckets,
        # the backend returns undecimated enum data
        buckets = []
        for ts, value in zip(response.enum.timestamps, response.enum.values):
            timestamp = _timestamp_from_conjure(ts)
            enum_value = response.enum.categories[value]
            buckets.append(EnumBucket(timestamp, {enum_value: 1}, EnumPoint(timestamp, enum_value), None))
        return buckets
    else:
        raise ValueError(f"Expected response to be bucketed enum, got {response.type}")


def batch_compute_enum_buckets(
    client: NominalClient,
    exprs: Iterable[_exprs.EnumExpr],
    start: params.NanosecondsUTC,
    end: params.NanosecondsUTC,
    buckets: int = 1000,
) -> Sequence[Sequence[EnumBucket]]:
    """Computed bucketed summaries for a batch of enum expressions

    Args:
        client: Nominal client to make requests with
        exprs: Expressions to compute buckets for
        start: Starting timestamp to summarize data from
        end: Ending timestamp to summarize data until
        buckets: Number of buckets to return per expression

    Returns:
        A Sequence of sequences of buckets. The top level sequence corresponds to the input expressions, whereas the
        inner sequences correspond to the individual buckets for each input expression. The order of buckets returned
        matches the order of expressions provided.
        NOTE: it is not a safe guarantee that the number of buckets returned is the same as the number requested
    """
    # Create request
    api_start = _timestamp_to_conjure(start)
    api_end = _timestamp_to_conjure(end)
    request = scout_compute_api.BatchComputeWithUnitsRequest(
        requests=[
            _create_compute_request_buckets(node._to_conjure(), {}, api_start, api_end, buckets) for node in exprs
        ]
    )

    # Make request
    resp = client._clients.compute.batch_compute_with_units(
        auth_header=client._clients.auth_header,
        request=request,
    )

    # Parse response
    results: list[Sequence[EnumBucket]] = []
    errors: list[Exception] = []
    for result in resp.results:
        compute_result = result.compute_result
        assert compute_result is not None

        compute_error = compute_result.error
        compute_response = compute_result.success
        if compute_error is not None:
            errors.append(RuntimeError(f"Failed to compute: {compute_error.error_type} ({compute_error.code})"))
        elif compute_response is not None:
            results.append(_enum_buckets_from_compute_response(compute_response))

    if errors:
        raise ExceptionGroup("Failed to compute batches", errors)

    return results


def batch_compute_buckets(
    client: NominalClient,
    exprs: Iterable[_exprs.NumericExpr],
    start: params.NanosecondsUTC,
    end: params.NanosecondsUTC,
    buckets: int = 1000,
) -> Sequence[Sequence[Bucket]]:
    """Computed bucketed summaries for a batch of numeric expressions

    Args:
        client: Nominal client to make requests with
        exprs: Expressions to compute buckets for
        start: Starting timestamp to summarize data from
        end: Ending timestamp to summarize data until
        buckets: Number of buckets to return per expression

    Returns:
        A Sequence of sequences of buckets. The top level sequence corresponds to the input expressions, whereas the
        inner sequences correspond to the individual buckets for each input expression. The order of buckets returned
        matches the order of expressions provided.
        NOTE: it is not a safe guarantee that the number of buckets returned is the same as the number requested
    """
    # Create request
    api_start = _timestamp_to_conjure(start)
    api_end = _timestamp_to_conjure(end)
    request = scout_compute_api.BatchComputeWithUnitsRequest(
        requests=[
            _create_compute_request_buckets(node._to_conjure(), {}, api_start, api_end, buckets) for node in exprs
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
                [
                    Bucket._from_conjure(ts, bucket)
                    for ts, bucket in _numeric_buckets_from_compute_response(compute_response)
                ]
            )

    if errors:
        raise ExceptionGroup("Failed to compute batches", errors)

    return results


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
    yield from _numeric_buckets_from_compute_response(response)


def _numeric_buckets_from_compute_response(
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


TypedSeriesT: TypeAlias = Union[scout_compute_api.NumericSeries, scout_compute_api.EnumSeries]


def _to_generic_series(typed_series: TypedSeriesT) -> scout_compute_api.Series:
    if isinstance(typed_series, scout_compute_api.NumericSeries):
        return scout_compute_api.Series(numeric=typed_series)
    elif isinstance(typed_series, scout_compute_api.EnumSeries):
        return scout_compute_api.Series(enum=typed_series)
    else:
        raise ValueError(f"Unexpected series type: {typed_series._type}")


def _to_compute_node(typed_series: TypedSeriesT) -> scout_compute_api.ComputeNode:
    if isinstance(typed_series, scout_compute_api.NumericSeries):
        return scout_compute_api.ComputeNode(numeric=typed_series)
    elif isinstance(typed_series, scout_compute_api.EnumSeries):
        return scout_compute_api.ComputeNode(enum=typed_series)
    else:
        raise ValueError(f"Unexpected series type: {typed_series._type}")


def _create_compute_request_buckets(
    node: TypedSeriesT,
    context: Mapping[str, TypedSeriesT],
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
                        series_node=_to_compute_node(v),
                    )
                )
                for k, v in context.items()
            }
        ),
        node=scout_compute_api.ComputableNode(
            series=scout_compute_api.SummarizeSeries(
                input=_to_generic_series(node),
                summarization_strategy=scout_compute_api.SummarizationStrategy(
                    decimate=scout_compute_api.DecimateStrategy(
                        buckets=scout_compute_api.DecimateWithBuckets(buckets=buckets)
                    )
                ),
                buckets=buckets,
            )
        ),
        start=start,
        end=end,
    )
