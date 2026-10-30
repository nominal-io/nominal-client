from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from nominal.core import NominalClient
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.exceptions import NominalComputeError
from nominal.experimental.compute.dsl import exprs as _exprs
from nominal.experimental.compute.dsl import params
from nominal.protos.compute.v1 import (
    categorical_results_pb2,
    computation_spec_pb2,
    compute_pb2,
    execution_inputs_pb2,
    execution_options_pb2,
    execution_results_pb2,
    expressions_pb2,
    numeric_results_pb2,
    result_common_pb2,
)
from nominal.ts import _SecondsNanos


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
    def _from_proto(cls, bucket: numeric_results_pb2.NumericResultBucket) -> Bucket:
        statistics = bucket.statistics
        if not (
            statistics.HasField("min")
            and statistics.HasField("max")
            and statistics.HasField("mean")
            and statistics.HasField("variance")
            and statistics.HasField("count")
        ):
            raise NominalComputeError(f"NumericBucket returned incomplete data: {statistics}")
        return cls(
            _SecondsNanos.from_proto(bucket.end).to_nanoseconds(),
            statistics.min,
            statistics.max,
            statistics.mean,
            statistics.variance,
            statistics.count,
        )


@dataclass(frozen=True)
class EnumPoint:
    """Timestamped data point for a enum series within Nominal"""

    timestamp: params.NanosecondsUTC
    value: str

    @classmethod
    def _from_proto(cls, point: categorical_results_pb2.CategoricalCodeSample, categories: Sequence[str]) -> EnumPoint:
        return cls(_SecondsNanos.from_proto(point.timestamp).to_nanoseconds(), categories[point.category_index])


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
    def _from_proto(
        cls, bucket: categorical_results_pb2.CategoricalResultBucket, categories: Sequence[str]
    ) -> EnumBucket:
        statistics = bucket.statistics
        return cls(
            _SecondsNanos.from_proto(bucket.end).to_nanoseconds(),
            {categories[k]: v for k, v in statistics.count_by_category_index.items()},
            EnumPoint._from_proto(statistics.first, categories),
            EnumPoint._from_proto(statistics.last, categories) if statistics.HasField("last") else None,
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
        Decimated data representing the provided numerical expression computed over the provided time range.

    Note:
        It is not a safe guarantee that the number of buckets returned is the same as
        the number requested.

    """
    request = compute_pb2.ComputeRequest(input=_create_compute_request_buckets(expr, start, end, buckets))
    with translate_grpc_errors():
        response = client._clients.compute.Compute(request)
    return _numeric_buckets_from_compute_response(response.result)


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
        Decimated data representing the provided numerical expression computed over the provided time range.

    Note:
        It is not a safe guarantee that the number of buckets returned is the same as
        the number requested.
    """
    request = compute_pb2.ComputeRequest(input=_create_compute_request_buckets(expr, start, end, buckets))
    with translate_grpc_errors():
        response = client._clients.compute.Compute(request)
    return _enum_buckets_from_compute_response(response.result)


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
        inner sequences correspond to the individual buckets for each input expression. The order of buckets
        returned matches the order of expressions provided.

    Raises:
        ExceptionGroup: One or more expressions failed to compute; each failure is a `RuntimeError` in
            ``exceptions``.

    Note:
        It is not a safe guarantee that the number of buckets returned is the same as
        the number requested.
    """
    request = compute_pb2.BatchComputeWithUnitsRequest(
        inputs=[_create_compute_request_buckets(expr, start, end, buckets) for expr in exprs]
    )
    if not request.inputs:
        return []
    with translate_grpc_errors():
        response = client._clients.compute.BatchComputeWithUnits(request)
    return [_enum_buckets_from_compute_response(result) for result in _batch_results(response)]


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
        inner sequences correspond to the individual buckets for each input expression. The order of buckets
        returned matches the order of expressions provided.

    Raises:
        ExceptionGroup: One or more expressions failed to compute; each failure is a `RuntimeError` in
            ``exceptions``.

    Note:
        It is not a safe guarantee that the number of buckets returned is the same as
        the number requested.
    """
    request = compute_pb2.BatchComputeWithUnitsRequest(
        inputs=[_create_compute_request_buckets(expr, start, end, buckets) for expr in exprs]
    )
    if not request.inputs:
        return []
    with translate_grpc_errors():
        response = client._clients.compute.BatchComputeWithUnits(request)
    return [_numeric_buckets_from_compute_response(result) for result in _batch_results(response)]


def _create_compute_request_buckets(
    expr: _exprs.NumericExpr | _exprs.EnumExpr, start: params.NanosecondsUTC, end: params.NanosecondsUTC, buckets: int
) -> execution_inputs_pb2.ExecutionInput:
    series = (
        expressions_pb2.SeriesExpression(numeric=expr._to_proto())
        if isinstance(expr, _exprs.NumericExpr)
        else expressions_pb2.SeriesExpression(categorical=expr._to_proto())
    )
    return execution_inputs_pb2.ExecutionInput(
        computation=computation_spec_pb2.ComputationSpec(
            series_summary=computation_spec_pb2.SeriesSummarySpec(
                input=series,
                summarization=execution_options_pb2.SeriesSummarization(
                    bucket_count=execution_options_pb2.BucketCountSummary(bucket_count=buckets)
                ),
            )
        ),
        context=expressions_pb2.ComputeContext(),
        start=_SecondsNanos.from_nanoseconds(start).to_proto(),
        end=_SecondsNanos.from_nanoseconds(end).to_proto(),
    )


def _batch_results(
    response: compute_pb2.BatchComputeWithUnitsResponse,
) -> list[execution_results_pb2.ComputationResult]:
    results = []
    errors: list[Exception] = []
    for result in response.results:
        assert result.HasField("outcome")
        outcome = result.outcome
        if outcome.HasField("failure"):
            errors.append(
                RuntimeError(f"Failed to compute: {outcome.failure.error_type} ({outcome.failure.http_status_code})")
            )
        elif outcome.HasField("success"):
            results.append(outcome.success)
    if errors:
        raise ExceptionGroup("Failed to compute batches", errors)
    return results


def _single_point_bucket(point: result_common_pb2.NumericSample) -> Bucket:
    return Bucket(
        _SecondsNanos.from_proto(point.timestamp).to_nanoseconds(), point.value, point.value, point.value, 0, 1
    )


def _numeric_buckets_from_compute_response(response: execution_results_pb2.ComputationResult) -> Sequence[Bucket]:
    if response.HasField("numeric_sample"):
        return (
            [_single_point_bucket(response.numeric_sample.sample)] if response.numeric_sample.HasField("sample") else []
        )
    if response.HasField("numeric_samples"):
        return [_single_point_bucket(point) for point in response.numeric_samples.samples]
    if response.HasField("numeric_buckets"):
        return [Bucket._from_proto(bucket) for bucket in response.numeric_buckets.buckets]
    return []


def _enum_buckets_from_compute_response(response: execution_results_pb2.ComputationResult) -> Sequence[EnumBucket]:
    if response.HasField("categorical_buckets"):
        return [
            EnumBucket._from_proto(bucket, response.categorical_buckets.categories)
            for bucket in response.categorical_buckets.buckets
        ]
    if response.HasField("categorical_samples"):
        buckets = []
        for sample in response.categorical_samples.samples:
            point = EnumPoint._from_proto(sample, response.categorical_samples.categories)
            buckets.append(EnumBucket(point.timestamp, {point.value: 1}, point, None))
        return buckets
    raise ValueError(f"Expected response to be bucketed enum, got {response.WhichOneof('kind')}")
