from unittest.mock import MagicMock

import pytest

from nominal.exceptions import NominalComputeError
from nominal.experimental.compute import Bucket, compute_buckets
from nominal.experimental.compute._buckets import EnumBucket, EnumPoint, compute_enum_buckets
from nominal.experimental.compute.dsl import NumericExpr
from nominal.experimental.compute.dsl.exprs import EnumExpr
from nominal.protos.compute.v1 import (
    categorical_results_pb2,
    compute_pb2,
    execution_results_pb2,
    numeric_results_pb2,
    result_common_pb2,
)
from nominal.protos.types.time import time_pb2


def test_numeric_samples_preserve_nanoseconds_and_synthetic_statistics():
    """Undecimated samples retain nanoseconds and become single-point buckets."""
    client = MagicMock()
    client._clients.compute.Compute.return_value = compute_pb2.ComputeResponse(
        result=execution_results_pb2.ComputationResult(
            numeric_samples=numeric_results_pb2.NumericSamples(
                samples=[
                    result_common_pb2.NumericSample(timestamp=time_pb2.Timestamp(seconds=-1, nanos=999999999), value=3)
                ]
            )
        )
    )
    expression = NumericExpr.datasource_channel("dataset", "speed", {"axis": "x"}).scale(2).derivative("ms")
    assert compute_buckets(client, expression, -1, 1000000001, 10) == [Bucket(-1, 3, 3, 3, 0, 1)]
    request = client._clients.compute.Compute.call_args.args[0]
    assert request.input.start == time_pb2.Timestamp(seconds=-1, nanos=999999999)
    assert request.input.end == time_pb2.Timestamp(seconds=1, nanos=1)
    assert request.input.computation.series_summary.summarization.bucket_count.bucket_count == 10
    derivative = request.input.computation.series_summary.input.numeric.derivative
    assert derivative.time_unit == time_pb2.MILLISECONDS
    assert derivative.input.scalar_transform.scale.value.literal == 2
    channel = derivative.input.scalar_transform.input.channel_selection.data_source
    assert channel.data_source.literal == "dataset"
    assert channel.tags.exact_values.values["axis"].literal == "x"


def test_enum_buckets_preserve_category_indexes_and_optional_last_point():
    """Enum decoding uses category indexes and distinguishes an absent last point."""
    client = MagicMock()
    client._clients.compute.Compute.return_value = compute_pb2.ComputeResponse(
        result=execution_results_pb2.ComputationResult(
            categorical_buckets=categorical_results_pb2.CategoricalBuckets(
                categories=["off", "on"],
                buckets=[
                    categorical_results_pb2.CategoricalResultBucket(
                        end=time_pb2.Timestamp(nanos=8),
                        statistics=categorical_results_pb2.CategoricalBucketStatistics(
                            count_by_category_index={1: 3, 0: 2},
                            first=categorical_results_pb2.CategoricalCodeSample(
                                timestamp=time_pb2.Timestamp(nanos=1), category_index=1
                            ),
                        ),
                    )
                ],
            )
        )
    )
    expression = EnumExpr.run_channel("run", "scope", "state").shift(-1).resample(5)
    assert compute_enum_buckets(client, expression, 0, 10) == [
        EnumBucket(8, {"on": 3, "off": 2}, EnumPoint(1, "on"), None)
    ]
    series = client._clients.compute.Compute.call_args.args[0].input.computation.series_summary.input.categorical
    assert series.resample.interval.literal.nanos == 5
    assert series.resample.fill.HasField("forward_fill")
    assert series.resample.input.time_shift.duration.literal.seconds == -1
    assert series.resample.input.time_shift.duration.literal.nanos == 999999999


def test_numeric_bucket_rejects_absent_statistics_but_accepts_zero():
    """Proto presence checks keep valid zero statistics and reject omitted values."""
    client = MagicMock()
    statistics = numeric_results_pb2.NumericBucketStatistics(min=0, max=0, mean=0, variance=0, count=0)
    client._clients.compute.Compute.return_value = compute_pb2.ComputeResponse(
        result=execution_results_pb2.ComputationResult(
            numeric_buckets=numeric_results_pb2.NumericBuckets(
                buckets=[numeric_results_pb2.NumericResultBucket(end=time_pb2.Timestamp(), statistics=statistics)]
            )
        )
    )
    expr = NumericExpr.datasource_channel("dataset", "value")
    assert compute_buckets(client, expr, 0, 1) == [Bucket(0, 0, 0, 0, 0, 0)]
    client._clients.compute.Compute.return_value.result.numeric_buckets.buckets[0].statistics.ClearField("count")
    with pytest.raises(NominalComputeError, match="incomplete data"):
        compute_buckets(client, expr, 0, 1)


def test_nested_dsl_preserves_mapping_bounds_threshold_and_rolling_unit():
    """Nested conversions retain open bounds, zero constants, ranges, and duration precision."""
    expr = NumericExpr.asset_channel("asset", "scope", "value")
    mapped = (
        expr.filter(expr.threshold(0, ">=").invert())
        .rolling(1000000001, "std")
        .value_map({(None, 0): "negative", (0, None): "positive"}, "")
        ._to_proto()
    )
    mapping = mapped.numeric_mapping
    assert not mapping.mapping[0].HasField("start")
    assert mapping.mapping[0].end.HasField("literal")
    assert mapping.default_value.HasField("literal")
    assert mapping.input.rolling_aggregation.window.literal.seconds == 1
    assert mapping.input.rolling_aggregation.window.literal.nanos == 1
    assert mapping.input.rolling_aggregation.reduction.HasField("standard_deviation")
    assert (
        mapping.input.rolling_aggregation.input.range_filter.ranges.complement.numeric_threshold.threshold.literal == 0
    )


@pytest.mark.parametrize("expression_type", [NumericExpr, EnumExpr])
def test_untagged_dsl_channels_omit_tag_constraints(expression_type):
    """Untagged DSL channels omit tag constraints instead of sending an invalid empty map."""
    asset = expression_type.asset_channel("asset", "scope", "value")._to_proto().channel_selection.asset
    dataset = expression_type.datasource_channel("dataset", "value", {})._to_proto().channel_selection.data_source
    run = expression_type.run_channel("run", "scope", "value", {})._to_proto().channel_selection.run
    assert not asset.HasField("tags")
    assert not dataset.HasField("tags")
    assert not run.HasField("tags")
