from __future__ import annotations

import typing

from nominal.protos.compute.v1 import (
    categorical_parameters_pb2,
    common_pb2,
    constants_pb2,
    expressions_pb2,
    numeric_reductions_pb2,
)
from nominal.protos.types.time import time_pb2
from nominal.ts import _SecondsNanos

Nanoseconds = int
NanosecondsUTC = int
DoubleConstant = float
TimeUnitLiteral = typing.Literal["ns", "us", "ms", "s", "m", "h", "d"]
ThresholdOperatorLiteral = typing.Literal[">", ">=", "<", "<=", "==", "!="]
RollingOperationLiteral = typing.Literal["mean", "sum", "min", "max", "count", "std"]
EnumUnionOperationLiteral = typing.Literal["min", "max", "throw"]


def _float_to_proto(value: DoubleConstant) -> constants_pb2.DoubleConstant:
    return constants_pb2.DoubleConstant(literal=value)


def _duration_ns_to_proto(duration: Nanoseconds) -> constants_pb2.DurationConstant:
    seconds, nanos = divmod(duration, 1_000_000_000)
    return constants_pb2.DurationConstant(literal=common_pb2.ComputeDuration(seconds=seconds, nanos=nanos))


def _timestamp_to_proto(timestamp: NanosecondsUTC) -> constants_pb2.TimestampConstant:
    value = _SecondsNanos.from_nanoseconds(timestamp)
    return constants_pb2.TimestampConstant(
        literal=common_pb2.ComputeTimestamp(seconds=value.seconds, nanos=value.nanos)
    )


def _time_unit_to_proto(time_unit: TimeUnitLiteral) -> time_pb2.TimeUnit.ValueType:
    return {
        "ns": time_pb2.NANOSECONDS,
        "us": time_pb2.MICROSECONDS,
        "ms": time_pb2.MILLISECONDS,
        "s": time_pb2.SECONDS,
        "m": time_pb2.MINUTES,
        "h": time_pb2.HOURS,
        "d": time_pb2.DAYS,
    }[time_unit]


def _threshold_operator_to_proto(operator: ThresholdOperatorLiteral) -> expressions_pb2.ComparisonOperation.ValueType:
    return {
        ">": expressions_pb2.COMPARISON_OPERATION_GREATER_THAN,
        ">=": expressions_pb2.COMPARISON_OPERATION_GREATER_THAN_OR_EQUAL_TO,
        "<": expressions_pb2.COMPARISON_OPERATION_LESS_THAN,
        "<=": expressions_pb2.COMPARISON_OPERATION_LESS_THAN_OR_EQUAL_TO,
        "==": expressions_pb2.COMPARISON_OPERATION_EQUAL_TO,
        "!=": expressions_pb2.COMPARISON_OPERATION_NOT_EQUAL_TO,
    }[operator]


def _rolling_operation_to_proto(operator: RollingOperationLiteral) -> numeric_reductions_pb2.NumericReduction:
    return {
        "mean": numeric_reductions_pb2.NumericReduction(average=numeric_reductions_pb2.AverageReduction()),
        "sum": numeric_reductions_pb2.NumericReduction(sum=numeric_reductions_pb2.SumReduction()),
        "min": numeric_reductions_pb2.NumericReduction(min=numeric_reductions_pb2.MinReduction()),
        "max": numeric_reductions_pb2.NumericReduction(max=numeric_reductions_pb2.MaxReduction()),
        "count": numeric_reductions_pb2.NumericReduction(count=numeric_reductions_pb2.CountReduction()),
        "std": numeric_reductions_pb2.NumericReduction(
            standard_deviation=numeric_reductions_pb2.StandardDeviationReduction()
        ),
    }[operator]


def _enum_union_operation_to_proto(
    operator: EnumUnionOperationLiteral,
) -> categorical_parameters_pb2.CategoricalMergeOperation.ValueType:
    return {
        "throw": categorical_parameters_pb2.CATEGORICAL_MERGE_OPERATION_REJECT_DUPLICATES,
        "min": categorical_parameters_pb2.CATEGORICAL_MERGE_OPERATION_MIN,
        "max": categorical_parameters_pb2.CATEGORICAL_MERGE_OPERATION_MAX,
    }[operator]
