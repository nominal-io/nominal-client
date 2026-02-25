from __future__ import annotations

import typing

from nominal_api import api, scout_compute_api, scout_run_api

Nanoseconds = int
NanosecondsUTC = int
DoubleConstant = float
TimeUnitLiteral = typing.Literal["ns", "us", "ms", "s", "m", "h", "d"]
ThresholdOperatorLiteral = typing.Literal[">", ">=", "<", "<=", "==", "!="]
RollingOperationLiteral = typing.Literal["mean", "sum", "min", "max", "count", "std"]
EnumUnionOperationLiteral = typing.Literal["min", "max", "throw"]


def _float_to_conjure(value: DoubleConstant) -> scout_compute_api.DoubleConstant:
    return scout_compute_api.DoubleConstant(literal=value)


def _window_to_conjure(window: Nanoseconds) -> scout_compute_api.Window:
    seconds, nanos = divmod(window, 1_000_000_000)
    return scout_compute_api.Window(duration=_duration_ns_to_conjure(window))


def _duration_ns_to_conjure(duration: Nanoseconds) -> scout_compute_api.DurationConstant:
    seconds, nanos = divmod(duration, 1_000_000_000)
    return scout_compute_api.DurationConstant(literal=scout_run_api.Duration(seconds=seconds, nanos=nanos))


def _timestamp_to_conjure(timestamp: NanosecondsUTC) -> scout_compute_api.TimestampConstant:
    seconds, nanos = divmod(timestamp, 1_000_000_000)
    return scout_compute_api.TimestampConstant(literal=api.Timestamp(seconds=seconds, nanos=nanos))


def _time_unit_to_conjure(time_unit: TimeUnitLiteral) -> api.TimeUnit:
    mapping = {
        "ns": api.TimeUnit.NANOSECONDS,
        "us": api.TimeUnit.MICROSECONDS,
        "ms": api.TimeUnit.MILLISECONDS,
        "s": api.TimeUnit.SECONDS,
        "m": api.TimeUnit.MINUTES,
        "h": api.TimeUnit.HOURS,
        "d": api.TimeUnit.DAYS,
    }
    return mapping[time_unit]


def _threshold_operator_to_conjure(operator: ThresholdOperatorLiteral) -> scout_compute_api.ThresholdOperator:
    mapping = {
        ">": scout_compute_api.ThresholdOperator.GREATER_THAN,
        ">=": scout_compute_api.ThresholdOperator.GREATER_THAN_OR_EQUAL_TO,
        "<": scout_compute_api.ThresholdOperator.LESS_THAN,
        "<=": scout_compute_api.ThresholdOperator.LESS_THAN_OR_EQUAL_TO,
        "==": scout_compute_api.ThresholdOperator.EQUAL_TO,
        "!=": scout_compute_api.ThresholdOperator.NOT_EQUAL_TO,
    }
    return mapping[operator]


def _rolling_operation_to_conjure(operator: RollingOperationLiteral) -> scout_compute_api.RollingOperator:
    mapping = {
        "mean": scout_compute_api.RollingOperator(average=scout_compute_api.Average()),
        "sum": scout_compute_api.RollingOperator(sum=scout_compute_api.Sum()),
        "min": scout_compute_api.RollingOperator(min=scout_compute_api.Minimum()),
        "max": scout_compute_api.RollingOperator(max=scout_compute_api.Maximum()),
        "count": scout_compute_api.RollingOperator(count=scout_compute_api.Count()),
        "std": scout_compute_api.RollingOperator(standard_deviation=scout_compute_api.StandardDeviation()),
    }
    return mapping[operator]


def _enum_union_operation_to_conjure(operator: EnumUnionOperationLiteral) -> scout_compute_api.EnumUnionOperation:
    mapping = {
        "throw": scout_compute_api.EnumUnionOperation.THROW,
        "min": scout_compute_api.EnumUnionOperation.MIN,
        "max": scout_compute_api.EnumUnionOperation.MAX,
    }
    return mapping[operator]
