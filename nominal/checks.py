from __future__ import annotations
from dataclasses import dataclass, field
from datetime import timedelta
import enum
from typing import Any


class TimestampAggregate(enum.Enum):
    SUM = enum.auto()
    MAX = enum.auto()
    MIN = enum.auto()
    MEAN = enum.auto()


class RollingAggregate(enum.Enum):
    AVERAGE = enum.auto()
    COUNT = enum.auto()
    SUM = enum.auto()
    MAXIMUM = enum.auto()
    MINIMUM = enum.auto()
    STANDARD_DEVIATION = enum.auto()


class CumulativeAggregate(enum.Enum):
    SUM = enum.auto()


class TimeUnit(enum.Enum):
    HOURS = enum.auto()
    MINUTES = enum.auto()
    SECONDS = enum.auto()
    MILLISECONDS = enum.auto()
    MICROSECONDS = enum.auto()
    NANOSECONDS = enum.auto()


class DerivativeHandleNegatives(enum.Enum):
    ALLOW = enum.auto()
    TRUNCATE = enum.auto()
    REMOVE = enum.auto()


class Operation:
    pass


@dataclass
class Resample(Operation):
    interval: timedelta


@dataclass
class DeduplicateTimestamps(Operation):
    operation: TimestampAggregate


@dataclass
class RollingAggregate(Operation):
    operation: RollingAggregate
    window: timedelta


@dataclass
class CumulativeAggregate(Operation):
    operation: CumulativeAggregate


@dataclass
class TimeShift(Operation):
    duration: timedelta


@dataclass
class Scale(Operation):
    scalar: int | float


@dataclass
class Offset(Operation):
    scalar: int | float


@dataclass
class Tangent(Operation):
    pass


@dataclass
class Cosine(Operation):
    pass


@dataclass
class AbsoluteValue(Operation):
    pass


@dataclass
class Sine(Operation):
    pass


@dataclass
class Arcsin(Operation):
    pass


@dataclass
class Arccos(Operation):
    pass


@dataclass
class Arctan2(Operation):
    pass


@dataclass
class Derivative(Operation):
    time_unit: TimeUnit
    handle_negatives: DerivativeHandleNegatives


@dataclass
class Integral(Operation):
    time_unit: TimeUnit


@dataclass
class ValueDifference(Operation):
    pass


@dataclass
class TimeDifference(Operation):
    time_unit: TimeUnit


@dataclass
class BitwiseAnd(Operation):
    mask: int


@dataclass
class BitwiseOr(Operation):
    mask: int


@dataclass
class BitwiseXor(Operation):
    mask: int


@dataclass
class GetNthBit(Operation):
    bit_index: int


@dataclass
class UnitConversion(Operation):
    output_unit: Any


@dataclass
class Transform:
    operations: list[Operation] = field(default_factory=list)

    def resample(self, interval: timedelta) -> Transform:
        self.operations.append(Resample(interval))

    def deduplicate_timestamps(self, operation: TimestampAggregate) -> Transform:
        self.operations.append(DeduplicateTimestamps(operation))

    def rolling_aggregate(self, operation: RollingAggregate, window: timedelta) -> Transform:
        self.operations.append(RollingAggregate(operation, window))

    def cumulative_aggregate(self, operation: CumulativeAggregate) -> Transform:
        self.operations.append(CumulativeAggregate(operation))

    def time_shift(self, duration: timedelta) -> Transform:
        self.operations.append(TimeShift(duration))

    def scale(self, scalar: int | float) -> Transform:
        self.operations.append(Scale(scalar))

    def offset(self, scalar: int | float) -> Transform:
        self.operations.append(Offset(scalar))

    def tangent(self) -> Transform:
        self.operations.append(Tangent())

    def cosine(self) -> Transform:
        self.operations.append(Cosine())

    def absolute_value(self) -> Transform:
        self.operations.append(AbsoluteValue())

    def sine(self) -> Transform:
        self.operations.append(Sine())

    def arcsin(self) -> Transform:
        self.operations.append(Arcsin())

    def arccos(self) -> Transform:
        self.operations.append(Arccos())

    def arctan2(self) -> Transform:
        self.operations.append(Arctan2())

    def derivative(self, time_unit: TimeUnit, handle_negatives: DerivativeHandleNegatives) -> Transform:
        self.operations.append(Derivative(time_unit, handle_negatives))

    def integral(self, time_unit: TimeUnit) -> Transform:
        self.operations.append(Integral(time_unit))

    def value_difference(self) -> Transform:
        self.operations.append(ValueDifference())

    def time_difference(self, time_unit: TimeUnit) -> Transform:
        self.operations.append(TimeDifference(time_unit))

    def bitwise_and(self, mask: int) -> Transform:
        self.operations.append(BitwiseAnd(mask))

    def bitwise_or(self, mask: int) -> Transform:
        self.operations.append(BitwiseOr(mask))

    def bitwise_xor(self, mask: int) -> Transform:
        self.operations.append(BitwiseXor(mask))

    def get_nth_bit(self, bit_index: int) -> Transform:
        self.operations.append(GetNthBit(bit_index))

    def unit_conversion(self, output_unit: Any) -> Transform:
        self.operations.append(UnitConversion(output_unit))


class ConditionalBinaryOperation(enum.Enum):
    LT = enum.auto()
    GT = enum.auto()
    LTE = enum.auto()
    GTE = enum.auto()
    EQ = enum.auto()
    NEQ = enum.auto()


class ConditionalUnaryOperation(enum.Enum):
    VALUE_CHANGE = enum.auto()
    VALUE_STALE = enum.auto()
    EXTREMA = enum.auto()
