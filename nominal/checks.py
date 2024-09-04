from __future__ import annotations
from dataclasses import dataclass
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


@dataclass
class Transform:
    def resample(self, interval: timedelta) -> Transform: ...
    def deduplicate_timestamps(self, operation: TimestampAggregate) -> Transform: ...
    def rolling_aggregate(self, operation: RollingAggregate, window: timedelta) -> Transform: ...
    def cumulative_aggregate(self, operation: CumulativeAggregate) -> Transform: ...
    def time_shift(self, duration: timedelta) -> Transform: ...
    def scale(self, scalar: int | float) -> Transform: ...
    def offset(self, scalar: int | float) -> Transform: ...
    def tangent(self) -> Transform: ...
    def cosine(self) -> Transform: ...
    def absolute_value(self) -> Transform: ...
    def sine(self) -> Transform: ...
    def arcsin(self) -> Transform: ...
    def arccos(self) -> Transform: ...
    def arctan2(self) -> Transform: ...
    def derivative(self, time_unit: TimeUnit, handle_negatives: DerivativeHandleNegatives) -> Transform: ...
    def integral(self, time_unit: TimeUnit) -> Transform: ...
    def value_difference(self) -> Transform: ...
    def time_difference(self, time_unit: TimeUnit) -> Transform: ...
    def bitwise_and(self, mask: int) -> Transform: ...
    def bitwise_or(self, mask: int) -> Transform: ...
    def bitwise_xor(self, mask: int) -> Transform: ...
    def get_nth_bit(self, bit_index: int) -> Transform: ...
    def unit_conversion(self, output_unit: Any) -> Transform: ...


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
