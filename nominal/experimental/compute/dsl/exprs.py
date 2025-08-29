from __future__ import annotations

import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass

from nominal_api import scout_compute_api

from nominal.experimental.compute.dsl import params


@dataclass(frozen=True)
class Expr(ABC):
    """Base class for all compute expressions."""


@dataclass(frozen=True)
class NumericExpr(Expr):
    @abstractmethod
    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        raise NotImplementedError()

    @classmethod
    def datasource_channel(
        cls, datasource_rid: str, channel_name: str, tags: typing.Mapping[str, str] | None = None
    ) -> NumericExpr:
        return _expr_impls.NumericDatasourceChannelExpr(datasource_rid, channel_name, {**tags} if tags else {})

    @classmethod
    def asset_channel(
        cls,
        asset_rid: str,
        data_scope_name: str,
        channel_name: str,
        additional_tags: typing.Mapping[str, str] | None = None,
    ) -> NumericExpr:
        return _expr_impls.NumericAssetChannelExpr(
            asset_rid, data_scope_name, channel_name, {**additional_tags} if additional_tags else {}
        )

    @classmethod
    def run_channel(
        cls,
        run_rid: str,
        data_scope_name: str,
        channel_name: str,
        additional_tags: typing.Mapping[str, str] | None = None,
    ) -> NumericExpr:
        return _expr_impls.NumericRunChannelExpr(
            run_rid, data_scope_name, channel_name, {**additional_tags} if additional_tags else {}
        )

    def abs(self, /) -> NumericExpr:
        return _expr_impls.AbsExpr(_node=self)

    def acos(self, /) -> NumericExpr:
        return _expr_impls.AcosExpr(_node=self)

    def asin(self, /) -> NumericExpr:
        return _expr_impls.AsinExpr(_node=self)

    def atan2(self, x_node: NumericExpr, /) -> NumericExpr:
        return _expr_impls.Atan2Expr(_y_node=self, _x_node=x_node)

    def cos(self, /) -> NumericExpr:
        return _expr_impls.CosExpr(_node=self)

    def cumulative_sum(self, /, start_timestamp: params.NanosecondsUTC) -> NumericExpr:
        return _expr_impls.CumulativeSumExpr(_node=self, _start_timestamp=start_timestamp)

    def derivative(self, /, time_unit: params.TimeUnitLiteral) -> NumericExpr:
        return _expr_impls.DerivativeExpr(_node=self, _time_unit=time_unit)

    def divide(self, right_node: NumericExpr, /) -> NumericExpr:
        return _expr_impls.DivideExpr(_left_node=self, _right_node=right_node)

    def filter(self, ranges: RangeExpr, /) -> NumericExpr:
        return _expr_impls.FilterExpr(_node=self, _ranges=ranges)

    def floor_divide(self, right_node: NumericExpr, /) -> NumericExpr:
        return _expr_impls.FloorDivideExpr(_left_node=self, _right_node=right_node)

    def integral(self, /, start_timestamp: params.NanosecondsUTC, time_unit: params.TimeUnitLiteral) -> NumericExpr:
        return _expr_impls.IntegralExpr(_node=self, _start_timestamp=start_timestamp, _time_unit=time_unit)

    def ln(self, /) -> NumericExpr:
        return _expr_impls.LnExpr(_node=self)

    def log(self, /) -> NumericExpr:
        return _expr_impls.LogarithmExpr(_node=self)

    def max(self, nodes: typing.Sequence[NumericExpr], /) -> NumericExpr:
        return _expr_impls.MaxExpr(_nodes=[self, *nodes])

    def mean(self, nodes: typing.Sequence[NumericExpr], /) -> NumericExpr:
        return _expr_impls.MeanExpr(_nodes=[self, *nodes])

    def min(self, nodes: typing.Sequence[NumericExpr], /) -> NumericExpr:
        return _expr_impls.MinExpr(_nodes=[self, *nodes])

    def minus(self, right_node: NumericExpr, /) -> NumericExpr:
        return _expr_impls.MinusExpr(_left_node=self, _right_node=right_node)

    def modulo(self, right_node: NumericExpr, /) -> NumericExpr:
        return _expr_impls.ModuloExpr(_left_node=self, _right_node=right_node)

    def multiply(self, right_node: NumericExpr, /) -> NumericExpr:
        return _expr_impls.MultiplyExpr(_left_node=self, _right_node=right_node)

    def offset(self, /, offset: params.DoubleConstant) -> NumericExpr:
        return _expr_impls.OffsetExpr(_node=self, _offset=offset)

    def plus(self, right_node: NumericExpr, /) -> NumericExpr:
        return _expr_impls.PlusExpr(_left_node=self, _right_node=right_node)

    def power(self, exponent_node: NumericExpr, /) -> NumericExpr:
        return _expr_impls.PowerExpr(_base_node=self, _exponent_node=exponent_node)

    def product(self, nodes: typing.Sequence[NumericExpr], /) -> NumericExpr:
        return _expr_impls.ProductExpr(_inputs=[self, *nodes])

    def rolling(self, /, window: params.Nanoseconds, operator: params.RollingOperationLiteral) -> NumericExpr:
        return _expr_impls.RollingExpr(_node=self, _window=window, _operator=operator)

    def scale(self, /, scalar: params.DoubleConstant) -> NumericExpr:
        return _expr_impls.ScaleExpr(_node=self, _scalar=scalar)

    def sin(self, /) -> NumericExpr:
        return _expr_impls.SinExpr(_node=self)

    def sqrt(self, /) -> NumericExpr:
        return _expr_impls.SqrtExpr(_node=self)

    def sum(self, nodes: typing.Sequence[NumericExpr], /) -> NumericExpr:
        return _expr_impls.SumExpr(_nodes=[self, *nodes])

    def tan(self, /) -> NumericExpr:
        return _expr_impls.TanExpr(_node=self)

    def time_difference(self, /, time_unit: params.TimeUnitLiteral | None = None) -> NumericExpr:
        return _expr_impls.TimeDifferenceExpr(_node=self, _time_unit=time_unit)

    def threshold(self, /, threshold: params.DoubleConstant, operator: params.ThresholdOperatorLiteral) -> RangeExpr:
        return _expr_impls.ThresholdExpr(_node=self, _threshold=threshold, _operator=operator)

    def value_difference(self, /) -> NumericExpr:
        return _expr_impls.ValueDifferenceExpr(_node=self)

    # Magic methods for operators
    __abs__ = abs
    __add__ = plus
    __floordiv__ = floor_divide
    __mod__ = modulo
    __mul__ = multiply
    __pow__ = power
    __sub__ = minus
    __truediv__ = divide


@dataclass(frozen=True)
class RangeExpr(Expr):
    @abstractmethod
    def _to_conjure(self) -> scout_compute_api.RangeSeries:
        raise NotImplementedError()

    def intersect(self, ranges: typing.Sequence[RangeExpr], /) -> RangeExpr:
        return _expr_impls.IntersectRangesExpr(_nodes=[self, *ranges])

    def invert(self, /) -> RangeExpr:
        return _expr_impls.InvertRangesExpr(_nodes=self)

    def union(self, ranges: typing.Sequence[RangeExpr], /) -> RangeExpr:
        return _expr_impls.UnionRangesExpr(_nodes=[self, *ranges])


# imported at the end to prevent circular references
from nominal.experimental.compute.dsl import _expr_impls  # noqa: E402
