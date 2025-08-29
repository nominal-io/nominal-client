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


@dataclass(frozen=True)
class BatchNumericExpr(Expr):
    # Mapping of expression name => expression
    _numeric_exprs: dict[str, NumericExpr]

    def __getitem__(self, expression_name: str) -> NumericExpr:
        """Retrieve a numeric expression"""
        return self._numeric_exprs[expression_name]

    def __setitem__(self, expression_name: str, expression: NumericExpr) -> None:
        """Set a numeric expression as part of the batch"""
        self._numeric_exprs[expression_name] = expression

    def __delitem__(self, expression_name: str) -> None:
        """Delete a numeric expression in the batch"""
        del self._numeric_exprs[expression_name]

    def __contains__(self, expression_name: str) -> bool:
        """Check for existence of an expression by name"""
        return expression_name in self._numeric_exprs

    @classmethod
    def datasource_channels(
        cls,
        datasource_rid: str,
        channel_names: set[str],
        tags: typing.Mapping[str, str],
    ) -> BatchNumericExpr:
        return BatchNumericExpr(
            {
                channel_name: NumericExpr.datasource_channel(datasource_rid, channel_name, tags)
                for channel_name in channel_names
            }
        )

    @classmethod
    def asset_channels(
        cls,
        asset_rid: str,
        data_scope_name: str,
        channel_names: set[str],
        additional_tags: typing.Mapping[str, str] | None = None,
    ) -> BatchNumericExpr:
        return BatchNumericExpr(
            {
                channel_name: NumericExpr.asset_channel(asset_rid, data_scope_name, channel_name, additional_tags)
                for channel_name in channel_names
            }
        )

    @classmethod
    def run_channel(
        cls,
        run_rid: str,
        data_scope_name: str,
        channel_names: set[str],
        additional_tags: typing.Mapping[str, str] | None = None,
    ) -> BatchNumericExpr:
        return BatchNumericExpr(
            {
                channel_name: NumericExpr.run_channel(run_rid, data_scope_name, channel_name, additional_tags)
                for channel_name in channel_names
            }
        )

    def _to_conjure(self) -> list[scout_compute_api.NumericSeries]:
        return [ex._to_conjure() for ex in self._numeric_exprs.values()]

    def abs(self, /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.abs() for name, ex in self._numeric_exprs.items()})

    def acos(self, /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.acos() for name, ex in self._numeric_exprs.items()})

    def asin(self, /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.asin() for name, ex in self._numeric_exprs.items()})

    def atan2(self, x_node: NumericExpr, /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.atan2(x_node) for name, ex in self._numeric_exprs.items()})

    def cos(self, /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.cos() for name, ex in self._numeric_exprs.items()})

    def cumulative_sum(self, /, start_timestamp: params.NanosecondsUTC) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.cumulative_sum(start_timestamp) for name, ex in self._numeric_exprs.items()})

    def derivative(self, /, time_unit: params.TimeUnitLiteral) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.derivative(time_unit) for name, ex in self._numeric_exprs.items()})

    def divide(self, right_node: NumericExpr, /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.divide(right_node) for name, ex in self._numeric_exprs.items()})

    def filter(self, ranges: RangeExpr, /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.filter(ranges) for name, ex in self._numeric_exprs.items()})

    def floor_divide(self, right_node: NumericExpr, /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.floor_divide(right_node) for name, ex in self._numeric_exprs.items()})

    def integral(
        self, /, start_timestamp: params.NanosecondsUTC, time_unit: params.TimeUnitLiteral
    ) -> BatchNumericExpr:
        return BatchNumericExpr(
            {name: ex.integral(start_timestamp, time_unit) for name, ex in self._numeric_exprs.items()}
        )

    def ln(self, /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.ln() for name, ex in self._numeric_exprs.items()})

    def log(self, /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.log() for name, ex in self._numeric_exprs.items()})

    def max(self, nodes: typing.Sequence[NumericExpr], /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.max(nodes) for name, ex in self._numeric_exprs.items()})

    def mean(self, nodes: typing.Sequence[NumericExpr], /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.mean(nodes) for name, ex in self._numeric_exprs.items()})

    def min(self, nodes: typing.Sequence[NumericExpr], /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.min(nodes) for name, ex in self._numeric_exprs.items()})

    def minus(self, right_node: NumericExpr, /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.minus(right_node) for name, ex in self._numeric_exprs.items()})

    def modulo(self, right_node: NumericExpr, /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.modulo(right_node) for name, ex in self._numeric_exprs.items()})

    def multiply(self, right_node: NumericExpr, /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.multiply(right_node) for name, ex in self._numeric_exprs.items()})

    def offset(self, /, offset: params.DoubleConstant) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.offset(offset) for name, ex in self._numeric_exprs.items()})

    def plus(self, right_node: NumericExpr, /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.plus(right_node) for name, ex in self._numeric_exprs.items()})

    def power(self, exponent_node: NumericExpr, /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.power(exponent_node) for name, ex in self._numeric_exprs.items()})

    def product(self, nodes: typing.Sequence[NumericExpr], /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.product(nodes) for name, ex in self._numeric_exprs.items()})

    def rolling(self, /, window: params.Nanoseconds, operator: params.RollingOperationLiteral) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.rolling(window, operator) for name, ex in self._numeric_exprs.items()})

    def scale(self, /, scalar: params.DoubleConstant) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.scale(scalar) for name, ex in self._numeric_exprs.items()})

    def sin(self, /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.sin() for name, ex in self._numeric_exprs.items()})

    def sqrt(self, /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.sqrt() for name, ex in self._numeric_exprs.items()})

    def sum(self, nodes: typing.Sequence[NumericExpr], /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.sum(nodes) for name, ex in self._numeric_exprs.items()})

    def tan(self, /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.tan() for name, ex in self._numeric_exprs.items()})

    def time_difference(self, /, time_unit: params.TimeUnitLiteral | None = None) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.time_difference(time_unit) for name, ex in self._numeric_exprs.items()})

    def threshold(
        self, /, threshold: params.DoubleConstant, operator: params.ThresholdOperatorLiteral
    ) -> BatchRangeExpr:
        return BatchRangeExpr({name: ex.threshold(threshold, operator) for name, ex in self._numeric_exprs.items()})

    def value_difference(self, /) -> BatchNumericExpr:
        return BatchNumericExpr({name: ex.value_difference() for name, ex in self._numeric_exprs.items()})

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
class BatchRangeExpr(Expr):
    _range_exprs: dict[str, RangeExpr]

    def _to_conjure(self) -> list[scout_compute_api.RangeSeries]:
        return [ex._to_conjure() for ex in self._range_exprs.values()]

    def intersect(self, ranges: typing.Sequence[RangeExpr], /) -> BatchRangeExpr:
        return BatchRangeExpr({name: ex.intersect(ranges) for name, ex in self._range_exprs.items()})

    def invert(self, /) -> BatchRangeExpr:
        return BatchRangeExpr({name: ex.invert() for name, ex in self._range_exprs.items()})

    def union(self, ranges: typing.Sequence[RangeExpr], /) -> BatchRangeExpr:
        return BatchRangeExpr({name: ex.union(ranges) for name, ex in self._range_exprs.items()})


# imported at the end to prevent circular references
from nominal.experimental.compute.dsl import _expr_impls  # noqa: E402
