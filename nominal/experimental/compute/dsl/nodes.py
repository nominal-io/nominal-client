from __future__ import annotations

import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass

from nominal_api import scout_compute_api

from nominal.experimental.compute.dsl import params


@dataclass(frozen=True)
class Node(ABC):
    """Base class for all compute nodes."""


@dataclass(frozen=True)
class NumericNode(Node):
    @abstractmethod
    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        raise NotImplementedError()

    @classmethod
    def channel(cls, asset_rid: str, data_scope_name: str, channel_name: str) -> NumericNode:
        return _node_impls.NumericChannel(
            _asset_rid=asset_rid, _data_scope_name=data_scope_name, _channel_name=channel_name
        )

    def abs(self, /) -> NumericNode:
        return _node_impls.AbsNode(_node=self)

    def acos(self, /) -> NumericNode:
        return _node_impls.AcosNode(_node=self)

    def asin(self, /) -> NumericNode:
        return _node_impls.AsinNode(_node=self)

    def atan2(self, x_node: NumericNode, /) -> NumericNode:
        return _node_impls.Atan2Node(_y_node=self, _x_node=x_node)

    def cos(self, /) -> NumericNode:
        return _node_impls.CosNode(_node=self)

    def cumulative_sum(self, /, start_timestamp: params.NanosecondsUTC) -> NumericNode:
        return _node_impls.CumulativeSumNode(_node=self, _start_timestamp=start_timestamp)

    def derivative(self, /, time_unit: params.TimeUnitLiteral) -> NumericNode:
        return _node_impls.DerivativeNode(_node=self, _time_unit=time_unit)

    def divide(self, right_node: NumericNode, /) -> NumericNode:
        return _node_impls.DivideNode(_left_node=self, _right_node=right_node)

    def filter(self, ranges: RangeNode, /) -> NumericNode:
        return _node_impls.FilterNode(_node=self, _ranges=ranges)

    def floor_divide(self, right_node: NumericNode, /) -> NumericNode:
        return _node_impls.FloorDivideNode(_left_node=self, _right_node=right_node)

    def integral(self, /, start_timestamp: params.NanosecondsUTC, time_unit: params.TimeUnitLiteral) -> NumericNode:
        return _node_impls.IntegralNode(_node=self, _start_timestamp=start_timestamp, _time_unit=time_unit)

    def ln(self, /) -> NumericNode:
        return _node_impls.LnNode(_node=self)

    def logarithm(self, /) -> NumericNode:
        return _node_impls.LogarithmNode(_node=self)

    def max(self, nodes: typing.Sequence[NumericNode], /) -> NumericNode:
        return _node_impls.MaxNode(_inputs=[self, *nodes])

    def mean(self, nodes: typing.Sequence[NumericNode], /) -> NumericNode:
        return _node_impls.MeanNode(_inputs=[self, *nodes])

    def min(self, nodes: typing.Sequence[NumericNode], /) -> NumericNode:
        return _node_impls.MinNode(_inputs=[self, *nodes])

    def minus(self, right_node: NumericNode, /) -> NumericNode:
        return _node_impls.MinusNode(_left_node=self, _right_node=right_node)

    def modulo(self, right_node: NumericNode, /) -> NumericNode:
        return _node_impls.ModuloNode(_left_node=self, _right_node=right_node)

    def multiply(self, right_node: NumericNode, /) -> NumericNode:
        return _node_impls.MultiplyNode(_left_node=self, _right_node=right_node)

    def offset(self, /, offset: params.DoubleConstant) -> NumericNode:
        return _node_impls.OffsetNode(_node=self, _offset=offset)

    def plus(self, right_node: NumericNode, /) -> NumericNode:
        return _node_impls.PlusNode(_left_node=self, _right_node=right_node)

    def power(self, exponent_node: NumericNode, /) -> NumericNode:
        return _node_impls.PowerNode(_base_node=self, _exponent_node=exponent_node)

    def product(self, nodes: typing.Sequence[NumericNode], /) -> NumericNode:
        return _node_impls.ProductNode(_inputs=[self, *nodes])

    def rolling(self, /, window: params.Nanoseconds, operator: params.RollingOperationLiteral) -> NumericNode:
        return _node_impls.RollingNode(_node=self, _window=window, _operator=operator)

    def scale(self, /, scalar: params.DoubleConstant) -> NumericNode:
        return _node_impls.ScaleNode(_node=self, _scalar=scalar)

    def sin(self, /) -> NumericNode:
        return _node_impls.SinNode(_node=self)

    def sqrt(self, /) -> NumericNode:
        return _node_impls.SqrtNode(_node=self)

    def sum(self, nodes: typing.Sequence[NumericNode], /) -> NumericNode:
        return _node_impls.SumNode(_nodes=[self, *nodes])

    def tan(self, /) -> NumericNode:
        return _node_impls.TanNode(_node=self)

    def time_difference(self, /, time_unit: params.TimeUnitLiteral | None = None) -> NumericNode:
        return _node_impls.TimeDifferenceNode(_node=self, _time_unit=time_unit)

    def threshold(self, /, threshold: params.DoubleConstant, operator: params.ThresholdOperatorLiteral) -> RangeNode:
        return _node_impls.ThresholdNode(_node=self, _threshold=threshold, _operator=operator)

    def value_difference(self, /) -> NumericNode:
        return _node_impls.ValueDifferenceNode(_node=self)

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
class RangeNode(Node):
    @abstractmethod
    def _to_conjure(self) -> scout_compute_api.RangeSeries:
        raise NotImplementedError()

    def intersect(self, ranges: typing.Sequence[RangeNode], /) -> RangeNode:
        return _node_impls.IntersectRangesNode(_ranges=[self, *ranges])

    def not_(self, /) -> RangeNode:
        return _node_impls.NotRangesNode(_ranges=self)

    def union(self, ranges: typing.Sequence[RangeNode], /) -> RangeNode:
        return _node_impls.UnionRangesNode(_ranges=[self, *ranges])


# imported at the end to prevent circular references
from nominal.experimental.compute.dsl import _node_impls  # noqa: E402
