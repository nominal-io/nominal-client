from __future__ import annotations

import typing
from dataclasses import dataclass

from nominal_api import scout_compute_api

from nominal.experimental.compute.dsl import nodes, params


@dataclass(frozen=True)
class AbsNode(nodes.NumericNode):
    _node: nodes.NumericNode

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="|input|",
                inputs={"input": self._node._to_conjure()},
            )
        )


@dataclass(frozen=True)
class AcosNode(nodes.NumericNode):
    _node: nodes.NumericNode

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="acos(input)",
                inputs={"input": self._node._to_conjure()},
            )
        )


@dataclass(frozen=True)
class AsinNode(nodes.NumericNode):
    _node: nodes.NumericNode

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="asin(input)",
                inputs={"input": self._node._to_conjure()},
            )
        )


@dataclass(frozen=True)
class Atan2Node(nodes.NumericNode):
    _y_node: nodes.NumericNode
    _x_node: nodes.NumericNode

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="atan2(y, x)",
                inputs={
                    "y": self._y_node._to_conjure(),
                    "x": self._x_node._to_conjure(),
                },
            )
        )


@dataclass(frozen=True)
class CosNode(nodes.NumericNode):
    _node: nodes.NumericNode

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="cos(input)",
                inputs={"input": self._node._to_conjure()},
            )
        )


@dataclass(frozen=True)
class CumulativeSumNode(nodes.NumericNode):
    _node: nodes.NumericNode
    _start_timestamp: params.NanosecondsUTC

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            cumulative_sum=scout_compute_api.CumulativeSumSeries(
                input=self._node._to_conjure(),
                start_timestamp=params._timestamp_to_conjure(self._start_timestamp),
            )
        )


@dataclass(frozen=True)
class DerivativeNode(nodes.NumericNode):
    _node: nodes.NumericNode
    _time_unit: params.TimeUnitLiteral

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            derivative=scout_compute_api.DerivativeSeries(
                input=self._node._to_conjure(),
                time_unit=params._time_unit_to_conjure(self._time_unit),
            )
        )


@dataclass(frozen=True)
class DivideNode(nodes.NumericNode):
    _left_node: nodes.NumericNode
    _right_node: nodes.NumericNode

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="left / right",
                inputs={
                    "left": self._left_node._to_conjure(),
                    "right": self._right_node._to_conjure(),
                },
            )
        )


@dataclass(frozen=True)
class FilterNode(nodes.NumericNode):
    _node: nodes.NumericNode
    _ranges: nodes.RangeNode

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            filter_transformation=scout_compute_api.NumericFilterTransformationSeries(
                input=self._node._to_conjure(),
                filter=self._ranges._to_conjure(),
            )
        )


@dataclass(frozen=True)
class FloorDivideNode(nodes.NumericNode):
    _left_node: nodes.NumericNode
    _right_node: nodes.NumericNode

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="left // right",
                inputs={
                    "left": self._left_node._to_conjure(),
                    "right": self._right_node._to_conjure(),
                },
            )
        )


@dataclass(frozen=True)
class IntegralNode(nodes.NumericNode):
    _node: nodes.NumericNode
    _start_timestamp: params.NanosecondsUTC
    _time_unit: params.TimeUnitLiteral

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            integral=scout_compute_api.IntegralSeries(
                input=self._node._to_conjure(),
                start_timestamp=params._timestamp_to_conjure(self._start_timestamp),
                time_unit=params._time_unit_to_conjure(self._time_unit),
            )
        )


@dataclass(frozen=True)
class LnNode(nodes.NumericNode):
    _node: nodes.NumericNode

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            unary_arithmetic=scout_compute_api.UnaryArithmeticSeries(
                input=self._node._to_conjure(),
                operation=scout_compute_api.UnaryArithmeticOperation.LN,
            )
        )


@dataclass(frozen=True)
class LogarithmNode(nodes.NumericNode):
    _node: nodes.NumericNode

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            unary_arithmetic=scout_compute_api.UnaryArithmeticSeries(
                input=self._node._to_conjure(),
                operation=scout_compute_api.UnaryArithmeticOperation.LOG,
            )
        )


@dataclass(frozen=True)
class MaxNode(nodes.NumericNode):
    _inputs: typing.Sequence[nodes.NumericNode]

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            max=scout_compute_api.MaxSeries(
                inputs=[node._to_conjure() for node in self._inputs],
            )
        )


@dataclass(frozen=True)
class MeanNode(nodes.NumericNode):
    _inputs: typing.Sequence[nodes.NumericNode]

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            mean=scout_compute_api.MeanSeries(
                inputs=[node._to_conjure() for node in self._inputs],
            )
        )


@dataclass(frozen=True)
class MinNode(nodes.NumericNode):
    _inputs: typing.Sequence[nodes.NumericNode]

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            min=scout_compute_api.MinSeries(
                inputs=[node._to_conjure() for node in self._inputs],
            )
        )


@dataclass(frozen=True)
class MinusNode(nodes.NumericNode):
    _left_node: nodes.NumericNode
    _right_node: nodes.NumericNode

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="left - right",
                inputs={
                    "left": self._left_node._to_conjure(),
                    "right": self._right_node._to_conjure(),
                },
            )
        )


@dataclass(frozen=True)
class ModuloNode(nodes.NumericNode):
    _left_node: nodes.NumericNode
    _right_node: nodes.NumericNode

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression=r"left % right",
                inputs={
                    "left": self._left_node._to_conjure(),
                    "right": self._right_node._to_conjure(),
                },
            )
        )


@dataclass(frozen=True)
class MultiplyNode(nodes.NumericNode):
    _left_node: nodes.NumericNode
    _right_node: nodes.NumericNode

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="left * right",
                inputs={
                    "left": self._left_node._to_conjure(),
                    "right": self._right_node._to_conjure(),
                },
            )
        )


@dataclass(frozen=True)
class NumericChannel(nodes.NumericNode):
    _asset_rid: str
    _data_scope_name: str
    _channel_name: str

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            channel=scout_compute_api.ChannelSeries(
                asset=scout_compute_api.AssetChannel(
                    additional_tags={},
                    asset_rid=scout_compute_api.StringConstant(literal=self._asset_rid),
                    channel=scout_compute_api.StringConstant(literal=self._channel_name),
                    data_scope_name=scout_compute_api.StringConstant(literal=self._data_scope_name),
                    group_by_tags=[],
                    tags_to_group_by=[],
                    additional_tag_filters=None,
                )
            )
        )


@dataclass(frozen=True)
class OffsetNode(nodes.NumericNode):
    _node: nodes.NumericNode
    _offset: float

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            offset=scout_compute_api.OffsetSeries(
                input=self._node._to_conjure(),
                scalar=params._float_to_conjure(self._offset),
            )
        )


@dataclass(frozen=True)
class PlusNode(nodes.NumericNode):
    _left_node: nodes.NumericNode
    _right_node: nodes.NumericNode

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="left + right",
                inputs={
                    "left": self._left_node._to_conjure(),
                    "right": self._right_node._to_conjure(),
                },
            )
        )


@dataclass(frozen=True)
class PowerNode(nodes.NumericNode):
    _base_node: nodes.NumericNode
    _exponent_node: nodes.NumericNode

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="base ** exponent",
                inputs={
                    "base": self._base_node._to_conjure(),
                    "exponent": self._exponent_node._to_conjure(),
                },
            )
        )


@dataclass(frozen=True)
class ProductNode(nodes.NumericNode):
    _inputs: typing.Sequence[nodes.NumericNode]

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            product=scout_compute_api.ProductSeries(
                inputs=[node._to_conjure() for node in self._inputs],
            )
        )


@dataclass(frozen=True)
class RollingNode(nodes.NumericNode):
    _node: nodes.NumericNode
    _window: params.Nanoseconds
    _operator: params.RollingOperationLiteral

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            rolling_operation=scout_compute_api.RollingOperationSeries(
                input=self._node._to_conjure(),
                window=params._window_to_conjure(self._window),
                operator=params._rolling_operation_to_conjure(self._operator),
            )
        )


@dataclass(frozen=True)
class ScaleNode(nodes.NumericNode):
    _node: nodes.NumericNode
    _scalar: float

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            scale=scout_compute_api.ScaleSeries(
                input=self._node._to_conjure(),
                scalar=params._float_to_conjure(self._scalar),
            )
        )


@dataclass(frozen=True)
class SinNode(nodes.NumericNode):
    _node: nodes.NumericNode

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="sin(input)",
                inputs={"input": self._node._to_conjure()},
            )
        )


@dataclass(frozen=True)
class SqrtNode(nodes.NumericNode):
    _node: nodes.NumericNode

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="sqrt(input)",
                inputs={"input": self._node._to_conjure()},
            )
        )


@dataclass(frozen=True)
class SumNode(nodes.NumericNode):
    _nodes: typing.Sequence[nodes.NumericNode]

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            sum=scout_compute_api.SumSeries(
                inputs=[node._to_conjure() for node in self._nodes],
            )
        )


@dataclass(frozen=True)
class TanNode(nodes.NumericNode):
    _node: nodes.NumericNode

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="tan(input)",
                inputs={"input": self._node._to_conjure()},
            )
        )


@dataclass(frozen=True)
class ThresholdNode(nodes.RangeNode):
    _node: nodes.NumericNode
    _threshold: float
    _operator: params.ThresholdOperatorLiteral

    def _to_conjure(self) -> scout_compute_api.RangeSeries:
        return scout_compute_api.RangeSeries(
            threshold=scout_compute_api.ThresholdingRanges(
                input=self._node._to_conjure(),
                threshold=params._float_to_conjure(self._threshold),
                operator=params._threshold_operator_to_conjure(self._operator),
            )
        )


@dataclass(frozen=True)
class IntersectRangesNode(nodes.RangeNode):
    _ranges: typing.Sequence[nodes.RangeNode]

    def _to_conjure(self) -> scout_compute_api.RangeSeries:
        return scout_compute_api.RangeSeries(
            intersect_range=scout_compute_api.IntersectRanges(
                inputs=[range_node._to_conjure() for range_node in self._ranges]
            )
        )


@dataclass(frozen=True)
class NotRangesNode(nodes.RangeNode):
    _ranges: nodes.RangeNode

    def _to_conjure(self) -> scout_compute_api.RangeSeries:
        return scout_compute_api.RangeSeries(not_=scout_compute_api.NotRanges(input=self._ranges._to_conjure()))


@dataclass(frozen=True)
class UnionRangesNode(nodes.RangeNode):
    _ranges: typing.Sequence[nodes.RangeNode]

    def _to_conjure(self) -> scout_compute_api.RangeSeries:
        return scout_compute_api.RangeSeries(
            union_range=scout_compute_api.UnionRanges(inputs=[range_node._to_conjure() for range_node in self._ranges])
        )


@dataclass(frozen=True)
class TimeDifferenceNode(nodes.NumericNode):
    _node: nodes.NumericNode
    _time_unit: params.TimeUnitLiteral | None = None

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        # TimeDifferenceSeries expects a generic Series input, so we need to wrap our NumericSeries
        input_series = scout_compute_api.Series(numeric=self._node._to_conjure())
        return scout_compute_api.NumericSeries(
            time_difference=scout_compute_api.TimeDifferenceSeries(
                input=input_series,
                time_unit=params._time_unit_to_conjure(self._time_unit) if self._time_unit else None,
            )
        )


@dataclass(frozen=True)
class ValueDifferenceNode(nodes.NumericNode):
    _node: nodes.NumericNode

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            value_difference=scout_compute_api.ValueDifferenceSeries(
                input=self._node._to_conjure(),
            )
        )
