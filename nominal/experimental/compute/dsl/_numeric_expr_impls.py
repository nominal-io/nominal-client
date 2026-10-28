from __future__ import annotations

import typing
from dataclasses import dataclass

from nominal.experimental.compute.dsl import exprs, params
from nominal.protos.compute.v1 import (
    constants_pb2,
    expressions_pb2,
    numeric_parameters_pb2,
    source_selection_pb2,
)


@dataclass(frozen=True)
class AbsExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            arithmetic=expressions_pb2.NumericArithmeticExpression(
                formula="|input|",
                inputs={"input": self._node._to_proto()},
            )
        )


@dataclass(frozen=True)
class AcosExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            arithmetic=expressions_pb2.NumericArithmeticExpression(
                formula="acos(input)",
                inputs={"input": self._node._to_proto()},
            )
        )


@dataclass(frozen=True)
class AsinExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            arithmetic=expressions_pb2.NumericArithmeticExpression(
                formula="asin(input)",
                inputs={"input": self._node._to_proto()},
            )
        )


@dataclass(frozen=True)
class Atan2Expr(exprs.NumericExpr):
    _y_node: exprs.NumericExpr
    _x_node: exprs.NumericExpr

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            arithmetic=expressions_pb2.NumericArithmeticExpression(
                formula="atan2(y, x)",
                inputs={
                    "y": self._y_node._to_proto(),
                    "x": self._x_node._to_proto(),
                },
            )
        )


@dataclass(frozen=True)
class CosExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            arithmetic=expressions_pb2.NumericArithmeticExpression(
                formula="cos(input)",
                inputs={"input": self._node._to_proto()},
            )
        )


@dataclass(frozen=True)
class CumulativeSumExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr
    _start_timestamp: params.NanosecondsUTC

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            cumulative_sum=expressions_pb2.NumericCumulativeSumExpression(
                input=self._node._to_proto(),
                start=params._timestamp_to_proto(self._start_timestamp),
            )
        )


@dataclass(frozen=True)
class DerivativeExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr
    _time_unit: params.TimeUnitLiteral

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            derivative=expressions_pb2.NumericDerivativeExpression(
                input=self._node._to_proto(),
                time_unit=params._time_unit_to_proto(self._time_unit),
            )
        )


@dataclass(frozen=True)
class DivideExpr(exprs.NumericExpr):
    _left_node: exprs.NumericExpr
    _right_node: exprs.NumericExpr

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            arithmetic=expressions_pb2.NumericArithmeticExpression(
                formula="left / right",
                inputs={
                    "left": self._left_node._to_proto(),
                    "right": self._right_node._to_proto(),
                },
            )
        )


@dataclass(frozen=True)
class FilterExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr
    _ranges: exprs.RangeExpr

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            range_filter=expressions_pb2.NumericRangeFilterExpression(
                input=self._node._to_proto(), ranges=self._ranges._to_proto()
            )
        )


@dataclass(frozen=True)
class FloorDivideExpr(exprs.NumericExpr):
    _left_node: exprs.NumericExpr
    _right_node: exprs.NumericExpr

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            arithmetic=expressions_pb2.NumericArithmeticExpression(
                formula="left // right",
                inputs={
                    "left": self._left_node._to_proto(),
                    "right": self._right_node._to_proto(),
                },
            )
        )


@dataclass(frozen=True)
class IntegralExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr
    _start_timestamp: params.NanosecondsUTC
    _time_unit: params.TimeUnitLiteral

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            integral=expressions_pb2.NumericIntegralExpression(
                input=self._node._to_proto(),
                start=params._timestamp_to_proto(self._start_timestamp),
                time_unit=params._time_unit_to_proto(self._time_unit),
            )
        )


@dataclass(frozen=True)
class LnExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            unary=expressions_pb2.NumericUnaryExpression(
                input=self._node._to_proto(), operation=expressions_pb2.UNARY_NUMERIC_OPERATION_LN
            )
        )


@dataclass(frozen=True)
class LogarithmExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            unary=expressions_pb2.NumericUnaryExpression(
                input=self._node._to_proto(), operation=expressions_pb2.UNARY_NUMERIC_OPERATION_LOG10
            )
        )


@dataclass(frozen=True)
class MaxExpr(exprs.NumericExpr):
    _nodes: typing.Sequence[exprs.NumericExpr]

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            nary=expressions_pb2.NumericNaryExpression(
                operation=expressions_pb2.NARY_NUMERIC_OPERATION_MAX, inputs=[node._to_proto() for node in self._nodes]
            )
        )


@dataclass(frozen=True)
class MeanExpr(exprs.NumericExpr):
    _nodes: typing.Sequence[exprs.NumericExpr]

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            nary=expressions_pb2.NumericNaryExpression(
                operation=expressions_pb2.NARY_NUMERIC_OPERATION_MEAN, inputs=[node._to_proto() for node in self._nodes]
            )
        )


@dataclass(frozen=True)
class MinExpr(exprs.NumericExpr):
    _nodes: typing.Sequence[exprs.NumericExpr]

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            nary=expressions_pb2.NumericNaryExpression(
                operation=expressions_pb2.NARY_NUMERIC_OPERATION_MIN, inputs=[node._to_proto() for node in self._nodes]
            )
        )


@dataclass(frozen=True)
class MinusExpr(exprs.NumericExpr):
    _left_node: exprs.NumericExpr
    _right_node: exprs.NumericExpr

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            arithmetic=expressions_pb2.NumericArithmeticExpression(
                formula="left - right",
                inputs={
                    "left": self._left_node._to_proto(),
                    "right": self._right_node._to_proto(),
                },
            )
        )


@dataclass(frozen=True)
class ModuloExpr(exprs.NumericExpr):
    _left_node: exprs.NumericExpr
    _right_node: exprs.NumericExpr

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            arithmetic=expressions_pb2.NumericArithmeticExpression(
                formula=r"left % right",
                inputs={
                    "left": self._left_node._to_proto(),
                    "right": self._right_node._to_proto(),
                },
            )
        )


@dataclass(frozen=True)
class MultiplyExpr(exprs.NumericExpr):
    _left_node: exprs.NumericExpr
    _right_node: exprs.NumericExpr

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            arithmetic=expressions_pb2.NumericArithmeticExpression(
                formula="left * right",
                inputs={
                    "left": self._left_node._to_proto(),
                    "right": self._right_node._to_proto(),
                },
            )
        )


@dataclass(frozen=True)
class NumericAssetChannelExpr(exprs.NumericExpr):
    _asset_rid: str
    _data_scope_name: str
    _channel_name: str
    _additional_tags: dict[str, str]

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            channel_selection=source_selection_pb2.ChannelSelection(
                asset=source_selection_pb2.AssetChannelSelection(
                    asset=source_selection_pb2.AssetReferenceExpression(literal=self._asset_rid),
                    channel=constants_pb2.StringConstant(literal=self._channel_name),
                    data_scope=constants_pb2.StringConstant(literal=self._data_scope_name),
                    tags=source_selection_pb2.ChannelTagConstraints(
                        exact_values=source_selection_pb2.ExactTagValues(
                            values={
                                k: constants_pb2.StringConstant(literal=v) for k, v in self._additional_tags.items()
                            }
                        )
                    ),
                )
            )
        )


@dataclass(frozen=True)
class NumericDatasourceChannelExpr(exprs.NumericExpr):
    _datasource_rid: str
    _channel_name: str
    _tags: dict[str, str]

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            channel_selection=source_selection_pb2.ChannelSelection(
                data_source=source_selection_pb2.DataSourceChannelSelection(
                    data_source=source_selection_pb2.DataSourceReferenceExpression(literal=self._datasource_rid),
                    channel=constants_pb2.StringConstant(literal=self._channel_name),
                    tags=source_selection_pb2.ChannelTagConstraints(
                        exact_values=source_selection_pb2.ExactTagValues(
                            values={k: constants_pb2.StringConstant(literal=v) for k, v in self._tags.items()}
                        )
                    ),
                )
            )
        )


@dataclass(frozen=True)
class NumericRunChannelExpr(exprs.NumericExpr):
    _run_rid: str
    _data_scope_name: str
    _channel_name: str
    _additional_tags: dict[str, str]

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            channel_selection=source_selection_pb2.ChannelSelection(
                run=source_selection_pb2.RunChannelSelection(
                    run=source_selection_pb2.RunReferenceExpression(literal=self._run_rid),
                    channel=constants_pb2.StringConstant(literal=self._channel_name),
                    data_scope=constants_pb2.StringConstant(literal=self._data_scope_name),
                    tags=source_selection_pb2.ChannelTagConstraints(
                        exact_values=source_selection_pb2.ExactTagValues(
                            values={
                                k: constants_pb2.StringConstant(literal=v) for k, v in self._additional_tags.items()
                            }
                        )
                    ),
                )
            )
        )


@dataclass(frozen=True)
class OffsetExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr
    _offset: float

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            scalar_transform=expressions_pb2.NumericScalarTransformExpression(
                input=self._node._to_proto(), offset=params._float_to_proto(self._offset)
            )
        )


@dataclass(frozen=True)
class PlusExpr(exprs.NumericExpr):
    _left_node: exprs.NumericExpr
    _right_node: exprs.NumericExpr

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            arithmetic=expressions_pb2.NumericArithmeticExpression(
                formula="left + right",
                inputs={
                    "left": self._left_node._to_proto(),
                    "right": self._right_node._to_proto(),
                },
            )
        )


@dataclass(frozen=True)
class PowerExpr(exprs.NumericExpr):
    _base_node: exprs.NumericExpr
    _exponent_node: exprs.NumericExpr

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            arithmetic=expressions_pb2.NumericArithmeticExpression(
                formula="base ** exponent",
                inputs={
                    "base": self._base_node._to_proto(),
                    "exponent": self._exponent_node._to_proto(),
                },
            )
        )


@dataclass(frozen=True)
class ProductExpr(exprs.NumericExpr):
    _inputs: typing.Sequence[exprs.NumericExpr]

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            nary=expressions_pb2.NumericNaryExpression(
                operation=expressions_pb2.NARY_NUMERIC_OPERATION_PRODUCT,
                inputs=[node._to_proto() for node in self._inputs],
            )
        )


@dataclass(frozen=True)
class RollingExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr
    _window: params.Nanoseconds
    _operator: params.RollingOperationLiteral

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            rolling_aggregation=expressions_pb2.NumericRollingAggregationExpression(
                input=self._node._to_proto(),
                window=params._duration_ns_to_proto(self._window),
                reduction=params._rolling_operation_to_proto(self._operator),
            )
        )


@dataclass(frozen=True)
class ScaleExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr
    _scalar: float

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            scalar_transform=expressions_pb2.NumericScalarTransformExpression(
                input=self._node._to_proto(),
                scale=numeric_parameters_pb2.ScalingFactor(value=params._float_to_proto(self._scalar)),
            )
        )


@dataclass(frozen=True)
class SinExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            arithmetic=expressions_pb2.NumericArithmeticExpression(
                formula="sin(input)",
                inputs={"input": self._node._to_proto()},
            )
        )


@dataclass(frozen=True)
class SqrtExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            arithmetic=expressions_pb2.NumericArithmeticExpression(
                formula="sqrt(input)",
                inputs={"input": self._node._to_proto()},
            )
        )


@dataclass(frozen=True)
class SumExpr(exprs.NumericExpr):
    _nodes: typing.Sequence[exprs.NumericExpr]

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            nary=expressions_pb2.NumericNaryExpression(
                operation=expressions_pb2.NARY_NUMERIC_OPERATION_SUM, inputs=[node._to_proto() for node in self._nodes]
            )
        )


@dataclass(frozen=True)
class TanExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            arithmetic=expressions_pb2.NumericArithmeticExpression(
                formula="tan(input)",
                inputs={"input": self._node._to_proto()},
            )
        )


@dataclass(frozen=True)
class TimeDifferenceExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr
    _time_unit: params.TimeUnitLiteral | None = None

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        # TimeDifferenceSeries expects a generic Series input, so we need to wrap our NumericSeries
        input_series = expressions_pb2.SeriesExpression(numeric=self._node._to_proto())
        return expressions_pb2.NumericExpression(
            time_difference=expressions_pb2.NumericTimeDifferenceExpression(
                input=input_series,
                unit=params._time_unit_to_proto(self._time_unit) if self._time_unit else None,
            )
        )


@dataclass(frozen=True)
class ValueDifferenceExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_proto(self) -> expressions_pb2.NumericExpression:
        return expressions_pb2.NumericExpression(
            value_difference=expressions_pb2.NumericValueDifferenceExpression(
                input=self._node._to_proto(),
            )
        )
