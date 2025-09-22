from __future__ import annotations

import typing
from dataclasses import dataclass

from nominal_api import scout_compute_api

from nominal.experimental.compute.dsl import exprs, params


@dataclass(frozen=True)
class AbsExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="|input|",
                inputs={"input": self._node._to_conjure()},
            )
        )


@dataclass(frozen=True)
class AcosExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="acos(input)",
                inputs={"input": self._node._to_conjure()},
            )
        )


@dataclass(frozen=True)
class AsinExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="asin(input)",
                inputs={"input": self._node._to_conjure()},
            )
        )


@dataclass(frozen=True)
class Atan2Expr(exprs.NumericExpr):
    _y_node: exprs.NumericExpr
    _x_node: exprs.NumericExpr

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
class CosExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="cos(input)",
                inputs={"input": self._node._to_conjure()},
            )
        )


@dataclass(frozen=True)
class CumulativeSumExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr
    _start_timestamp: params.NanosecondsUTC

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            cumulative_sum=scout_compute_api.CumulativeSumSeries(
                input=self._node._to_conjure(),
                start_timestamp=params._timestamp_to_conjure(self._start_timestamp),
            )
        )


@dataclass(frozen=True)
class DerivativeExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr
    _time_unit: params.TimeUnitLiteral

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            derivative=scout_compute_api.DerivativeSeries(
                input=self._node._to_conjure(),
                time_unit=params._time_unit_to_conjure(self._time_unit),
            )
        )


@dataclass(frozen=True)
class DivideExpr(exprs.NumericExpr):
    _left_node: exprs.NumericExpr
    _right_node: exprs.NumericExpr

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
class FilterExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr
    _ranges: exprs.RangeExpr

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            filter_transformation=scout_compute_api.NumericFilterTransformationSeries(
                input=self._node._to_conjure(),
                filter=self._ranges._to_conjure(),
            )
        )


@dataclass(frozen=True)
class FloorDivideExpr(exprs.NumericExpr):
    _left_node: exprs.NumericExpr
    _right_node: exprs.NumericExpr

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
class IntegralExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr
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
class LnExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            unary_arithmetic=scout_compute_api.UnaryArithmeticSeries(
                input=self._node._to_conjure(),
                operation=scout_compute_api.UnaryArithmeticOperation.LN,
            )
        )


@dataclass(frozen=True)
class LogarithmExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            unary_arithmetic=scout_compute_api.UnaryArithmeticSeries(
                input=self._node._to_conjure(),
                operation=scout_compute_api.UnaryArithmeticOperation.LOG,
            )
        )


@dataclass(frozen=True)
class MaxExpr(exprs.NumericExpr):
    _nodes: typing.Sequence[exprs.NumericExpr]

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            max=scout_compute_api.MaxSeries(
                inputs=[node._to_conjure() for node in self._nodes],
            )
        )


@dataclass(frozen=True)
class MeanExpr(exprs.NumericExpr):
    _nodes: typing.Sequence[exprs.NumericExpr]

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            mean=scout_compute_api.MeanSeries(
                inputs=[node._to_conjure() for node in self._nodes],
            )
        )


@dataclass(frozen=True)
class MinExpr(exprs.NumericExpr):
    _nodes: typing.Sequence[exprs.NumericExpr]

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            min=scout_compute_api.MinSeries(
                inputs=[node._to_conjure() for node in self._nodes],
            )
        )


@dataclass(frozen=True)
class MinusExpr(exprs.NumericExpr):
    _left_node: exprs.NumericExpr
    _right_node: exprs.NumericExpr

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
class ModuloExpr(exprs.NumericExpr):
    _left_node: exprs.NumericExpr
    _right_node: exprs.NumericExpr

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
class MultiplyExpr(exprs.NumericExpr):
    _left_node: exprs.NumericExpr
    _right_node: exprs.NumericExpr

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
class NumericAssetChannelExpr(exprs.NumericExpr):
    _asset_rid: str
    _data_scope_name: str
    _channel_name: str
    _additional_tags: dict[str, str]

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            channel=scout_compute_api.ChannelSeries(
                asset=scout_compute_api.AssetChannel(
                    additional_tags={
                        key: scout_compute_api.StringConstant(literal=value)
                        for key, value in self._additional_tags.items()
                    },
                    asset_rid=scout_compute_api.StringConstant(literal=self._asset_rid),
                    data_scope_name=scout_compute_api.StringConstant(literal=self._data_scope_name),
                    channel=scout_compute_api.StringConstant(literal=self._channel_name),
                    group_by_tags=[],
                    tags_to_group_by=[],
                    additional_tag_filters=None,
                )
            )
        )


@dataclass(frozen=True)
class NumericDatasourceChannelExpr(exprs.NumericExpr):
    _datasource_rid: str
    _channel_name: str
    _tags: dict[str, str]

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            channel=scout_compute_api.ChannelSeries(
                data_source=scout_compute_api.DataSourceChannel(
                    channel=scout_compute_api.StringConstant(literal=self._channel_name),
                    data_source_rid=scout_compute_api.StringConstant(literal=self._datasource_rid),
                    tags={key: scout_compute_api.StringConstant(literal=value) for key, value in self._tags.items()},
                    group_by_tags=[],
                    tags_to_group_by=[],
                    tag_filters=None,
                )
            )
        )


@dataclass(frozen=True)
class NumericRunChannelExpr(exprs.NumericExpr):
    _run_rid: str
    _data_scope_name: str
    _channel_name: str
    _additional_tags: dict[str, str]

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            channel=scout_compute_api.ChannelSeries(
                run=scout_compute_api.RunChannel(
                    additional_tags={
                        key: scout_compute_api.StringConstant(literal=value)
                        for key, value in self._additional_tags.items()
                    },
                    run_rid=scout_compute_api.StringConstant(literal=self._run_rid),
                    data_scope_name=scout_compute_api.StringConstant(literal=self._data_scope_name),
                    channel=scout_compute_api.StringConstant(literal=self._channel_name),
                    group_by_tags=[],
                    tags_to_group_by=[],
                    additional_tag_filters=None,
                )
            )
        )


@dataclass(frozen=True)
class OffsetExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr
    _offset: float

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            offset=scout_compute_api.OffsetSeries(
                input=self._node._to_conjure(),
                scalar=params._float_to_conjure(self._offset),
            )
        )


@dataclass(frozen=True)
class PlusExpr(exprs.NumericExpr):
    _left_node: exprs.NumericExpr
    _right_node: exprs.NumericExpr

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
class PowerExpr(exprs.NumericExpr):
    _base_node: exprs.NumericExpr
    _exponent_node: exprs.NumericExpr

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
class ProductExpr(exprs.NumericExpr):
    _inputs: typing.Sequence[exprs.NumericExpr]

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            product=scout_compute_api.ProductSeries(
                inputs=[node._to_conjure() for node in self._inputs],
            )
        )


@dataclass(frozen=True)
class RollingExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr
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
class ScaleExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr
    _scalar: float

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            scale=scout_compute_api.ScaleSeries(
                input=self._node._to_conjure(),
                scalar=params._float_to_conjure(self._scalar),
            )
        )


@dataclass(frozen=True)
class SinExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="sin(input)",
                inputs={"input": self._node._to_conjure()},
            )
        )


@dataclass(frozen=True)
class SqrtExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="sqrt(input)",
                inputs={"input": self._node._to_conjure()},
            )
        )


@dataclass(frozen=True)
class SumExpr(exprs.NumericExpr):
    _nodes: typing.Sequence[exprs.NumericExpr]

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            sum=scout_compute_api.SumSeries(
                inputs=[node._to_conjure() for node in self._nodes],
            )
        )


@dataclass(frozen=True)
class TanExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            arithmetic=scout_compute_api.ArithmeticSeries(
                expression="tan(input)",
                inputs={"input": self._node._to_conjure()},
            )
        )


@dataclass(frozen=True)
class TimeDifferenceExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr
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
class ValueDifferenceExpr(exprs.NumericExpr):
    _node: exprs.NumericExpr

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            value_difference=scout_compute_api.ValueDifferenceSeries(
                input=self._node._to_conjure(),
            )
        )
