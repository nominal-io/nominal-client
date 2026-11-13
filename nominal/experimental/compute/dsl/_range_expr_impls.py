from __future__ import annotations

import typing
from dataclasses import dataclass

from nominal.experimental.compute.dsl import exprs, params
from nominal.protos.compute.v1 import (
    expressions_pb2,
)


@dataclass(frozen=True)
class ThresholdExpr(exprs.RangeExpr):
    _node: exprs.NumericExpr
    _threshold: float
    _operator: params.ThresholdOperatorLiteral

    def _to_proto(self) -> expressions_pb2.RangeExpression:
        return expressions_pb2.RangeExpression(
            numeric_threshold=expressions_pb2.NumericThresholdRanges(
                input=self._node._to_proto(),
                threshold=params._float_to_proto(self._threshold),
                comparison=params._threshold_operator_to_proto(self._operator),
            )
        )


@dataclass(frozen=True)
class IntersectRangesExpr(exprs.RangeExpr):
    _nodes: typing.Sequence[exprs.RangeExpr]

    def _to_proto(self) -> expressions_pb2.RangeExpression:
        return expressions_pb2.RangeExpression(
            intersection=expressions_pb2.RangeExpressionCollection(
                inputs=[range_node._to_proto() for range_node in self._nodes]
            )
        )


@dataclass(frozen=True)
class InvertRangesExpr(exprs.RangeExpr):
    _nodes: exprs.RangeExpr

    def _to_proto(self) -> expressions_pb2.RangeExpression:
        return expressions_pb2.RangeExpression(complement=self._nodes._to_proto())


@dataclass(frozen=True)
class UnionRangesExpr(exprs.RangeExpr):
    _nodes: typing.Sequence[exprs.RangeExpr]

    def _to_proto(self) -> expressions_pb2.RangeExpression:
        return expressions_pb2.RangeExpression(
            union=expressions_pb2.RangeExpressionCollection(
                inputs=[range_node._to_proto() for range_node in self._nodes]
            )
        )
