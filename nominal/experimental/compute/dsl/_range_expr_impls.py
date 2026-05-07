from __future__ import annotations

import typing
from dataclasses import dataclass

from nominal_api import scout_compute_api

from nominal.experimental.compute.dsl import exprs, params


@dataclass(frozen=True)
class ThresholdExpr(exprs.RangeExpr):
    _node: exprs.NumericExpr
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
class IntersectRangesExpr(exprs.RangeExpr):
    _nodes: typing.Sequence[exprs.RangeExpr]

    def _to_conjure(self) -> scout_compute_api.RangeSeries:
        return scout_compute_api.RangeSeries(
            intersect_range=scout_compute_api.IntersectRanges(
                inputs=[range_node._to_conjure() for range_node in self._nodes]
            )
        )


@dataclass(frozen=True)
class InvertRangesExpr(exprs.RangeExpr):
    _nodes: exprs.RangeExpr

    def _to_conjure(self) -> scout_compute_api.RangeSeries:
        return scout_compute_api.RangeSeries(not_=scout_compute_api.NotRanges(input=self._nodes._to_conjure()))


@dataclass(frozen=True)
class UnionRangesExpr(exprs.RangeExpr):
    _nodes: typing.Sequence[exprs.RangeExpr]

    def _to_conjure(self) -> scout_compute_api.RangeSeries:
        return scout_compute_api.RangeSeries(
            union_range=scout_compute_api.UnionRanges(inputs=[range_node._to_conjure() for range_node in self._nodes])
        )
