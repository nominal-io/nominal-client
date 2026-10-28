from __future__ import annotations

from dataclasses import dataclass

from nominal.experimental.compute.dsl import exprs, params
from nominal.protos.compute.v1 import (
    categorical_parameters_pb2,
    constants_pb2,
    expressions_pb2,
    numeric_parameters_pb2,
    source_selection_pb2,
)


@dataclass(frozen=True)
class EnumAssetChannelExpr(exprs.EnumExpr):
    _asset_rid: str
    _data_scope_name: str
    _channel_name: str
    _additional_tags: dict[str, str]

    def _to_proto(self) -> expressions_pb2.CategoricalExpression:
        return expressions_pb2.CategoricalExpression(
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
class EnumDatasourceChannelExpr(exprs.EnumExpr):
    _datasource_rid: str
    _channel_name: str
    _tags: dict[str, str]

    def _to_proto(self) -> expressions_pb2.CategoricalExpression:
        return expressions_pb2.CategoricalExpression(
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
class EnumRunChannelExpr(exprs.EnumExpr):
    _run_rid: str
    _data_scope_name: str
    _channel_name: str
    _additional_tags: dict[str, str]

    def _to_proto(self) -> expressions_pb2.CategoricalExpression:
        return expressions_pb2.CategoricalExpression(
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
class FilterExpr(exprs.EnumExpr):
    _node: exprs.EnumExpr
    _ranges: exprs.RangeExpr

    def _to_proto(self) -> expressions_pb2.CategoricalExpression:
        return expressions_pb2.CategoricalExpression(
            range_filter=expressions_pb2.CategoricalRangeFilterExpression(
                input=self._node._to_proto(), ranges=self._ranges._to_proto()
            )
        )


@dataclass(frozen=True)
class ResampleExpr(exprs.EnumExpr):
    _node: exprs.EnumExpr
    _interval_ns: params.Nanoseconds

    def _to_proto(self) -> expressions_pb2.CategoricalExpression:
        return expressions_pb2.CategoricalExpression(
            resample=expressions_pb2.CategoricalResampleExpression(
                input=self._node._to_proto(),
                interval=params._duration_ns_to_proto(self._interval_ns),
                fill=categorical_parameters_pb2.CategoricalResampleFill(
                    forward_fill=numeric_parameters_pb2.ForwardFillResample()
                ),
            )
        )


@dataclass(frozen=True)
class ShiftExpr(exprs.EnumExpr):
    _node: exprs.EnumExpr
    _duration_ns: params.Nanoseconds

    def _to_proto(self) -> expressions_pb2.CategoricalExpression:
        return expressions_pb2.CategoricalExpression(
            time_shift=expressions_pb2.CategoricalTimeShiftExpression(
                input=self._node._to_proto(),
                duration=params._duration_ns_to_proto(self._duration_ns),
            )
        )


@dataclass(frozen=True)
class TimeFilterExpr(exprs.EnumExpr):
    _node: exprs.EnumExpr
    _start_ns: params.NanosecondsUTC
    _end_ns: params.NanosecondsUTC

    def _to_proto(self) -> expressions_pb2.CategoricalExpression:
        return expressions_pb2.CategoricalExpression(
            time_range_filter=expressions_pb2.CategoricalTimeRangeFilterExpression(
                input=self._node._to_proto(),
                start=params._timestamp_to_proto(self._start_ns),
                end=params._timestamp_to_proto(self._end_ns),
            )
        )


@dataclass(frozen=True)
class UnionExpr(exprs.EnumExpr):
    _nodes: list[exprs.EnumExpr]
    _operation: params.EnumUnionOperationLiteral

    def _to_proto(self) -> expressions_pb2.CategoricalExpression:
        return expressions_pb2.CategoricalExpression(
            union=expressions_pb2.CategoricalUnionExpression(
                inputs=[node._to_proto() for node in self._nodes],
                operation=params._enum_union_operation_to_proto(self._operation),
            )
        )


@dataclass(frozen=True)
class ValueMapExpr(exprs.EnumExpr):
    _node: exprs.NumericExpr
    _mapping: dict[tuple[float | None, float | None], str]
    _default: str | None

    def _to_proto(self) -> expressions_pb2.CategoricalExpression:
        return expressions_pb2.CategoricalExpression(
            numeric_mapping=expressions_pb2.CategoricalNumericMappingExpression(
                input=self._node._to_proto(),
                mapping=[
                    expressions_pb2.NumericCategoricalInterval(
                        start=None if start is None else params._float_to_proto(start),
                        end=None if end is None else params._float_to_proto(end),
                        value=constants_pb2.StringConstant(literal=output),
                    )
                    for (start, end), output in self._mapping.items()
                ],
                default_value=None if self._default is None else constants_pb2.StringConstant(literal=self._default),
            )
        )
