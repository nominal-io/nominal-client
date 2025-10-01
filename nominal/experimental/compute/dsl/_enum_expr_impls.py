from __future__ import annotations

from dataclasses import dataclass

from nominal_api import scout_compute_api

from nominal.experimental.compute.dsl import exprs, params


@dataclass(frozen=True)
class EnumAssetChannelExpr(exprs.EnumExpr):
    _asset_rid: str
    _data_scope_name: str
    _channel_name: str
    _additional_tags: dict[str, str]

    def _to_conjure(self) -> scout_compute_api.EnumSeries:
        return scout_compute_api.EnumSeries(
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
class EnumDatasourceChannelExpr(exprs.EnumExpr):
    _datasource_rid: str
    _channel_name: str
    _tags: dict[str, str]

    def _to_conjure(self) -> scout_compute_api.EnumSeries:
        return scout_compute_api.EnumSeries(
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
class EnumRunChannelExpr(exprs.EnumExpr):
    _run_rid: str
    _data_scope_name: str
    _channel_name: str
    _additional_tags: dict[str, str]

    def _to_conjure(self) -> scout_compute_api.EnumSeries:
        return scout_compute_api.EnumSeries(
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
class FilterExpr(exprs.EnumExpr):
    _node: exprs.EnumExpr
    _ranges: exprs.RangeExpr

    def _to_conjure(self) -> scout_compute_api.EnumSeries:
        return scout_compute_api.EnumSeries(
            filter_transformation=scout_compute_api.EnumFilterTransformationSeries(
                input=self._node._to_conjure(),
                filter=self._ranges._to_conjure(),
            )
        )


@dataclass(frozen=True)
class ResampleExpr(exprs.EnumExpr):
    _node: exprs.EnumExpr
    _interval_ns: params.Nanoseconds

    def _to_conjure(self) -> scout_compute_api.EnumSeries:
        return scout_compute_api.EnumSeries(
            resample=scout_compute_api.EnumResampleSeries(
                input=self._node._to_conjure(),
                resample_configuration=scout_compute_api.EnumResampleConfiguration(
                    interval=params._duration_ns_to_conjure(self._interval_ns),
                    interpolation=scout_compute_api.EnumResampleInterpolationConfiguration(
                        forward_fill_resample_interpolation_configuration=scout_compute_api.ForwardFillResampleInterpolationConfiguration()
                    ),
                ),
            )
        )


@dataclass(frozen=True)
class ShiftExpr(exprs.EnumExpr):
    _node: exprs.EnumExpr
    _duration_ns: params.Nanoseconds

    def _to_conjure(self) -> scout_compute_api.EnumSeries:
        return scout_compute_api.EnumSeries(
            time_shift=scout_compute_api.EnumTimeShiftSeries(
                input=self._node._to_conjure(),
                duration=params._duration_ns_to_conjure(self._duration_ns),
            )
        )


@dataclass(frozen=True)
class TimeFilterExpr(exprs.EnumExpr):
    _node: exprs.EnumExpr
    _start_ns: params.NanosecondsUTC
    _end_ns: params.NanosecondsUTC

    def _to_conjure(self) -> scout_compute_api.EnumSeries:
        return scout_compute_api.EnumSeries(
            time_range_filter=scout_compute_api.EnumTimeRangeFilterSeries(
                input=self._node._to_conjure(),
                start_time=params._timestamp_to_conjure(self._start_ns),
                end_time=params._timestamp_to_conjure(self._end_ns),
            )
        )


@dataclass(frozen=True)
class UnionExpr(exprs.EnumExpr):
    _nodes: list[exprs.EnumExpr]
    _operation: params.EnumUnionOperationLiteral

    def _to_conjure(self) -> scout_compute_api.EnumSeries:
        return scout_compute_api.EnumSeries(
            union=scout_compute_api.EnumUnionSeries(
                input=[node._to_conjure() for node in self._nodes],
                operation=params._enum_union_operation_to_conjure(self._operation),
            )
        )


@dataclass(frozen=True)
class ValueMapExpr(exprs.EnumExpr):
    _node: exprs.NumericExpr
    _mapping: dict[tuple[float | None, float | None], str]
    _default: str | None

    def _to_conjure(self) -> scout_compute_api.EnumSeries:
        mapping = []
        for range, output in self._mapping.items():
            start, end = range
            mapping.append(
                scout_compute_api.RangeMap(
                    start=None if start is None else scout_compute_api.DoubleConstant(start),
                    end=None if end is None else scout_compute_api.DoubleConstant(end),
                    output=scout_compute_api.StringConstant(literal=output),
                )
            )

        default_constant = None
        if self._default is not None:
            default_constant = scout_compute_api.StringConstant(literal=self._default)

        return scout_compute_api.EnumSeries(
            value_map=scout_compute_api.ValueMapSeries(
                input=self._node._to_conjure(),
                mapping=mapping,
                default=default_constant,
            )
        )
