from __future__ import annotations

import inspect
from typing import Any, Iterable

from nominal_api import scout_compute_api
import nominal_api.module as module_api
from nominal.experimental.compute.dsl.exprs import Expr, NumericExpr, RangeExpr


def _series_to_parameter_value(
    series: scout_compute_api.NumericSeries | scout_compute_api.RangeSeries,
) -> scout_compute_api.FunctionParameterValue:
    if isinstance(series, scout_compute_api.NumericSeries):
        return _empty_context_wrap(scout_compute_api.ComputeNode(numeric=series))
    return _empty_context_wrap(scout_compute_api.ComputeNode(ranges=series))


def _series_to_variable_value(
    series: scout_compute_api.NumericSeries | scout_compute_api.RangeSeries,
) -> scout_compute_api.VariableValue:
    if isinstance(series, scout_compute_api.NumericSeries):
        return scout_compute_api.VariableValue(
            compute_node=_empty_context_wrap(scout_compute_api.ComputeNode(numeric=series))
        )
    return scout_compute_api.VariableValue(
        compute_node=_empty_context_wrap(scout_compute_api.ComputeNode(ranges=series))
    )


def _empty_context_wrap(node: scout_compute_api.ComputeNode) -> scout_compute_api.ComputeNodeWithContext:
    return scout_compute_api.ComputeNodeWithContext(context=scout_compute_api.Context(variables={}), series_node=node)


def _validate_signature(sig: inspect.Signature, hints: dict[str, Any]) -> None:
    for param in sig.parameters.values():
        if param.kind == inspect.Parameter.POSITIONAL_ONLY:
            raise ValueError(f"Positional-only parameter '{param.name}' is not allowed in module functions")
        if param.kind == inspect.Parameter.VAR_POSITIONAL:
            raise ValueError(f"Varargs parameter '*{param.name}' is not allowed in module functions")
        if param.kind == inspect.Parameter.KEYWORD_ONLY:
            raise ValueError(f"Keyword-only parameter '{param.name}' is not allowed in module functions")
        if param.default != inspect.Parameter.empty:
            raise ValueError(f"Default value for parameter '{param.name}' is not allowed in module functions")
        if param.name not in hints:
            raise ValueError(f"Parameter '{param.name}' must be annotated in the function signature")
        ann = hints[param.name]
        if not isinstance(ann, type) or not issubclass(ann, Expr):
            raise TypeError(f"Parameter '{param.name}' annotation must be an Expr subtype; got {ann!r}")
    ret_ann = hints["return"]
    if not isinstance(ret_ann, type) or not issubclass(ret_ann, Expr):
        raise TypeError(f"Return annotation must be an Expr subtype; got {ret_ann!r}")


def _create_function_parameters(
    sig: inspect.Signature, hints: dict[str, Any]
) -> Iterable[module_api.FunctionParameter]:
    for param in sig.parameters.values():
        param_type = hints[param.name]
        if issubclass(param_type, NumericExpr):
            yield module_api.FunctionParameter(
                name=param.name, type=module_api.ValueType.NUMERIC_SERIES, default_value=param.name
            )
        elif issubclass(param_type, RangeExpr):
            yield module_api.FunctionParameter(
                name=param.name, type=module_api.ValueType.RANGES_SERIES, default_value=param.name
            )
        else:
            raise ValueError(f"Parameter '{param.name}' must be a NumericExpr or RangeExpr")
