from __future__ import annotations

import inspect
from typing import Any, Iterable

from nominal_api import scout_compute_api
from nominal.experimental.compute.dsl.exprs import Expr, NumericExpr, RangeExpr
import nominal_api.module as module_api


def _series_to_parameter_value(
    series: scout_compute_api.NumericSeries | scout_compute_api.RangeSeries,
) -> scout_compute_api.FunctionParameterValue:
    if isinstance(series, scout_compute_api.NumericSeries):
        return _empty_context_wrap(scout_compute_api.ComputeNode(numeric=series))
    return _empty_context_wrap(scout_compute_api.ComputeNode(ranges=series))


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
            yield module_api.FunctionParameter(name=param.name, type=module_api.ValueType.NUMERIC_SERIES)
        elif issubclass(param_type, RangeExpr):
            yield module_api.FunctionParameter(name=param.name, type=module_api.ValueType.RANGES_SERIES)
        else:
            raise ValueError(f"Parameter '{param.name}' must be a NumericExpr or RangeExpr")


# def register(client: NominalClient, module_cls: Module) -> Module:
#     meta = module_cls.__module_metadata__
#     instance = module_cls()
#     functions = []
#     for export in meta.exports:
#         expr = export.fn(instance)
#         functions.append(
#             module_api.Function(
#                 description=export.description,
#                 function_node=_expr_to_func_node(expr),
#                 is_exported=True,
#                 name=export.name,
#                 parameters=[],
#             )
#         )
#     request = module_api.CreateModuleRequest(
#         definition=module_api.ModuleVersionDefinition(
#             default_variables=[],
#             functions=functions,
#             parameters=[
#                 module_api.ModuleParameter(name=name, type=module_api.ValueType.ASSET_RID)
#                 for name in meta.params.keys()
#             ],
#         ),
#         description=meta.description,
#         name=meta.name,
#         title=meta.name,
#     )
#     service = client._clients.client_factory(module_api.ModuleService)
#     module = service.create_module(client._clients.auth_header, request)
#     return Module._from_conjure(module)


# def apply(client: NominalClient, module: Module, *, asset: str) -> ModuleApplication:
#     service = client._clients.client_factory(module_api.ModuleService)
#     request = module_api.CreateModuleApplicationRequest(
#         module_rid=module.rid,
#         asset_rid=asset,
#     )
#     resp = service.create_module_application(client._clients.auth_header, request)
#     return ModuleApplication._from_conjure(resp.result)
