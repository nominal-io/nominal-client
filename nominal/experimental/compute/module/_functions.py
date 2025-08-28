from __future__ import annotations
from typing import Type
from nominal.core.client import NominalClient
from nominal.experimental.compute.dsl.exprs import NumericExpr, RangeExpr
from nominal.experimental.compute.module._types import Module, ModuleApplication, UserModuleDefnT
from nominal_api import module as module_api


def _expr_to_func_node(expr: NumericExpr | RangeExpr) -> module_api.FunctionNode:
    if isinstance(expr, NumericExpr):
        return module_api.FunctionNode(numeric=expr._to_conjure())
    return module_api.FunctionNode(ranges=expr._to_conjure())


def register(client: NominalClient, module_cls: Type[UserModuleDefnT]) -> Module:
    meta = module_cls.__module_metadata__
    instance = module_cls()
    functions = []
    for export in meta.exports:
        expr = export.fn(instance)
        functions.append(
            module_api.Function(
                description=export.description,
                function_node=_expr_to_func_node(expr),
                is_exported=True,
                name=export.name,
                parameters=[],
            )
        )
    request = module_api.CreateModuleRequest(
        definition=module_api.ModuleVersionDefinition(
            default_variables=[],
            functions=functions,
            parameters=[
                module_api.ModuleParameter(name=name, type=module_api.ValueType.ASSET_RID)
                for name in meta.params.keys()
            ],
        ),
        description=meta.description,
        name=meta.name,
        title=meta.name,
    )
    service = client._clients.client_factory(module_api.ModuleService)
    module = service.create_module(client._clients.auth_header, request)
    return Module._from_conjure(module)


def apply(client: NominalClient, module: Module, *, asset: str) -> ModuleApplication:
    service = client._clients.client_factory(module_api.ModuleService)
    request = module_api.CreateModuleApplicationRequest(
        module_rid=module.rid,
        asset_rid=asset,
    )
    resp = service.create_module_application(client._clients.auth_header, request)
    return ModuleApplication._from_conjure(resp.result)
