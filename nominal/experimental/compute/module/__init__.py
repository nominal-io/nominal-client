from __future__ import annotations
from typing import Any, Type
from nominal.experimental.compute.dsl import params

from nominal.experimental.compute.module._types import (
    _ExportedFunction,
    _ExportedFunctionT,
    _ModuleDefnProtocol,
    _ModuleMetadata,
)
from nominal.experimental.compute.module._types import Module, ModuleApplication
from nominal.experimental.compute.module._functions import apply, register

__all__ = [
    "Module",
    "ModuleApplication",
    "apply",
    "defn",
    "export",
    "register",
]


def defn(cls: Type[Any]) -> Type[_ModuleDefnProtocol]:
    exports: list[_ExportedFunction] = []
    module_params: dict[str, params.StringVariable] = {}
    for attr_name, attr in cls.__dict__.items():
        if getattr(attr, "__module_export__", False):
            exports.append(
                _ExportedFunction(
                    name=attr_name,
                    description=attr.__doc__ or "",
                    fn=attr,
                )
            )
        if isinstance(attr, params.StringVariable) and attr == params.StringVariable("ASSET_RID"):
            if module_params:
                raise Exception("Currently only one module parameter is allowed (asset)")
            module_params[attr_name] = attr
    meta = _ModuleMetadata(
        name=cls.__name__,
        description=cls.__doc__ or "",
        params=module_params,
        exports=exports,
    )
    setattr(cls, "__module_metadata__", meta)
    return cls


def export(fn: _ExportedFunctionT) -> _ExportedFunctionT:
    fn.__module_export__ = True
    return fn
