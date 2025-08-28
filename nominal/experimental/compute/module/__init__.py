from __future__ import annotations

from typing import Any, Type, TypeVar

from nominal.experimental.compute.dsl import params
from nominal.experimental.compute.module._functions import apply, register
from nominal.experimental.compute.module._types import (
    Module,
    ModuleApplication,
    _ExportedFunction,
    _ExportedFunctionProtocol,
    _ModuleDefnProtocol,
    _ModuleMetadata,
)

__all__ = [
    "Module",
    "ModuleApplication",
    "apply",
    "defn",
    "export",
    "register",
]


_ExportedFunctionT = TypeVar("_ExportedFunctionT", bound=_ExportedFunctionProtocol[Any, Any])


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
    # Alternative implementation: subclass from the passed in class, and add __module_metadata__ classvar.
    # However, I couldn't get the types to work nicely. Ideally you want to return an intersection type
    # like T & _ModuleDefnProtocol which isn't supported with Python types.
    return cls


def export(fn: _ExportedFunctionT) -> _ExportedFunctionT:
    fn.__module_export__ = True
    # Alternative implementation: create a class, set __call__ to fn, and add __module_export__ classvar.
    return fn
