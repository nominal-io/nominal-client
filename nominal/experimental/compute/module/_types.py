from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, Generic, ParamSpec, Protocol, TypeVar, runtime_checkable

from nominal_api import module as module_api

from nominal.experimental.compute.dsl import exprs, params


@runtime_checkable
class _ModuleDefnProtocol(Protocol):
    __module_metadata__: _ModuleMetadata


UserModuleDefnT = TypeVar("UserModuleDefnT", bound=_ModuleDefnProtocol)


_Params = ParamSpec("_Params")
_RT = TypeVar("_RT", covariant=True)


class _ExportedFunctionProtocol(Protocol, Generic[_Params, _RT]):
    __module_export__: bool

    def __call__(self, *args: _Params.args, **kwargs: _Params.kwargs) -> _RT: ...


_ExportedFunctionT = TypeVar("_ExportedFunctionT", bound=_ExportedFunctionProtocol[Any, Any])


@dataclass(frozen=True)
class _ExportedFunction:
    name: str
    description: str
    fn: Callable[[_ModuleDefnProtocol], exprs.NumericExpr | exprs.RangeExpr]


@dataclass(frozen=True)
class _ModuleMetadata:
    name: str
    description: str
    params: dict[str, params.StringVariable]
    exports: list[_ExportedFunction]


@dataclass(frozen=True)
class Module:
    rid: str
    name: str
    title: str
    description: str

    @classmethod
    def _from_conjure(cls, module: module_api.Module) -> "Module":
        return cls(
            rid=module.metadata.rid,
            name=module.metadata.name,
            title=module.metadata.title,
            description=module.metadata.description,
        )


@dataclass(frozen=True)
class ModuleApplication:
    rid: str
    module_rid: str

    @classmethod
    def _from_conjure(cls, application: module_api.ModuleApplication) -> ModuleApplication:
        return cls(rid=application.rid, module_rid=application.module.rid)
