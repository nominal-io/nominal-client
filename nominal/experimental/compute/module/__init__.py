from __future__ import annotations
import inspect
from typing import Callable, cast

from nominal.experimental.compute.dsl import params
from nominal.experimental.compute.module._types import Module, ModuleVariables


__all__ = [
    "defn",
    "Module",
    "ModuleVariables",
]


def defn(f: Callable[[], ModuleVariables] | Callable[[params.StringVariable], ModuleVariables]) -> Module:
    sig = inspect.signature(f)
    variables = {}
    parameters = {}
    if len(sig.parameters) == 0:
        variables = cast(Callable[[], ModuleVariables], f)()
    elif len(sig.parameters) == 1:
        (parameter,) = sig.parameters.values()
        asset_variable = params.StringVariable("ASSET_RID")
        parameters = {parameter.name: asset_variable}
        variables = cast(Callable[[params.StringVariable], ModuleVariables], f)(asset_variable)
    else:
        raise ValueError(f"Module definition function {f.__name__} must take 0 or 1 parameters")
    return Module(name=f.__name__, description=f.__doc__ or "", parameters=parameters, variables=variables)
