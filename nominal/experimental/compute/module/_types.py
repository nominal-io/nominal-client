from __future__ import annotations
from functools import wraps
import inspect
from dataclasses import dataclass, field
from typing import Callable, Mapping, TypeAlias, ParamSpec, TypeVar, cast, get_type_hints

from nominal_api import scout_compute_api
from nominal.experimental.compute.dsl import params
from nominal.experimental.compute.dsl.exprs import Expr, NumericExpr, RangeExpr
from nominal.experimental.compute.module._functions import (
    _validate_signature,
    _create_function_parameters,
    _series_to_parameter_value,
)
import nominal_api.module as module_api

THIS_MODULE_NAME_CONSTANT = scout_compute_api.StringConstant("$THIS.MODULE_NAME")
THIS_MODULE_VERSION_CONSTANT = scout_compute_api.StringConstant("$THIS.MODULE_VERSION")


ModuleVariables: TypeAlias = dict[str, Expr]
P = ParamSpec("P")
RetExpr = TypeVar("RetExpr", bound=Expr)


@dataclass(frozen=True)
class Function:
    """A function in the module."""

    name: str
    node: Expr
    description: str
    parameters: list[module_api.FunctionParameter]
    is_exported: bool

    def _to_conjure(self) -> module_api.Function:
        if isinstance(self.node, NumericExpr):
            function_node = module_api.FunctionNode(numeric=self.node._to_conjure())
        elif isinstance(self.node, RangeExpr):
            function_node = module_api.FunctionNode(ranges=self.node._to_conjure())
        else:
            raise TypeError(f"Function node must be NumericExpr or RangeExpr, got {type(self.node).__name__}")

        return module_api.Function(
            name=self.name,
            function_node=function_node,
            is_exported=self.is_exported,
            parameters=self.parameters,
            description=self.description,
        )


@dataclass(frozen=True)
class Module:
    name: str
    description: str
    parameters: dict[str, params.StringVariable]
    variables: ModuleVariables
    functions: dict[str, Function] = field(default_factory=dict)

    def func(self, f: Callable[P, RetExpr]) -> Callable[P, RetExpr]:
        sig = inspect.signature(f)
        hints = get_type_hints(f)
        _validate_signature(sig, hints)

        # bind module variables to the function to get a parameterized compute expression
        kwargs = {param.name: self.variables[param.name] for param in sig.parameters.values()}
        expr = f(**kwargs)  # type: ignore

        self.functions[f.__name__] = Function(
            name=f.__name__,
            node=expr,
            description=f.__doc__ or "",
            parameters=list(_create_function_parameters(sig, hints)),
            is_exported=True,
        )

        @wraps(f)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> RetExpr:
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            for param_name, arg in bound_args.arguments.items():
                expected_type = hints[param_name]
                if not isinstance(arg, expected_type):
                    raise TypeError(
                        f"Argument '{param_name}' must be of type {expected_type.__name__}, got {type(arg).__name__}"
                    )
            if isinstance(expr, NumericExpr):
                return cast(RetExpr, NumericFunctionCallExpr(_name=f.__name__, _parameters=bound_args.arguments))
            elif isinstance(expr, RangeExpr):
                return cast(RetExpr, RangeFunctionCallExpr(_name=f.__name__, _parameters=bound_args.arguments))
            raise ValueError(f"Function return type {type(expr).__name__} is not a NumericExpr or RangeExpr")

        return wrapper

    def _to_conjure_create_module_request(self) -> module_api.CreateModuleRequest:
        return module_api.CreateModuleRequest(
            definition=module_api.ModuleVersionDefinition(
                default_variables=[],
                parameters=[
                    module_api.ModuleParameter(name=key, type=module_api.ValueType.ASSET_RID)
                    for key in self.parameters.keys()
                ],
                functions=[func._to_conjure() for func in self.functions.values()],
            ),
            description=self.description,
            name=self.name,
            title=self.name,
        )


@dataclass(frozen=True)
class NumericFunctionCallExpr(NumericExpr):
    """Call a function from the module."""

    _name: str
    _parameters: Mapping[str, NumericExpr | RangeExpr]

    def _to_conjure(self) -> scout_compute_api.NumericSeries:
        return scout_compute_api.NumericSeries(
            derived=scout_compute_api.DerivedSeries(
                function=scout_compute_api.FunctionDerivedSeries(
                    function_args={
                        name: _series_to_parameter_value(param._to_conjure())
                        for name, param in self._parameters.items()
                    },
                    function_name=scout_compute_api.StringConstant(literal=self._name),
                    module_name=THIS_MODULE_NAME_CONSTANT,
                    version_reference=scout_compute_api.ModuleVersionReference(
                        pinned=scout_compute_api.PinnedModuleVersionReference(version=THIS_MODULE_VERSION_CONSTANT)
                    ),
                ),
            )
        )


@dataclass(frozen=True)
class RangeFunctionCallExpr(RangeExpr):
    """Call a function from the module."""

    _name: str
    _parameters: Mapping[str, NumericExpr | RangeExpr]

    def _to_conjure(self) -> scout_compute_api.RangeSeries:
        return scout_compute_api.RangeSeries(
            derived=scout_compute_api.DerivedSeries(
                function=scout_compute_api.FunctionDerivedSeries(
                    function_args={
                        name: _series_to_parameter_value(param._to_conjure())
                        for name, param in self._parameters.items()
                    },
                    function_name=scout_compute_api.StringConstant(literal=self._name),
                    module_name=THIS_MODULE_NAME_CONSTANT,
                    version_reference=scout_compute_api.ModuleVersionReference(
                        pinned=scout_compute_api.PinnedModuleVersionReference(version=THIS_MODULE_VERSION_CONSTANT)
                    ),
                ),
            )
        )
