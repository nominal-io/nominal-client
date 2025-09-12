from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from functools import wraps
from typing import Callable, Mapping, Protocol, TypeVar, cast, get_type_hints

from nominal_api import module as module_api
from nominal_api import scout_compute_api
from typing_extensions import ParamSpec, TypeAlias

from nominal.core import NominalClient
from nominal.core._clientsbunch import HasScoutParams
from nominal.experimental.compute.dsl import params
from nominal.experimental.compute.dsl.exprs import Expr, NumericExpr, RangeExpr
from nominal.experimental.compute.module._utils import (
    _create_function_parameters,
    _expr_to_value_type,
    _series_to_variable_value,
    _to_compute_node_with_context,
    _validate_signature,
)

THIS_MODULE_NAME_CONSTANT = scout_compute_api.StringConstant("$THIS.MODULE_NAME")
THIS_MODULE_VERSION_CONSTANT = scout_compute_api.StringConstant("$THIS.MODULE_VERSION")


ModuleVariables: TypeAlias = "dict[str, NumericExpr | RangeExpr]"
P = ParamSpec("P")
RetExpr = TypeVar("RetExpr", NumericExpr, RangeExpr)


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
class ModuleDefinition:
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
        # TODO: remove numeric expr hardcoding
        kwargs = {param.name: NumericExpr.reference(param.name) for param in sig.parameters.values()}
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

    def register(self, client: NominalClient) -> Module:
        if client._clients.workspace_rid is None:
            raise ValueError("Workspace RID must be set on the client")
        request = _create_module_request(self, client._clients.workspace_rid)
        service = client._clients.client_factory(module_api.ModuleService)
        module = service.create_module(client._clients.auth_header, request)
        return Module._from_conjure(client._clients, module.metadata)


def _create_module_request(defn: ModuleDefinition, workspace_rid: str) -> module_api.CreateModuleRequest:
    return module_api.CreateModuleRequest(
        definition=_create_module_version_definition(defn),
        description=defn.description,
        name=defn.name,
        title=defn.name,
        workspace=workspace_rid,
    )


def _create_module_version_definition(defn: ModuleDefinition) -> module_api.ModuleVersionDefinition:
    return module_api.ModuleVersionDefinition(
        default_variables=[
            module_api.ModuleVariable(
                name=key,
                type=_expr_to_value_type(expr),
                value=scout_compute_api.VariableValue(compute_node=_to_compute_node_with_context(expr._to_conjure())),
            )
            for key, expr in defn.variables.items()
        ],
        parameters=[  # TODO: handle other parameter names
            module_api.ModuleParameter(name="asset_rid", type=module_api.ValueType.ASSET_RID)
            for _ in defn.parameters.keys()
        ],
        functions=[func._to_conjure() for func in defn.functions.values()],
    )


@dataclass(frozen=True)
class Module:
    rid: str
    name: str
    title: str
    description: str
    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def module(self) -> module_api.ModuleService: ...

    @classmethod
    def _from_conjure(cls, clients: _Clients, metadata: module_api.ModuleMetadata) -> Module:
        return Module(
            rid=metadata.rid,
            name=metadata.name,
            title=metadata.title,
            description=metadata.description,
            _clients=clients,
        )

    def apply(self, *, asset: str) -> ModuleApplication:
        request = module_api.CreateModuleApplicationRequest(
            module_rid=self.rid,
            asset_rid=asset,
        )
        resp = self._clients.module.create_module_application(self._clients.auth_header, request)
        return ModuleApplication._from_conjure(resp.result)

    def update(self, defn: ModuleDefinition) -> Module:
        if self._clients.workspace_rid is None:
            raise ValueError("Workspace RID must be set on the client")
        request = module_api.UpdateModuleRequest(
            definition=_create_module_version_definition(defn),
            description=self.description,
            title=self.title,
        )
        module = self._clients.module.update_module(self._clients.auth_header, self.rid, request)
        return Module._from_conjure(self._clients, module.metadata)


@dataclass(frozen=True)
class ModuleApplication:
    rid: str
    module_rid: str
    asset_rid: str

    @classmethod
    def _from_conjure(cls, module_application: module_api.ModuleApplication) -> ModuleApplication:
        return ModuleApplication(
            rid=module_application.rid,
            module_rid=module_application.module.rid,
            asset_rid=module_application.asset_rid,
        )


# TODO: should these go in the DSL?
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
                        name: scout_compute_api.FunctionParameterValue(
                            value=_series_to_variable_value(param._to_conjure())
                        )
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
                        name: scout_compute_api.FunctionParameterValue(
                            value=_series_to_variable_value(param._to_conjure())
                        )
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
