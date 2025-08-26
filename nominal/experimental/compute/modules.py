from __future__ import annotations
from dataclasses import dataclass
from typing import Self, Sequence, Type, TypeAlias, TypeVar
from nominal.core.client import NominalClient
from nominal.experimental.compute.dsl import params
from nominal.experimental.compute.dsl.exprs import NumericExpr

AssetRid: TypeAlias = str
Asset: TypeAlias = AssetRid | params.StringVariable


ModuleT = TypeVar("ModuleT", bound=Module)


@module.defn
class MyModule:
    asset: Asset = module.param("asset", Asset)

    @module.export
    def my_function(self) -> NumericExpr:
        c1 = NumericExpr.channel(self.asset, "scope", "channel1")
        c2 = NumericExpr.channel(self.asset, "scope", "channel2")
        return c1 + c2


def register_module(client: NominalClient, module_cls: Type[Module]) -> None:
    module = module_cls.from_assets(params.StringVariable("ASSET_RID"))  # need to register the asset params
    exports = {"my_function", module.my_function()}  # need to register the exported funtions
    # api call ...


def apply_module(client: NominalClient, module: Type[ModuleT], assets: Sequence[str]) -> ModuleT: ...
