from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping, Protocol, Sequence

from typing_extensions import Self

from nominal._api.combined import scout_asset_api, scout_assets
from nominal.core._clientsbunch import HasAuthHeader
from nominal.core._utils import HasRid, update_dataclass


@dataclass(frozen=True)
class Asset(HasRid):
    rid: str
    name: str
    description: str | None
    properties: Mapping[str, str]
    labels: Sequence[str]

    _clients: _Clients = field(repr=False)

    class _Clients(HasAuthHeader, Protocol):
        @property
        def assets(self) -> scout_assets.AssetService: ...

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> Self:
        """Replace asset metadata.
        Updates the current instance, and returns it.
        Only the metadata passed in will be replaced, the rest will remain untouched.

        Note: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
        calling this method. E.g.:

            new_labels = ["new-label-a", "new-label-b"]
            for old_label in asset.labels:
                new_labels.append(old_label)
            asset = asset.update(labels=new_labels)
        """
        request = scout_asset_api.UpdateAssetRequest(
            description=description,
            labels=None if labels is None else list(labels),
            properties=None if properties is None else dict(properties),
            title=name,
        )
        response = self._clients.assets.update_asset(self._clients.auth_header, request, self.rid)
        asset = self.__class__._from_conjure(self._clients, response)
        update_dataclass(self, asset, fields=self.__dataclass_fields__)
        return self

    @classmethod
    def _from_conjure(cls, clients: _Clients, asset: scout_asset_api.Asset) -> Self:
        return cls(
            rid=asset.rid,
            name=asset.title,
            description=asset.description,
            properties=MappingProxyType(asset.properties),
            labels=tuple(asset.labels),
            _clients=clients,
        )
