from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from nominal.core._clientsbunch import ON_BEHALF_OF_USER_RID_HEADER
from nominal.core.client import NominalClient
from nominal.core.dataset import Dataset


@dataclass(frozen=True)
class DatasetImpersonationClient:
    """Experimental client wrapper for dataset creation on behalf of another user."""

    _client: NominalClient
    _user_rid: str

    @property
    def user_rid(self) -> str:
        return self._user_rid

    def create_dataset(
        self,
        name: str,
        *,
        description: str | None = None,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        prefix_tree_delimiter: str | None = None,
    ) -> Dataset:
        impersonated_client = self._derived_client()
        return impersonated_client.create_dataset(
            name,
            description=description,
            labels=labels,
            properties=properties,
            prefix_tree_delimiter=prefix_tree_delimiter,
        )

    def _derived_client(self) -> NominalClient:
        clients = self._client._clients.with_catalog_request_headers({ON_BEHALF_OF_USER_RID_HEADER: self._user_rid})
        return NominalClient(_clients=clients, _profile=self._client._profile)


def as_user(client: NominalClient, user_rid: str) -> DatasetImpersonationClient:
    """Return an experimental wrapper for dataset creation on behalf of another user."""
    return DatasetImpersonationClient(_client=client, _user_rid=user_rid)
