from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Iterable, Mapping, Protocol, Sequence, cast

from nominal_api import (
    scout_asset_api,
    scout_assets,
    scout_run_api,
)
from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._conjure_utils import Link, create_links
from nominal.core._utils import HasRid, rid_from_instance_or_string, update_dataclass
from nominal.core.attachment import Attachment, _iter_get_attachments
from nominal.core.data_scope_container import (
    ScopeType,
    ScopeTypeSpecifier,
    _DataScopeContainer,
)
from nominal.core.dataset import Dataset, _create_dataset


@dataclass(frozen=True)
class Asset(HasRid, _DataScopeContainer):
    rid: str
    name: str
    description: str | None
    properties: Mapping[str, str]
    labels: Sequence[str]

    _clients: _Clients = field(repr=False)

    class _Clients(
        _DataScopeContainer._Clients,
        HasScoutParams,
        Protocol,
    ):
        @property
        def assets(self) -> scout_assets.AssetService: ...

    @property
    def nominal_url(self) -> str:
        """Returns a link to the page for this Asset in the Nominal app"""
        # TODO (drake): move logic into _from_conjure() factory function to accomodate different URL schemes
        return f"https://app.gov.nominal.io/assets/{self.rid}"

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
        links: Sequence[str] | Sequence[Link] | None = None,
    ) -> Self:
        """Replace asset metadata.
        Updates the current instance, and returns it.
        Only the metadata passed in will be replaced, the rest will remain untouched.

        Links can be URLs or tuples of (URL, name).

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
            links=None if links is None else create_links(links),
        )
        response = self._clients.assets.update_asset(self._clients.auth_header, request, self.rid)
        asset = self.__class__._from_conjure(self._clients, response)
        update_dataclass(self, asset, fields=self.__dataclass_fields__)
        return self

    def _get_asset(self) -> scout_asset_api.Asset:
        response = self._clients.assets.get_assets(self._clients.auth_header, [self.rid])
        if len(response) == 0 or self.rid not in response:
            raise ValueError(f"no asset found with RID {self.rid!r}: {response!r}")
        if len(response) > 1:
            raise ValueError(f"multiple assets found with RID {self.rid!r}: {response!r}")
        return response[self.rid]

    def _rids_by_scope_name(self, stype: ScopeTypeSpecifier) -> Mapping[str, str]:
        asset = self._get_asset()
        rid_attrib = {"dataset": "dataset", "logset": "log_set", "connection": "connection", "video": "video"}
        return {
            scope.data_scope_name: cast(str, getattr(scope.data_source, rid_attrib[stype]))
            for scope in asset.data_scopes
            if scope.data_source.type.lower() == stype
        }

    def _add_data_scope(
        self,
        scope_name: str,
        scope: HasRid | str,
        scope_type: ScopeTypeSpecifier,
        *,
        series_tags: Mapping[str, str] | None = None,
        offset: datetime.timedelta | None = None,
    ) -> None:
        offset_duration = None
        if offset:
            seconds, nanos = divmod(offset.total_seconds(), 1)
            offset_duration = scout_run_api.Duration(nanos=int(nanos * 1e9), seconds=int(seconds))

        param_names = {"dataset": "dataset", "logset": "log_set", "connection": "connection", "video": "video"}
        datasource_args = {param_names[scope_type]: rid_from_instance_or_string(scope)}
        request = scout_asset_api.AddDataScopesToAssetRequest(
            data_scopes=[
                scout_asset_api.CreateAssetDataScope(
                    data_scope_name=scope_name,
                    data_source=scout_run_api.DataSource(**datasource_args),
                    series_tags={**series_tags} if series_tags else {},
                    offset=offset_duration,
                )
            ],
        )
        self._clients.assets.add_data_scopes_to_asset(self.rid, self._clients.auth_header, request)

    def add_attachment(self, attachment: Attachment | str) -> None:
        """Add attachments that have already been uploaded to this asset.

        `attachments` can be `Attachment` instances, or attachment RIDs.
        """
        request = scout_asset_api.UpdateAttachmentsRequest(
            attachments_to_add=[rid_from_instance_or_string(attachment)], attachments_to_remove=[]
        )
        self._clients.assets.update_asset_attachments(self._clients.auth_header, request, self.rid)

    def attachments(self) -> Iterable[Attachment]:
        asset = self._get_asset()
        for a in _iter_get_attachments(self._clients.auth_header, self._clients.attachment, asset.attachments):
            yield Attachment._from_conjure(self._clients, a)

    # Backcompat
    _iter_list_attachments = attachments

    def list_attachments(self) -> Sequence[Attachment]:
        return list(self.attachments())

    def remove_attachments(self, attachments: Iterable[Attachment | str]) -> None:
        """Remove attachments from this asset.
        Does not remove the attachments from Nominal.

        `attachments` can be `Attachment` instances, or attachment RIDs.
        """
        rids = [rid_from_instance_or_string(a) for a in attachments]
        request = scout_asset_api.UpdateAttachmentsRequest(attachments_to_add=[], attachments_to_remove=rids)
        self._clients.assets.update_asset_attachments(self._clients.auth_header, request, self.rid)

    def get_or_create_dataset(
        self,
        data_scope_name: str,
        *,
        name: str | None = None,
        description: str | None = None,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
    ) -> Dataset:
        """Retrieve a dataset by data scope name, or create a new one if it does not exist."""
        try:
            return self.get_dataset(data_scope_name)
        except ValueError:
            enriched_dataset = _create_dataset(
                self._clients.auth_header,
                self._clients.catalog,
                name or data_scope_name,
                description=description,
                properties=properties,
                labels=labels,
                workspace_rid=self._clients.workspace_rid,
            )
            dataset = Dataset._from_conjure(self._clients, enriched_dataset)
            self.add_dataset(data_scope_name, dataset)
            return dataset

    def archive(self) -> None:
        """Archive this asset.
        Archived assets are not deleted, but are hidden from the UI.
        """
        self._clients.assets.archive(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """Unarchive this asset, allowing it to be viewed in the UI."""
        self._clients.assets.unarchive(self._clients.auth_header, self.rid)

    def remove_data_scopes(
        self,
        *,
        names: Sequence[str] | None = None,
        scopes: Sequence[ScopeType | str] | None = None,
    ) -> None:
        """Remove data scopes from this asset.

        `names` are scope names.
        `scopes` are rids or scope objects.
        """
        data_scope_names = set() if names is None else set(names)
        data_sources = set() if scopes is None else set([rid_from_instance_or_string(scope) for scope in scopes])

        if isinstance(data_sources, str):
            raise RuntimeError("Expect `data_sources` to be a sequence, not a string")

        data_source_rids = {rid_from_instance_or_string(ds) for ds in data_sources}

        conjure_asset = self._get_asset()

        data_sources_to_keep = [
            scout_asset_api.CreateAssetDataScope(
                data_scope_name=ds.data_scope_name,
                data_source=ds.data_source,
                series_tags=ds.series_tags,
                offset=ds.offset,
            )
            for ds in conjure_asset.data_scopes
            if ds.data_scope_name not in data_scope_names
            and (ds.data_source.dataset or ds.data_source.connection or ds.data_source.video or ds.data_source.log_set)
            not in data_source_rids
        ]

        response = self._clients.assets.update_asset(
            self._clients.auth_header,
            scout_asset_api.UpdateAssetRequest(
                data_scopes=data_sources_to_keep,
            ),
            self.rid,
        )
        asset = self.__class__._from_conjure(self._clients, response)
        update_dataclass(self, asset, fields=self.__dataclass_fields__)

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
