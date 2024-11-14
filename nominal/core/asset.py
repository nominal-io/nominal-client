from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Iterable, Mapping, Protocol, Sequence, cast

from typing_extensions import Self

from nominal._api.combined import attachments_api, scout_asset_api, scout_assets, scout_run_api
from nominal.core._clientsbunch import HasAuthHeader
from nominal.core._utils import HasRid, rid_from_instance_or_string, update_dataclass
from nominal.core.attachment import Attachment, _iter_get_attachments
from nominal.core.connection import Connection
from nominal.core.dataset import Dataset, _get_datasets
from nominal.core.log import LogSet
from nominal.core.video import Video


@dataclass(frozen=True)
class Asset(HasRid):
    rid: str
    name: str
    description: str | None
    properties: Mapping[str, str]
    labels: Sequence[str]

    _clients: _Clients = field(repr=False)

    class _Clients(Dataset._Clients, HasAuthHeader, Protocol):
        @property
        def assets(self) -> scout_assets.AssetService: ...
        @property
        def attachment(self) -> attachments_api.AttachmentService: ...

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

    @property
    def nominal_url(self) -> str:
        """Returns a link to the page for this Asset in the Nominal app"""
        # TODO (drake): move logic into _from_conjure() factory function to accomodate different URL schemes
        return f"https://app.gov.nominal.io/assets/{self.rid}"

    def add_dataset(self, data_scope_name: str, dataset: Dataset | str) -> None:
        """Add a dataset to this asset.

        Datasets map "data_scope_name" (their name within the asset) to a Dataset (or dataset rid). The same type of
        datasets should use the same data scope name across assets, since checklists and templates use data scope names
        to reference datasets.
        """
        # TODO(alkasm): support series tags & offset
        request = scout_asset_api.AddDataScopesToAssetRequest(
            data_scopes=[
                scout_asset_api.CreateAssetDataScope(
                    data_scope_name=data_scope_name,
                    data_source=scout_run_api.DataSource(dataset=rid_from_instance_or_string(dataset)),
                    series_tags={},
                )
            ],
        )
        self._clients.assets.add_data_scopes_to_asset(self.rid, self._clients.auth_header, request)

    def _get_asset(self) -> scout_asset_api.Asset:
        response = self._clients.assets.get_assets(self._clients.auth_header, [self.rid])
        if len(response) == 0 or self.rid not in response:
            raise ValueError(f"no asset found with RID {self.rid!r}: {response!r}")
        if len(response) > 1:
            raise ValueError(f"multiple assets found with RID {self.rid!r}: {response!r}")
        return response[self.rid]

    def _iter_list_datasets(self) -> Iterable[tuple[str, Dataset]]:
        asset = self._get_asset()
        dataset_rids_by_data_scope_name = {}
        for scope in asset.data_scopes:
            if scope.data_source.type == "dataset":
                dataset_rid = cast(str, scope.data_source.dataset)
                dataset_rids_by_data_scope_name[scope.data_scope_name] = dataset_rid
        datasets_by_rids = {
            ds.rid: Dataset._from_conjure(self._clients, ds)
            for ds in _get_datasets(
                self._clients.auth_header, self._clients.catalog, dataset_rids_by_data_scope_name.values()
            )
        }
        for data_scope_name, rid in dataset_rids_by_data_scope_name.items():
            dataset = datasets_by_rids[rid]
            yield (data_scope_name, dataset)

    def list_datasets(self) -> Sequence[tuple[str, Dataset]]:
        """List the datasets associated with this asset.
        Returns (data_scope_name, dataset) pairs for each dataset.
        """
        return list(self._iter_list_datasets())

    def add_log_set(self, data_scope_name: str, log_set: LogSet | str) -> None:
        """Add a log set to this asset.

        Log sets map "ref names" (their name within the run) to a Log set (or log set rid).
        """
        # TODO(alkasm): support series tags & offset
        request = scout_asset_api.AddDataScopesToAssetRequest(
            data_scopes=[
                scout_asset_api.CreateAssetDataScope(
                    data_scope_name=data_scope_name,
                    data_source=scout_run_api.DataSource(log_set=rid_from_instance_or_string(log_set)),
                    series_tags={},
                )
            ],
        )
        self._clients.assets.add_data_scopes_to_asset(self.rid, self._clients.auth_header, request)

    def add_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        """Add attachments that have already been uploaded to this asset.

        `attachments` can be `Attachment` instances, or attachment RIDs.
        """
        rids = [rid_from_instance_or_string(a) for a in attachments]
        request = scout_asset_api.UpdateAttachmentsRequest(attachments_to_add=rids, attachments_to_remove=[])
        self._clients.assets.update_asset_attachments(self._clients.auth_header, request, self.rid)

    def remove_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        """Remove attachments from this asset.
        Does not remove the attachments from Nominal.

        `attachments` can be `Attachment` instances, or attachment RIDs.
        """
        rids = [rid_from_instance_or_string(a) for a in attachments]
        request = scout_asset_api.UpdateAttachmentsRequest(attachments_to_add=[], attachments_to_remove=rids)
        self._clients.assets.update_asset_attachments(self._clients.auth_header, request, self.rid)

    def _iter_list_attachments(self) -> Iterable[Attachment]:
        asset = self._get_asset()
        for a in _iter_get_attachments(self._clients.auth_header, self._clients.attachment, asset.attachments):
            yield Attachment._from_conjure(self._clients, a)

    def list_attachments(self) -> Sequence[Attachment]:
        return list(self._iter_list_attachments())

    def archive(self) -> None:
        """Archive this asset.
        Archived assets are not deleted, but are hidden from the UI.
        """
        self._clients.assets.archive(self._clients.auth_header, self.rid)

    def remove_data_sources(
        self,
        *,
        data_scope_names: Sequence[str] | None = None,
        data_sources: Sequence[Connection | Dataset | Video | str] | None = None,
    ) -> None:
        """Remove data sources from this asset.

        The list data_sources can contain Connection, Dataset, Video instances, or rids as string.
        """
        data_scope_names = data_scope_names or []
        data_source_rids = {rid_from_instance_or_string(ds) for ds in data_sources or []}

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
            and (ds.data_source.dataset or ds.data_source.connection or ds.data_source.video) not in data_source_rids
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

    def add_connection(self, data_scope_name: str, connection: Connection | str) -> None:
        """Add a connection to this asset.

        Data_scope_name maps "data scope name" (the name within the asset) to a Connection (or connection rid). The same
        type of connection should use the same data scope name across assets, since checklists and templates use data
        scope names to reference connections.
        """
        # TODO(alkasm): support series tags & offset
        request = scout_asset_api.AddDataScopesToAssetRequest(
            data_scopes=[
                scout_asset_api.CreateAssetDataScope(
                    data_scope_name=data_scope_name,
                    data_source=scout_run_api.DataSource(connection=rid_from_instance_or_string(connection)),
                    series_tags={},
                    offset=None,
                )
            ]
        )
        self._clients.assets.add_data_scopes_to_asset(self.rid, self._clients.auth_header, request)

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
