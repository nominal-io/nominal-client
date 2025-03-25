from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Iterable, Literal, Mapping, Protocol, Sequence, cast

from nominal_api import (
    scout_asset_api,
    scout_assets,
    scout_run_api,
)
from typing_extensions import Self, TypeAlias, deprecated

from nominal.core._clientsbunch import HasAuthHeader
from nominal.core._conjure_utils import Link, _build_links
from nominal.core._utils import HasRid, rid_from_instance_or_string, update_dataclass
from nominal.core.attachment import Attachment, _iter_get_attachments
from nominal.core.connection import Connection, _get_connections
from nominal.core.dataset import Dataset, _get_datasets
from nominal.core.datasource import DataSource
from nominal.core.log import LogSet, _get_log_set
from nominal.core.video import Video, _get_video

ScopeType: TypeAlias = "Connection | Dataset | LogSet | Video"


@dataclass(frozen=True)
class Asset(HasRid):
    rid: str
    name: str
    description: str | None
    properties: Mapping[str, str]
    labels: Sequence[str]

    _clients: _Clients = field(repr=False)

    class _Clients(
        DataSource._Clients,
        Video._Clients,
        LogSet._Clients,
        Attachment._Clients,
        HasAuthHeader,
        Protocol,
    ):
        @property
        def assets(self) -> scout_assets.AssetService: ...

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
            links=_build_links(links),
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

        Assets map "data_scope_name" (their name within the asset) to a Dataset (or dataset rid). The same type of
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

    def add_video(self, data_scope_name: str, video: Video | str) -> None:
        """Add a video to this asset.

        Assets map "data_scope_name" (name within the asset for the data) to a Video (or a video rid). The same type of
        videos (e.g., files from a given camera) should use the same data scope name across assets, since checklists and
        templates use data scope names to reference videos.
        """
        request = scout_asset_api.AddDataScopesToAssetRequest(
            data_scopes=[
                scout_asset_api.CreateAssetDataScope(
                    data_scope_name=data_scope_name,
                    data_source=scout_run_api.DataSource(video=rid_from_instance_or_string(video)),
                    series_tags={},
                ),
            ]
        )
        self._clients.assets.add_data_scopes_to_asset(self.rid, self._clients.auth_header, request)

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

    def _get_asset(self) -> scout_asset_api.Asset:
        response = self._clients.assets.get_assets(self._clients.auth_header, [self.rid])
        if len(response) == 0 or self.rid not in response:
            raise ValueError(f"no asset found with RID {self.rid!r}: {response!r}")
        if len(response) > 1:
            raise ValueError(f"multiple assets found with RID {self.rid!r}: {response!r}")
        return response[self.rid]

    def _scope_rid(self, stype: Literal["dataset", "video", "connection", "logset"]) -> dict[str, str]:
        asset = self._get_asset()
        rid_attrib = {"dataset": "dataset", "logset": "log_set", "connection": "connection", "video": "video"}
        return {
            scope.data_scope_name: cast(str, getattr(scope.data_source, rid_attrib[stype]))
            for scope in asset.data_scopes
            if scope.data_source.type.lower() == stype
        }

    def list_datasets(self) -> Sequence[tuple[str, Dataset]]:
        """List the datasets associated with this asset.
        Returns (data_scope_name, dataset) pairs for each dataset.
        """
        scope_rid = self._scope_rid(stype="dataset")
        datasets_meta = _get_datasets(self._clients.auth_header, self._clients.catalog, scope_rid.values())
        return [
            (scope, Dataset._from_conjure(self._clients, ds)) for (scope, ds) in zip(scope_rid.keys(), datasets_meta)
        ]

    def list_connections(self) -> Sequence[tuple[str, Connection]]:
        """List the connections associated with this asset.
        Returns (data_scope_name, connection) pairs for each connection.
        """
        scope_rid = self._scope_rid(stype="connection")
        connections_meta = _get_connections(self._clients, list(scope_rid.values()))
        return [
            (scope, Connection._from_conjure(self._clients, connection))
            for (scope, connection) in zip(scope_rid.keys(), connections_meta)
        ]

    def list_videos(self) -> Sequence[tuple[str, Video]]:
        """List the videos associated with this asset.
        Returns (data_scope_name, dataset) pairs for each video.
        """
        scope_rid = self._scope_rid(stype="video")
        return [
            (scope, Video._from_conjure(self._clients, _get_video(self._clients, rid)))
            for (scope, rid) in scope_rid.items()
        ]

    def list_logsets(self) -> Sequence[tuple[str, LogSet]]:
        """List the logsets associated with this asset.
        Returns (data_scope_name, logset) pairs for each logset.
        """
        scope_rid = self._scope_rid(stype="logset")
        return [
            (scope, LogSet._from_conjure(self._clients, _get_log_set(self._clients, rid)))
            for (scope, rid) in scope_rid.items()
        ]

    def list_data_scopes(self) -> Sequence[tuple[str, ScopeType]]:
        """List scopes associated with this asset.
        Returns (data_scope_name, scope) pairs, where scope can be
        a dataset, connection, video, or logset.
        """
        return (*self.list_datasets(), *self.list_connections(), *self.list_logsets(), *self.list_videos())

    def get_data_scope(self, data_scope_name: str) -> ScopeType:
        """Retrieve a datascope by data scope name, or raise ValueError if one is not found."""
        for scope, data in self.list_data_scopes():
            if scope == data_scope_name:
                return data

        raise ValueError(f"No such data scope found on asset {self.rid} with data_scope_name {data_scope_name}")

    def get_dataset(self, data_scope_name: str) -> Dataset:
        """Retrieve a dataset by data scope name, or raise ValueError if one is not found."""
        dataset = self.get_data_scope(data_scope_name)
        if isinstance(dataset, Dataset):
            return dataset
        else:
            raise ValueError(f"Data scope {data_scope_name} on asset {self.rid} is not a dataset")

    def get_connection(self, data_scope_name: str) -> Connection:
        """Retrieve a connection by data scope name, or raise ValueError if one is not found."""
        connection = self.get_data_scope(data_scope_name)
        if isinstance(connection, Connection):
            return connection
        else:
            raise ValueError(f"Data scope {data_scope_name} on asset {self.rid} is not a connection")

    def get_video(self, data_scope_name: str) -> Video:
        """Retrieve a video by data scope name, or raise ValueError if one is not found."""
        video = self.get_data_scope(data_scope_name)
        if isinstance(video, Video):
            return video
        else:
            raise ValueError(f"Data scope {data_scope_name} on asset {self.rid} is not a video")

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

    def unarchive(self) -> None:
        """Unarchive this asset, allowing it to be viewed in the UI."""
        self._clients.assets.unarchive(self._clients.auth_header, self.rid)

    def _remove_data_sources(
        self,
        *,
        data_scope_names: Sequence[str] | None = None,
        data_sources: Sequence[ScopeType | str] | None = None,
    ) -> None:
        data_scope_names = data_scope_names or []
        data_sources = data_sources or []

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

    @deprecated("Use `remove_data_scopes` instead")
    def remove_data_sources(
        self,
        *,
        data_scope_names: Sequence[str] | None = None,
        data_sources: Sequence[ScopeType | str] | None = None,
    ) -> None:
        """Remove data sources from this asset.

        The list data_sources can contain Connection, Dataset, Video instances, or rids as string.
        """
        self._remove_data_sources(data_scope_names=data_scope_names, data_sources=data_sources)

    # Newer alias to replace `remove_data_sources`
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
        self._remove_data_sources(data_scope_names=names, data_sources=scopes)

    def add_connection(
        self, data_scope_name: str, connection: Connection | str, *, series_tags: dict[str, str] | None = None
    ) -> None:
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
                    series_tags=series_tags or {},
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
