from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Iterable, Literal, Mapping, Protocol, Sequence, cast

from nominal_api import attachments_api, scout_asset_api, scout_assets, scout_datasource_connection, scout_run_api
from typing_extensions import Self

from nominal.core._clientsbunch import HasAuthHeader
from nominal.core._conjure_utils import Link, _build_links
from nominal.core._utils import HasRid, rid_from_instance_or_string, update_dataclass
from nominal.core.attachment import Attachment, _iter_get_attachments
from nominal.core.connection import Connection, _get_connections
from nominal.core.dataset import Dataset, _get_datasets
from nominal.core.log import LogSet, _get_log_set
from nominal.core.video import Video, _get_video


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
        def connection(self) -> scout_datasource_connection.ConnectionService: ...
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

    def _iter_list_scopes(
        self, scope_types: tuple[Literal["dataset", "logset", "connection", "video"], ...] | None = None
    ) -> Iterable[tuple[str, Dataset | Video | Connection | LogSet]]:
        asset = self._get_asset()

        rid_attrib = {"dataset": "dataset", "logset": "log_set", "connection": "connection", "video": "video"}

        if scope_types is None:
            scope_types = tuple(rid_attrib)

        scopes_by_type = defaultdict(list)

        for scope in asset.data_scopes:
            stype = scope.data_source.type.lower()
            dataset_rid = cast(str, getattr(scope.data_source, rid_attrib[stype]))
            scopes_by_type[stype].append((scope.data_scope_name, dataset_rid))

        datasets = {}
        if "dataset" in scope_types:
            datasets.update(
                {
                    ds.rid: Dataset._from_conjure(self._clients, ds)
                    for ds in _get_datasets(
                        self._clients.auth_header,
                        self._clients.catalog,
                        (rid for (scope, rid) in scopes_by_type["dataset"]),
                    )
                }
            )

        connections = {}
        if "connection" in scope_types:
            connections.update(
                {
                    connection.rid: Connection._from_conjure(self._clients, connection)
                    for connection in _get_connections(
                        self._clients, (rid for (scope, rid) in scopes_by_type["connection"])
                    )
                }
            )

        logsets = {}
        if "logset" in scope_types:
            for scope, rid in scopes_by_type["logset"]:
                logset_meta = _get_log_set(self._clients, rid)
                logsets[logset_meta.rid] = LogSet._from_conjure(self._clients, logset_meta)

        videos = {}
        if "video" in scope_types:
            for scope, rid in scopes_by_type["video"]:
                video_meta = _get_video(self._clients, rid)
                videos[video_meta.rid] = Video._from_conjure(self._clients, video_meta)

        for scopes in (datasets, connections, logsets, videos):
            yield from scopes.items()

    def list_datasets(self) -> Sequence[tuple[str, Dataset]]:
        """List the datasets associated with this asset.
        Returns (data_scope_name, dataset) pairs for each dataset.
        """
        return list(self._iter_list_scopes(scope_types=("dataset")))

    def list_data_sources(self) -> Sequence[tuple[str, Connection | Dataset | LogSet | Video]]:
        """List the data sources associated with this asset.
        Returns (data_scope_name, scope) pairs for each dataset,
        where scope can be a Dataset, LogSet, Video, or Connection.
        """
        return list(self._iter_list_scopes())

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

    def unarchive(self) -> None:
        """Unarchive this asset, allowing it to be viewed in the UI."""
        self._clients.assets.unarchive(self._clients.auth_header, self.rid)

    def remove_data_sources(
        self,
        *,
        data_scope_names: Sequence[str] | None = None,
        data_sources: Sequence[Connection | Dataset | LogSet | Video | str] | str | None = None,
    ) -> None:
        """Remove data sources from this asset.

        The list data_sources can contain Connection, Dataset, Video instances, or rids as string.
        """
        data_scope_names = data_scope_names or []

        if isinstance(data_sources, str):
            data_sources = [data_sources]
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
