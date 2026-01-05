from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Iterable, Literal, Mapping, Protocol, Sequence, TypeAlias

from nominal_api import (
    event,
    scout,
    scout_asset_api,
    scout_assets,
    scout_run_api,
)
from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._event_types import EventType, SearchEventOriginType
from nominal.core._utils.api_tools import (
    HasRid,
    Link,
    LinkDict,
    RefreshableMixin,
    create_links,
    rid_from_instance_or_string,
)
from nominal.core._utils.pagination_tools import search_runs_by_asset_paginated
from nominal.core.attachment import Attachment, _iter_get_attachments
from nominal.core.connection import Connection, _get_connections
from nominal.core.dataset import Dataset, _create_dataset, _DatasetWrapper, _get_datasets
from nominal.core.datasource import DataSource
from nominal.core.event import Event, _create_event, _search_events
from nominal.core.video import Video, _create_video, _get_video
from nominal.ts import IntegralNanosecondsDuration, IntegralNanosecondsUTC, _SecondsNanos

ScopeType: TypeAlias = Connection | Dataset | Video
ScopeTypeSpecifier: TypeAlias = Literal["connection", "dataset", "video"]

logger = logging.getLogger(__name__)


def _filter_scopes(
    scopes: Sequence[scout_asset_api.DataScope], scope_type: ScopeTypeSpecifier
) -> Sequence[scout_asset_api.DataScope]:
    return [scope for scope in scopes if scope.data_source.type.lower() == scope_type]


def _filter_scope_rids(
    scopes: Sequence[scout_asset_api.DataScope], scope_type: ScopeTypeSpecifier
) -> Mapping[str, str]:
    return {
        scope.data_scope_name: getattr(scope.data_source, scope_type) for scope in _filter_scopes(scopes, scope_type)
    }


@dataclass(frozen=True)
class Asset(_DatasetWrapper, HasRid, RefreshableMixin[scout_asset_api.Asset]):
    rid: str
    name: str
    description: str | None
    properties: Mapping[str, str]
    labels: Sequence[str]
    created_at: IntegralNanosecondsUTC

    _clients: _Clients = field(repr=False)

    class _Clients(
        DataSource._Clients,
        Video._Clients,
        Attachment._Clients,
        Event._Clients,
        HasScoutParams,
        Protocol,
    ):
        @property
        def assets(self) -> scout_assets.AssetService: ...
        @property
        def run(self) -> scout.RunService: ...
        @property
        def event(self) -> event.EventService: ...

    @property
    def nominal_url(self) -> str:
        """Returns a link to the page for this Asset in the Nominal app"""
        return f"{self._clients.app_base_url}/assets/{self.rid}"

    def _get_latest_api(self) -> scout_asset_api.Asset:
        response = self._clients.assets.get_assets(self._clients.auth_header, [self.rid])
        if len(response) == 0 or self.rid not in response:
            raise ValueError(f"no asset found with RID {self.rid!r}: {response!r}")
        if len(response) > 1:
            raise ValueError(f"multiple assets found with RID {self.rid!r}: {response!r}")
        return response[self.rid]

    def _list_dataset_scopes(self) -> Sequence[scout_asset_api.DataScope]:
        return _filter_scopes(self._get_latest_api().data_scopes, "dataset")

    def _scope_rids(self, scope_type: ScopeTypeSpecifier) -> Mapping[str, str]:
        asset = self._get_latest_api()
        return _filter_scope_rids(asset.data_scopes, scope_type)

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
        api_asset = self._clients.assets.update_asset(self._clients.auth_header, request, self.rid)
        return self._refresh_from_api(api_asset)

    def promote(self) -> Self:
        """Promote this asset to be a standard, searchable, and displayable asset.

        This method is only useful for assets that were created implicitly from creating a run directly on a dataset.
        Nothing will happen from calling this method (aside from a logged warning) if called on a non-staged
        asset (e.g. an asset created by create_asset, or an asset that's already been promoted).
        """
        if self._get_latest_api().is_staged:
            request = scout_asset_api.UpdateAssetRequest(is_staged=False)
            updated_asset = self._clients.assets.update_asset(self._clients.auth_header, request, self.rid)
            self._refresh_from_api(updated_asset)
        else:
            logger.warning("Not promoting asset %s-- already promoted!", self.rid)

        return self

    def get_data_scope(self, data_scope_name: str) -> ScopeType:
        """Retrieve a datascope by data scope name, or raise ValueError if one is not found."""
        for scope, data in self.list_data_scopes():
            if scope == data_scope_name:
                return data

        raise ValueError(f"No such data scope found on asset {self.rid} with data_scope_name {data_scope_name}")

    def list_data_scopes(self) -> Sequence[tuple[str, ScopeType]]:
        """List scopes associated with this asset.

        Returns:
            (data_scope_name, scope) pairs, where scope can be a dataset, connection, or video.
        """
        return (*self.list_datasets(), *self.list_connections(), *self.list_videos())

    def remove_data_scopes(
        self,
        *,
        names: Sequence[str] | None = None,
        scopes: Sequence[ScopeType | str] | None = None,
    ) -> None:
        """Remove data scopes from this asset.

        Args:
            names: Names of datascopes to remove
            scopes: Rids or instances of scope types (dataset, video, connection) to remove.
        """
        scope_names_to_remove = names or []
        data_scopes_to_remove = scopes or []

        scope_rids_to_remove = {rid_from_instance_or_string(ds) for ds in data_scopes_to_remove}
        conjure_asset = self._get_latest_api()

        data_scopes_to_keep = [
            scout_asset_api.CreateAssetDataScope(
                data_scope_name=ds.data_scope_name,
                data_source=ds.data_source,
                series_tags=ds.series_tags,
                offset=ds.offset,
            )
            for ds in conjure_asset.data_scopes
            if ds.data_scope_name not in scope_names_to_remove
            and all(
                rid not in scope_rids_to_remove
                for rid in (ds.data_source.dataset, ds.data_source.connection, ds.data_source.video)
            )
        ]

        updated_asset = self._clients.assets.update_asset(
            self._clients.auth_header,
            scout_asset_api.UpdateAssetRequest(
                data_scopes=data_scopes_to_keep,
            ),
            self.rid,
        )
        self._refresh_from_api(updated_asset)

    def add_dataset(
        self,
        data_scope_name: str,
        dataset: Dataset | str,
        *,
        series_tags: Mapping[str, str] | None = None,
    ) -> None:
        """Add a dataset to this asset.

        Assets map "data_scope_name" (their name within the asset) to a Dataset (or dataset rid). The same type of
        datasets should use the same data scope name across assets, since checklists and templates use data scope names
        to reference datasets.

        Args:
            data_scope_name: logical name for the data scope within the asset
            dataset: dataset to add to the asset
            series_tags: Key-value tags to pre-filter the dataset with before adding to the asset.
        """
        request = scout_asset_api.AddDataScopesToAssetRequest(
            data_scopes=[
                scout_asset_api.CreateAssetDataScope(
                    data_scope_name=data_scope_name,
                    data_source=scout_run_api.DataSource(dataset=rid_from_instance_or_string(dataset)),
                    series_tags={**series_tags} if series_tags else {},
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

    def add_connection(
        self,
        data_scope_name: str,
        connection: Connection | str,
        *,
        series_tags: Mapping[str, str] | None = None,
    ) -> None:
        """Add a connection to this asset.

        Data_scope_name maps "data scope name" (the name within the asset) to a Connection (or connection rid). The same
        type of connection should use the same data scope name across assets, since checklists and templates use data
        scope names to reference connections.

        Args:
            data_scope_name: logical name for the data scope within the asset
            connection: connection to add to the asset
            series_tags: Key-value tags to pre-filter the connection with before adding to the asset.
        """
        request = scout_asset_api.AddDataScopesToAssetRequest(
            data_scopes=[
                scout_asset_api.CreateAssetDataScope(
                    data_scope_name=data_scope_name,
                    data_source=scout_run_api.DataSource(connection=rid_from_instance_or_string(connection)),
                    series_tags={**series_tags} if series_tags else {},
                )
            ]
        )
        self._clients.assets.add_data_scopes_to_asset(self.rid, self._clients.auth_header, request)

    def add_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        """Add attachments that have already been uploaded to this asset.

        `attachments` can be `Attachment` instances, or attachment RIDs.
        """
        rids = [rid_from_instance_or_string(a) for a in attachments]
        request = scout_asset_api.UpdateAttachmentsRequest(attachments_to_add=rids, attachments_to_remove=[])
        self._clients.assets.update_asset_attachments(self._clients.auth_header, request, self.rid)

    def get_or_create_dataset(
        self,
        data_scope_name: str,
        *,
        name: str | None = None,
        description: str | None = None,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        prefix_tree_delimiter: str | None = None,
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

            if prefix_tree_delimiter is not None:
                dataset.set_channel_prefix_tree(prefix_tree_delimiter)

            self.add_dataset(data_scope_name, dataset)
            return dataset

    def get_or_create_video(
        self,
        data_scope_name: str,
        *,
        name: str | None = None,
        description: str | None = None,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
    ) -> Video:
        """Retrieve a video by data scope name, or create a new one if it does not exist."""
        try:
            return self.get_video(data_scope_name)
        except ValueError:
            response = _create_video(
                self._clients.auth_header,
                self._clients.video,
                name or data_scope_name,
                description=description,
                properties=properties,
                labels=labels,
                workspace_rid=self._clients.workspace_rid,
            )
            video = Video._from_conjure(self._clients, response)
            self.add_video(data_scope_name, video)
            return video

    def create_event(
        self,
        name: str,
        type: EventType,
        start: datetime.datetime | IntegralNanosecondsUTC,
        duration: datetime.timedelta | IntegralNanosecondsDuration = 0,
        *,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> Event:
        """Create an event associated with this Asset at a given point in time.

        Args:
            name: Name of the event
            type: Verbosity level of the event.
            start: Starting timestamp of the event
            duration: Duration of the event, or 0 for an event without duration.
            description: Optionally, a human readable description of the event to create
            properties: Key-value pairs to use as properties on the created event
            labels: Sequence of labels to use on the created event.

        Returns:
            The created event that is associated with the asset.
        """
        return _create_event(
            self._clients,
            name=name,
            type=type,
            start=start,
            duration=duration,
            description=description,
            assets=[self],
            properties=properties,
            labels=labels,
        )

    def create_run(
        self,
        name: str,
        start: datetime.datetime | IntegralNanosecondsUTC,
        end: datetime.datetime | IntegralNanosecondsUTC | None,
        *,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] = (),
        links: Sequence[str | Link | LinkDict] = (),
        attachments: Iterable[Attachment] | Iterable[str] = (),
    ) -> Run:
        """Create a run associated with this Asset for a given span of time.

        Args:
            name: Name of the run.
            start: Starting timestamp of the run.
            end: Ending timestamp of the run, or None for an unbounded run.
            description: Optionally, a human readable description of the run to create.
            properties: Key-value pairs to use as properties on the created run.
            labels: Sequence of labels to use on the created run.
            links: Link metadata to add to the created run.
            attachments: Attachments to associate with the created run.

        Returns:
            Returns the created run
        """
        return _create_run(
            self._clients,
            name=name,
            start=start,
            end=end,
            description=description,
            properties=properties,
            labels=labels,
            links=links,
            attachments=attachments,
            asset_rids=[self.rid],
        )

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

    def list_datasets(self) -> Sequence[tuple[str, Dataset]]:
        """List the datasets associated with this asset.
        Returns (data_scope_name, dataset) pairs for each dataset.
        """
        scope_rid = self._scope_rids(scope_type="dataset")
        if not scope_rid:
            return []

        datasets_map = {
            dataset.rid: dataset
            for dataset in _get_datasets(self._clients.auth_header, self._clients.catalog, scope_rid.values())
        }
        return [
            (name, Dataset._from_conjure(self._clients, datasets_map[rid]))
            for name, rid in scope_rid.items()
            if rid in datasets_map
        ]

    def list_connections(self) -> Sequence[tuple[str, Connection]]:
        """List the connections associated with this asset.
        Returns (data_scope_name, connection) pairs for each connection.
        """
        scope_rid = self._scope_rids(scope_type="connection")
        connections_meta = _get_connections(self._clients, list(scope_rid.values()))
        return [
            (scope, Connection._from_conjure(self._clients, connection))
            for (scope, connection) in zip(scope_rid.keys(), connections_meta)
        ]

    def list_videos(self) -> Sequence[tuple[str, Video]]:
        """List the videos associated with this asset.
        Returns (data_scope_name, dataset) pairs for each video.
        """
        scope_rid = self._scope_rids(scope_type="video")
        return [
            (scope, Video._from_conjure(self._clients, _get_video(self._clients, rid)))
            for (scope, rid) in scope_rid.items()
        ]

    def _iter_list_attachments(self) -> Iterable[Attachment]:
        asset = self._get_latest_api()
        for a in _iter_get_attachments(self._clients.auth_header, self._clients.attachment, asset.attachments):
            yield Attachment._from_conjure(self._clients, a)

    def list_attachments(self) -> Sequence[Attachment]:
        return list(self._iter_list_attachments())

    def list_runs(self) -> Sequence[Run]:
        """List all runs associated with this Asset."""
        return [
            Run._from_conjure(self._clients, run)
            for run in search_runs_by_asset_paginated(
                self._clients.run,
                self._clients.auth_header,
                self.rid,
            )
        ]

    def search_events(
        self,
        *,
        search_text: str | None = None,
        after: str | datetime.datetime | IntegralNanosecondsUTC | None = None,
        before: str | datetime.datetime | IntegralNanosecondsUTC | None = None,
        labels: Iterable[str] | None = None,
        properties: Mapping[str, str] | None = None,
        created_by_rid: str | None = None,
        workbook_rid: str | None = None,
        data_review_rid: str | None = None,
        assignee_rid: str | None = None,
        event_type: EventType | None = None,
        origin_types: Iterable[SearchEventOriginType] | None = None,
    ) -> Sequence[Event]:
        """Search for events associated with this Asset. See nominal.core.event._search_events for details."""
        return _search_events(
            self._clients,
            search_text=search_text,
            after=after,
            before=before,
            asset_rids=[self.rid],
            labels=labels,
            properties=properties,
            created_by_rid=created_by_rid,
            workbook_rid=workbook_rid,
            data_review_rid=data_review_rid,
            assignee_rid=assignee_rid,
            event_type=event_type,
            origin_types=origin_types,
        )

    def remove_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        """Remove attachments from this asset.
        Does not remove the attachments from Nominal.

        `attachments` can be `Attachment` instances, or attachment RIDs.
        """
        rids = [rid_from_instance_or_string(a) for a in attachments]
        request = scout_asset_api.UpdateAttachmentsRequest(attachments_to_add=[], attachments_to_remove=rids)
        self._clients.assets.update_asset_attachments(self._clients.auth_header, request, self.rid)

    def archive(self) -> None:
        """Archive this asset.
        Archived assets are not deleted, but are hidden from the UI.
        """
        self._clients.assets.archive(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """Unarchive this asset, allowing it to be viewed in the UI."""
        self._clients.assets.unarchive(self._clients.auth_header, self.rid)

    @classmethod
    def _from_conjure(cls, clients: _Clients, asset: scout_asset_api.Asset) -> Self:
        return cls(
            rid=asset.rid,
            name=asset.title,
            description=asset.description,
            properties=MappingProxyType(asset.properties),
            labels=tuple(asset.labels),
            created_at=_SecondsNanos.from_flexible(asset.created_at).to_nanoseconds(),
            _clients=clients,
        )


# Moving to bottom to deal with circular dependencies
from nominal.core.run import Run, _create_run  # noqa: E402
