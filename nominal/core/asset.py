from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Iterable, Mapping, Protocol, Sequence, TypeAlias

from nominal_api import (
    scout,
)
from typing_extensions import Self, deprecated

from nominal.core import data_review, streaming_checklist
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._event_types import EventType, SearchEventOriginType
from nominal.core._utils.api_tools import (
    HasRid,
    Link,
    LinkDict,
    RefreshableGrpcMixin,
    ScopeTypeSpecifier,
    create_proto_links,
    rid_from_instance_or_string,
)
from nominal.core._utils.frontend_urls import asset_url
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.pagination_tools import search_runs_by_asset_paginated
from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.core.attachment import Attachment, _iter_get_attachments
from nominal.core.connection import Connection, _get_connection, _get_connections
from nominal.core.dataset import Dataset, _create_dataset, _DatasetWrapper, _get_dataset, _get_datasets
from nominal.core.datasource import DataSource
from nominal.core.event import Event, _create_event, _search_events
from nominal.core.exceptions import LegacyVideoDeprecationWarning, NominalNotFoundError
from nominal.core.video import Video, _create_video, _get_video
from nominal.core.workbook import Workbook, _search_workbooks
from nominal.protos.asset.v2 import asset_pb2, asset_pb2_grpc
from nominal.protos.comments.v1 import comments_pb2_grpc
from nominal.protos.types import types_pb2
from nominal.ts import IntegralNanosecondsDuration, IntegralNanosecondsUTC

ScopeType: TypeAlias = Connection | Dataset | Video

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Asset(_DatasetWrapper, HasRid, RefreshableGrpcMixin[asset_pb2.Asset]):
    rid: str
    name: str
    description: str | None
    properties: Mapping[str, str]
    labels: Sequence[str]
    created_at: IntegralNanosecondsUTC
    updated_at: IntegralNanosecondsUTC
    is_archived: bool

    _clients: _Clients = field(repr=False)
    created_by_rid: str | None = field(default=None, repr=False)

    class _Clients(
        DataSource._Clients,
        Video._Clients,
        Attachment._Clients,
        Event._Clients,
        Workbook._Clients,
        data_review.DataReview._Clients,
        HasScoutParams,
        Protocol,
    ):
        @property
        def assets(self) -> asset_pb2_grpc.AssetServiceStub: ...
        @property
        def comments(self) -> comments_pb2_grpc.CommentsServiceStub: ...
        @property
        def run(self) -> scout.RunService: ...

    @property
    def nominal_url(self) -> str:
        """Returns a link to the page for this Asset in the Nominal app"""
        return asset_url(self._clients, self.rid)

    def _get_latest_api(self) -> asset_pb2.Asset:
        return _get_asset(self._clients, self.rid)

    def _apply_update(self, request: asset_pb2.UpdateAssetRequest) -> Self:
        """Send an update and refresh this instance from the returned asset."""
        with translate_grpc_errors():
            response = self._clients.assets.UpdateAsset(request)
        return self._refresh_from_api(response.asset)

    def _dataset_scopes(self) -> Sequence[asset_pb2.DataScope]:
        return _filter_proto_scopes(self._get_latest_api().data_scopes, "dataset")

    def _lookup_dataset_scope(self, data_scope_name: str) -> tuple[str, Mapping[str, str]] | None:
        for scope in self._dataset_scopes():
            if scope.data_scope_name == data_scope_name:
                return scope.data_source.dataset, scope.series_tags
        return None

    def _scope_rids(self, scope_type: ScopeTypeSpecifier) -> Mapping[str, str]:
        asset = self._get_latest_api()
        return {
            scope.data_scope_name: getattr(scope.data_source, scope_type)
            for scope in _filter_proto_scopes(asset.data_scopes, scope_type)
        }

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
        # None omits a field, leaving it unchanged; an empty wrapper clears it.
        updated_labels = None if labels is None else types_pb2.LabelUpdateWrapper(labels=list(labels))
        updated_properties = (
            None if properties is None else types_pb2.PropertyUpdateWrapper(properties=dict(properties))
        )
        updated_links = None if links is None else asset_pb2.LinkList(links=create_proto_links(links))

        request = asset_pb2.UpdateAssetRequest(
            asset_rid=self.rid,
            description=description,
            labels=updated_labels,
            properties=updated_properties,
            title=name,
            links=updated_links,
        )
        return self._apply_update(request)

    def promote(self) -> Self:
        """Promote this asset to be a standard, searchable, and displayable asset.

        This method is only useful for assets that were created implicitly from creating a run directly on a dataset.
        Nothing will happen from calling this method (aside from a logged warning) if called on a non-staged
        asset (e.g. an asset created by create_asset, or an asset that's already been promoted).
        """
        if self._get_latest_api().is_staged:
            self._apply_update(asset_pb2.UpdateAssetRequest(asset_rid=self.rid, is_staged=False))
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
        latest_asset = self._get_latest_api()

        data_scopes_to_keep = [
            asset_pb2.CreateAssetDataScope(
                data_scope_name=ds.data_scope_name,
                data_source=ds.data_source if ds.HasField("data_source") else None,
                series_tags=ds.series_tags,
                offset=ds.offset if ds.HasField("offset") else None,
            )
            for ds in latest_asset.data_scopes
            if ds.data_scope_name not in scope_names_to_remove
            and all(
                rid not in scope_rids_to_remove
                for rid in (ds.data_source.dataset, ds.data_source.connection, ds.data_source.video)
            )
        ]

        request = asset_pb2.UpdateAssetRequest(
            asset_rid=self.rid,
            data_scopes=asset_pb2.CreateAssetDataScopeList(data_scopes=data_scopes_to_keep),
        )
        self._apply_update(request)

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
        request = asset_pb2.AddDataScopesToAssetRequest(
            asset_rid=self.rid,
            data_scopes=[
                asset_pb2.CreateAssetDataScope(
                    data_scope_name=data_scope_name,
                    data_source=asset_pb2.DataSource(dataset=rid_from_instance_or_string(dataset)),
                    series_tags={**series_tags} if series_tags else {},
                )
            ],
        )
        with translate_grpc_errors():
            self._clients.assets.AddDataScopesToAsset(request)

    @deprecated(
        "Attaching a standalone `Video` to an asset is deprecated in favor of video channels on a dataset. Attach the "
        "dataset that carries the video channels with `Asset.add_dataset` instead.",
        category=LegacyVideoDeprecationWarning,
    )
    def add_video(self, data_scope_name: str, video: Video | str) -> None:
        """Add a video to this asset.

        Assets map "data_scope_name" (name within the asset for the data) to a Video (or a video rid). The same type of
        videos (e.g., files from a given camera) should use the same data scope name across assets, since checklists and
        templates use data scope names to reference videos.
        """
        request = asset_pb2.AddDataScopesToAssetRequest(
            asset_rid=self.rid,
            data_scopes=[
                asset_pb2.CreateAssetDataScope(
                    data_scope_name=data_scope_name,
                    data_source=asset_pb2.DataSource(video=rid_from_instance_or_string(video)),
                    series_tags={},
                )
            ],
        )
        with translate_grpc_errors():
            self._clients.assets.AddDataScopesToAsset(request)

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
        request = asset_pb2.AddDataScopesToAssetRequest(
            asset_rid=self.rid,
            data_scopes=[
                asset_pb2.CreateAssetDataScope(
                    data_scope_name=data_scope_name,
                    data_source=asset_pb2.DataSource(connection=rid_from_instance_or_string(connection)),
                    series_tags={**series_tags} if series_tags else {},
                )
            ],
        )
        with translate_grpc_errors():
            self._clients.assets.AddDataScopesToAsset(request)

    def add_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        """Add attachments that have already been uploaded to this asset.

        `attachments` can be `Attachment` instances, or attachment RIDs.
        """
        rids = [rid_from_instance_or_string(a) for a in attachments]
        request = asset_pb2.UpdateAssetAttachmentsRequest(
            asset_rid=self.rid, attachments_to_add=rids, attachments_to_remove=[]
        )
        with translate_grpc_errors():
            self._clients.assets.UpdateAssetAttachments(request)

    def get_or_create_dataset(
        self,
        data_scope_name: str,
        *,
        name: str | None = None,
        description: str | None = None,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        prefix_tree_delimiter: str | None = None,
        series_tags: Mapping[str, str] | None = None,
    ) -> Dataset:
        """Retrieve a dataset by data scope name, or create a new one if it does not exist.

        Args:
            data_scope_name: Datascope name to use when looking up or adding a dataset to an asset.
            name: Name of the dataset to create, if one is not found.
            description: Human readable description of the dataset to create, if one is not found.
            labels: Labels of the dataset to create, if one is not found.
            properties: Key-value properties of the dataset to create, if one is not found.
            prefix_tree_delimiter: The prefix tree delimiter to use with the created dataset, if one is not found.
            series_tags: Tags to filter the created dataset by in the datascope, if one is not found.

        Returns:
            The retrieved or created dataset.
        """
        # Attempt to retrieve and validate any existing dataset scope
        found_ds, found_tags = None, None
        try:
            logger.debug("Attempting to retrieve dataset scope named '%s'", data_scope_name)
            found_ds, found_tags = self._get_dataset_scope(data_scope_name)
        except ValueError:
            pass

        # If we found a dataset with the same datascope name, validate that the
        # series tags match
        if found_ds is not None and found_tags is not None:
            # symmetric difference to find tags found in one but not the other
            mismatching_tags = found_tags.items() ^ (series_tags or {}).items()
            if mismatching_tags:
                raise ValueError(
                    f"Cannot get_or_create_dataset '{data_scope_name}' with tags {series_tags}: "
                    f"datascope already exists with {found_tags} (difference={mismatching_tags})"
                )
            else:
                return found_ds

        # No such dataset exists! Create dataset
        enriched_dataset = _create_dataset(
            self._clients.auth_header,
            self._clients.catalog,
            name or data_scope_name,
            description=description,
            properties=properties,
            labels=labels,
            workspace_rid=self._clients.resolve_default_workspace_rid(),
        )
        dataset = Dataset._from_conjure(self._clients, enriched_dataset)
        if prefix_tree_delimiter is not None:
            dataset.set_channel_prefix_tree(prefix_tree_delimiter)

        # Add dataset to asset
        self.add_dataset(data_scope_name, dataset, series_tags=series_tags)

        logger.info(
            "No such dataset named '%s' found on asset '%s': created '%s",
            data_scope_name,
            self.rid,
            dataset.rid,
        )
        return dataset

    @deprecated(
        "`Asset.get_or_create_video` is deprecated in favor of video channels on a dataset. Use "
        "`Asset.get_or_create_dataset`, then `Dataset.add_video` to upload video to a channel on it.",
        category=LegacyVideoDeprecationWarning,
    )
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
                workspace_rid=self._clients.resolve_default_workspace_rid(),
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
        """Retrieve a dataset by data scope name.

        Args:
            data_scope_name: Name of the asset data scope to resolve.

        Returns:
            Dataset associated with the data scope name.

        Raises:
            ValueError: If no dataset data scope exists with the provided name.
        """
        dataset_rids_by_scope_name = self._scope_rids("dataset")
        dataset_rid = dataset_rids_by_scope_name.get(data_scope_name)
        if dataset_rid is None:
            raise ValueError(f"No dataset with data scope name '{data_scope_name}' found for this asset")

        return Dataset._from_conjure(
            self._clients,
            _get_dataset(self._clients.auth_header, self._clients.catalog, dataset_rid),
        )

    def get_connection(self, data_scope_name: str) -> Connection:
        """Retrieve a connection by data scope name.

        Args:
            data_scope_name: Name of the asset data scope to resolve.

        Returns:
            Connection associated with the data scope name.

        Raises:
            ValueError: If no connection data scope exists with the provided name.
        """
        connection_rids_by_scope_name = self._scope_rids("connection")
        connection_rid = connection_rids_by_scope_name.get(data_scope_name)
        if connection_rid is None:
            raise ValueError(f"No connection with data scope name '{data_scope_name}' found for this asset")

        return Connection._from_conjure(self._clients, _get_connection(self._clients, connection_rid))

    @deprecated(
        "Resolving a standalone `Video` data scope is deprecated in favor of video channels on a dataset. Use "
        "`Asset.get_dataset` with the data scope name, then `Dataset.list_video_files` to reach the video files.",
        category=LegacyVideoDeprecationWarning,
    )
    def get_video(self, data_scope_name: str) -> Video:
        """Retrieve a video by data scope name.

        Args:
            data_scope_name: Name of the asset data scope to resolve.

        Returns:
            Video associated with the data scope name.

        Raises:
            ValueError: If no video data scope exists with the provided name.
        """
        video_rids = self._scope_rids("video")
        video_rid = video_rids.get(data_scope_name)
        if video_rid is None:
            raise ValueError(f"No video with data scope name '{data_scope_name}' found for this asset")

        return Video._from_conjure(self._clients, _get_video(self._clients, video_rid))

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
        archive_status: ArchiveStatusFilter = ArchiveStatusFilter.NOT_ARCHIVED,
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
            archive_status=archive_status,
        )

    def search_data_reviews(
        self,
        runs: Sequence[Run | str] | None = None,
        *,
        archive_status: ArchiveStatusFilter = ArchiveStatusFilter.NOT_ARCHIVED,
    ) -> Sequence[data_review.DataReview]:
        """Search for data reviews associated with this Asset. See nominal.core.client.search_data_reviews
        for details.
        """
        return list(
            data_review._iter_search_data_reviews(
                self._clients,
                assets=[self.rid],
                runs=[rid_from_instance_or_string(run) for run in (runs or [])],
                archive_status=archive_status,
            )
        )

    def search_workbooks(
        self,
        *,
        exact_match: str | None = None,
        search_text: str | None = None,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
        created_by_rid: str | None = None,
        run_rid: str | None = None,
        include_drafts: bool = False,
        archive_status: ArchiveStatusFilter = ArchiveStatusFilter.NOT_ARCHIVED,
    ) -> Sequence[Workbook]:
        """Search for workbooks associated with this Asset.

        See ``nominal.core.NominalClient.search_workbooks`` for details.
        """
        return _search_workbooks(
            self._clients,
            exact_match=exact_match,
            search_text=search_text,
            labels=labels,
            properties=properties,
            asset_rid=self.rid,
            author_rid=created_by_rid,
            run_rid=run_rid,
            include_drafts=include_drafts,
            archive_status=archive_status,
        )

    def list_streaming_checklists(self) -> Sequence[str]:
        """List all Streaming Checklists associated with this Asset. See
        nominal.core.client.list_streaming_checklists for details.
        """
        return list(
            streaming_checklist._iter_list_streaming_checklists(
                self._clients,
                asset_rid=self.rid,
            )
        )

    def remove_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        """Remove attachments from this asset.
        Does not remove the attachments from Nominal.

        `attachments` can be `Attachment` instances, or attachment RIDs.
        """
        rids = [rid_from_instance_or_string(a) for a in attachments]
        request = asset_pb2.UpdateAssetAttachmentsRequest(
            asset_rid=self.rid, attachments_to_add=[], attachments_to_remove=rids
        )
        with translate_grpc_errors():
            self._clients.assets.UpdateAssetAttachments(request)

    def archive(self) -> None:
        """Archive this asset.
        Archived assets are not deleted, but are hidden from the UI.

        Note: this does not update the instance in place; call `refresh()` to see the change reflected.
        """
        with translate_grpc_errors():
            self._clients.assets.Archive(asset_pb2.ArchiveRequest(asset_rid=self.rid))

    def unarchive(self) -> None:
        """Unarchive this asset, allowing it to be viewed in the UI.

        Note: this does not update the instance in place; call `refresh()` to see the change reflected.
        """
        with translate_grpc_errors():
            self._clients.assets.Unarchive(asset_pb2.UnarchiveRequest(asset_rid=self.rid))

    @classmethod
    def _from_proto(cls, clients: _Clients, asset: asset_pb2.Asset) -> Self:
        return cls(
            rid=asset.rid,
            name=asset.title,
            description=asset.description or None,
            properties=MappingProxyType(dict(asset.properties)),
            labels=tuple(asset.labels),
            created_at=asset.created_at.ToNanoseconds(),
            updated_at=asset.updated_at.ToNanoseconds(),
            is_archived=asset.is_archived,
            _clients=clients,
            created_by_rid=asset.created_by or None,
        )


def _filter_proto_scopes(
    scopes: Iterable[asset_pb2.DataScope], scope_type: ScopeTypeSpecifier
) -> Sequence[asset_pb2.DataScope]:
    """The data scopes whose `data_source` is set to `scope_type`."""
    return [scope for scope in scopes if scope.data_source.WhichOneof("data_source") == scope_type]


def _get_assets(clients: Asset._Clients, rids: Sequence[str]) -> Mapping[str, asset_pb2.Asset]:
    """The assets with the given rids, keyed by rid. Rids that do not resolve are absent from the result."""
    with translate_grpc_errors():
        response = clients.assets.GetAssets(asset_pb2.GetAssetsRequest(rids=list(rids)))
    return response.responses


def _get_asset(clients: Asset._Clients, rid: str) -> asset_pb2.Asset:
    """The asset with the given rid.

    Raises:
        NominalNotFoundError: If no asset has that rid.
    """
    assets = _get_assets(clients, [rid])
    if rid not in assets:
        raise NominalNotFoundError(f"no asset found with RID {rid!r}")
    return assets[rid]


# Moving to bottom to deal with circular dependencies
from nominal.core.run import Run, _create_run  # noqa: E402
