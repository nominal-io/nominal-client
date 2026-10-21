from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Protocol, Self, Sequence, TypeAlias

from typing_extensions import deprecated

from nominal.core import data_review, streaming_checklist
from nominal.core._checklist_types import Priority
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._dataset_scope_ingest import _DatasetScopeIngestMixin
from nominal.core._event_types import EventType, SearchEventOriginType
from nominal.core._scope_resolution import DataScopes, resolve_datasets, resolve_videos
from nominal.core._utils.api_tools import (
    HasRid,
    Link,
    LinkDict,
    RefreshableGrpcMixin,
    create_proto_links,
    label_update,
    rid_from_instance_or_string,
)
from nominal.core._utils.api_types import NominalProperties
from nominal.core._utils.frontend_urls import asset_url
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.pagination_tools import search_runs_by_asset_paginated
from nominal.core._utils.properties import properties_from_proto, typed_property_update
from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.core.asset_type import AssetType
from nominal.core.attachment import Attachment, _get_accessible_attachments
from nominal.core.dataset import Dataset, _create_dataset
from nominal.core.event import Event, _create_event, _search_events
from nominal.core.video import Video, _create_video
from nominal.core.workbook import Workbook, _search_workbooks
from nominal.exceptions import LegacyVideoDeprecationWarning, NominalDataScopeNotFoundError, NominalNotFoundError
from nominal.protos.asset.v2 import asset_pb2, asset_pb2_grpc
from nominal.protos.comments.v1 import comments_pb2_grpc
from nominal.protos.run.v1 import run_service_pb2_grpc
from nominal.protos.spatial.v2 import spatial_pb2_grpc
from nominal.ts import IntegralNanosecondsDuration, IntegralNanosecondsUTC

ScopeType: TypeAlias = Dataset | Video

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Asset(_DatasetScopeIngestMixin, HasRid, RefreshableGrpcMixin[asset_pb2.Asset]):
    """A logical grouping of datasets, runs, events, and attachments.

    Often a digital twin of a physical asset; it can also represent a shared concept.
    """

    rid: str
    name: str
    description: str | None
    properties: NominalProperties
    labels: Sequence[str]
    created_at: IntegralNanosecondsUTC
    is_archived: bool

    _clients: _Clients = field(repr=False)
    created_by_rid: str | None = field(default=None, repr=False)
    type_rids: Sequence[str] = ()
    """Asset type memberships in this snapshot; use ``get_types()`` to fetch their definitions."""

    class _Clients(
        Dataset._Clients,
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
        def run(self) -> run_service_pb2_grpc.RunServiceStub: ...
        @property
        def spatial(self) -> spatial_pb2_grpc.SpatialServiceStub: ...

    @property
    def nominal_url(self) -> str:
        """Returns a link to the page for this Asset in the Nominal app"""
        return asset_url(self._clients, self.rid)

    def get_types(self) -> Sequence[AssetType]:
        """Fetch definitions for this snapshot's memberships, including archived types.

        Call ``refresh()`` first to fetch changed memberships. Missing or inaccessible
        definitions raise ``NominalNotFoundError`` rather than silently disappearing.
        """
        if not self.type_rids:
            return []
        with translate_grpc_errors():
            response = self._clients.assets.GetTypes(asset_pb2.GetTypesRequest(rids=list(self.type_rids)))
        missing = set(self.type_rids) - response.responses.keys()
        if missing:
            raise NominalNotFoundError(f"Asset types not found: {sorted(missing)!r}")
        return [AssetType._from_proto(self._clients, response.responses[rid]) for rid in self.type_rids]

    def _get_latest_api(self) -> asset_pb2.Asset:
        return _get_asset(self._clients, self.rid)

    def _scopes(self) -> DataScopes:
        """This asset's data scopes, read from one fetch of the asset."""
        return DataScopes.from_data_scopes(self._get_latest_api().data_scopes)

    def _get_dataset_scope(self, data_scope_name: str) -> tuple[Dataset, Mapping[str, str]]:
        named = self._scopes().named(data_scope_name)
        dataset = resolve_datasets(self._clients, named.datasets).get(data_scope_name)
        if dataset is None:
            raise NominalDataScopeNotFoundError(f"No such data scope found with data_scope_name {data_scope_name}")
        return dataset, named.datasets[data_scope_name].series_tags

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        properties: NominalProperties | None = None,
        labels: Sequence[str] | None = None,
        links: Sequence[str] | Sequence[Link] | None = None,
        types: Sequence[AssetType | str] | None = None,
    ) -> Self:
        """Replace asset metadata.
        Updates the current instance, and returns it.
        Only the metadata passed in will be replaced, the rest will remain untouched.

        Links can be URLs or tuples of (URL, name).

        Args:
            name: Replacement name.
            description: Replacement description.
            properties: String or numeric properties. Integers are stored as floats.
                None leaves properties unchanged; an empty mapping clears them.
            labels: Replacement labels.
            links: Replacement links.
            types: Replacement memberships. None preserves memberships; an empty
                sequence removes them all. The server checks the resulting properties
                against every assigned type.

        Note:
            This replaces the metadata rather than appending it. To append to labels or properties, merge them before
            calling this method. E.g.:

            ```python
            new_labels = ["new-label-a", "new-label-b"]
            for old_label in asset.labels:
                new_labels.append(old_label)
            asset = asset.update(labels=new_labels)
            ```

        Raises:
            TypeError: If a property value is a boolean or is not a string, integer, or float.
            NominalError: If the update request fails.
        """
        updated_links = None if links is None else asset_pb2.LinkList(links=create_proto_links(links, asset_pb2.Link))
        request = asset_pb2.UpdateAssetRequest(
            asset_rid=self.rid,
            description=description,
            labels=label_update(labels),
            typed_properties=typed_property_update(properties),
            title=name,
            links=updated_links,
            types=None
            if types is None
            else asset_pb2.TypeRidList(
                types=[rid_from_instance_or_string(kind) for kind in types],
            ),
        )
        with translate_grpc_errors():
            response = self._clients.assets.UpdateAsset(request)
        return self._refresh_from_api(response.asset)

    def promote(self) -> Self:
        """Promote this asset to be a standard, searchable, and displayable asset.

        This method is only useful for assets that were created implicitly from creating a run directly on a dataset.
        Nothing will happen from calling this method (aside from a logged warning) if called on a non-staged
        asset (e.g. an asset created by create_asset, or an asset that's already been promoted).

        Raises:
            NominalError: If retrieving or promoting the asset fails.
        """
        if self._get_latest_api().is_staged:
            request = asset_pb2.UpdateAssetRequest(asset_rid=self.rid, is_staged=False)
            with translate_grpc_errors():
                response = self._clients.assets.UpdateAsset(request)
            self._refresh_from_api(response.asset)
        else:
            logger.warning("Not promoting asset %s-- already promoted!", self.rid)

        return self

    def get_data_scope(self, data_scope_name: str) -> ScopeType:
        """Retrieve the dataset or video behind a data scope name.

        Raises:
            NominalNotFoundError: If no readable data scope has this name.
        """
        scope = _resolve_scopes(self._clients, self._scopes().named(data_scope_name)).get(data_scope_name)
        if scope is None:
            raise NominalDataScopeNotFoundError(
                f"No such data scope found on asset {self.rid} with data_scope_name {data_scope_name}"
            )
        return scope

    def list_data_scopes(self) -> Sequence[tuple[str, ScopeType]]:
        """List scopes associated with this asset.

        Returns:
            (data_scope_name, scope) pairs, where scope can be a dataset or video. Data scopes
            whose resource is missing or not readable are left out.

            Spatials are not included: they live in `nominal.experimental.spatial`, whose
            `list_spatials_in_asset` lists them.
        """
        return tuple(_resolve_scopes(self._clients, self._scopes()).items())

    def remove_data_scopes(
        self,
        *,
        names: Sequence[str] | None = None,
        scopes: Sequence[ScopeType | str] | None = None,
    ) -> None:
        """Remove data scopes from this asset.

        Args:
            names: Names of datascopes to remove
            scopes: Rids or instances of scope types (dataset, video) to remove.
                A spatial can be removed by passing its rid.

        Raises:
            NominalError: If retrieving or updating the asset's data scopes fails.
        """
        scope_names_to_remove = names or []
        data_scopes_to_remove = scopes or []

        scope_rids_to_remove = {rid_from_instance_or_string(ds) for ds in data_scopes_to_remove if ds}
        latest_asset = self._get_latest_api()

        data_scopes_to_keep = []
        for ds in latest_asset.data_scopes:
            if ds.data_scope_name in scope_names_to_remove:
                continue
            if any(
                rid in scope_rids_to_remove
                for rid in (
                    ds.data_source.dataset,
                    ds.data_source.connection,
                    ds.data_source.video,
                    ds.data_source.spatial,
                )
            ):
                continue
            data_scopes_to_keep.append(
                asset_pb2.CreateAssetDataScope(
                    data_scope_name=ds.data_scope_name,
                    data_source=ds.data_source if ds.HasField("data_source") else None,
                    series_tags=ds.series_tags,
                    offset=ds.offset if ds.HasField("offset") else None,
                )
            )

        request = asset_pb2.UpdateAssetRequest(
            asset_rid=self.rid,
            data_scopes=asset_pb2.CreateAssetDataScopeList(data_scopes=data_scopes_to_keep),
        )
        with translate_grpc_errors():
            response = self._clients.assets.UpdateAsset(request)
        self._refresh_from_api(response.asset)

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

        Raises:
            NominalError: If adding the data scope fails.
        """
        self._add_data_scope(
            data_scope_name, asset_pb2.DataSource(dataset=rid_from_instance_or_string(dataset)), series_tags
        )

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

        Raises:
            NominalError: If adding the data scope fails.
        """
        self._add_data_scope(data_scope_name, asset_pb2.DataSource(video=rid_from_instance_or_string(video)))

    def _add_data_scope(
        self,
        data_scope_name: str,
        data_source: asset_pb2.DataSource,
        series_tags: Mapping[str, str] | None = None,
    ) -> None:
        """Add one data scope to this asset.

        Raises:
            NominalError: If adding the data scope fails.
        """
        request = asset_pb2.AddDataScopesToAssetRequest(
            asset_rid=self.rid,
            data_scopes=[
                asset_pb2.CreateAssetDataScope(
                    data_scope_name=data_scope_name,
                    data_source=data_source,
                    series_tags={**series_tags} if series_tags else {},
                )
            ],
        )
        with translate_grpc_errors():
            self._clients.assets.AddDataScopesToAsset(request)

    def add_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        """Add attachments that have already been uploaded to this asset.

        `attachments` can be `Attachment` instances, or attachment RIDs.

        Raises:
            NominalError: If adding the attachments fails.
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
        properties: NominalProperties | None = None,
        prefix_tree_delimiter: str | None = None,
        series_tags: Mapping[str, str] | None = None,
    ) -> Dataset:
        """Retrieve a dataset by data scope name, or create a new one if it does not exist.

        Args:
            data_scope_name: Datascope name to use when looking up or adding a dataset to an asset.
            name: Name of the dataset to create, if one is not found.
            description: Human readable description of the dataset to create, if one is not found.
            labels: Labels of the dataset to create, if one is not found.
            properties: String or numeric properties for a newly created dataset.
                Integers are stored as floats.
            prefix_tree_delimiter: The prefix tree delimiter to use with the created dataset, if one is not found.
            series_tags: Tags to filter the created dataset by in the datascope, if one is not found.

        Returns:
            The retrieved or created dataset.

        Raises:
            TypeError: If a property value is a boolean or is not a string, integer, or float.
            NominalNotFoundError: If a dataset data scope with this name exists but its dataset is not readable.
            ValueError: If a dataset data scope with this name exists with different series tags, or a data scope
                of another type has this name.
        """
        scopes = self._scopes()
        if data_scope_name in scopes.datasets:
            found_ds = resolve_datasets(self._clients, scopes.named(data_scope_name).datasets).get(data_scope_name)
            if found_ds is None:
                raise NominalDataScopeNotFoundError(
                    f"No dataset with data scope name '{data_scope_name}' found for this asset"
                )
            found_tags = scopes.datasets[data_scope_name].series_tags
            # symmetric difference to find tags found in one but not the other
            mismatching_tags = found_tags.items() ^ (series_tags or {}).items()
            if mismatching_tags:
                raise ValueError(
                    f"Cannot get_or_create_dataset '{data_scope_name}' with tags {series_tags}: "
                    f"datascope already exists with {found_tags} (difference={mismatching_tags})"
                )
            return found_ds
        if data_scope_name in scopes:
            raise ValueError(
                f"Cannot get_or_create_dataset '{data_scope_name}': a data scope of another type on asset "
                f"{self.rid} has that name"
            )

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
        """Retrieve a video by data scope name, or create a new one if it does not exist.

        Raises:
            NominalNotFoundError: If a video data scope with this name exists but its video is not readable.
            ValueError: If a data scope of another type has this name.
        """
        scopes = self._scopes()
        if data_scope_name in scopes.videos:
            video = resolve_videos(self._clients, scopes.named(data_scope_name).videos).get(data_scope_name)
            if video is None:
                raise NominalDataScopeNotFoundError(
                    f"No video with data scope name '{data_scope_name}' found for this asset"
                )
            return video
        if data_scope_name in scopes:
            raise ValueError(
                f"Cannot get_or_create_video '{data_scope_name}': a data scope of another type on asset "
                f"{self.rid} has that name"
            )

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
        properties: NominalProperties | None = None,
        labels: Sequence[str] | None = None,
    ) -> Event:
        """Create an event associated with this Asset at a given point in time.

        Args:
            name: Name of the event
            type: Verbosity level of the event.
            start: Starting timestamp of the event
            duration: Duration of the event, or 0 for an event without duration.
            description: Optionally, a human readable description of the event to create
            properties: String or numeric properties for the created event.
                Integers are stored as floats.
            labels: Sequence of labels to use on the created event.

        Returns:
            The created event that is associated with the asset.

        Raises:
            TypeError: If a property value is a boolean or is not a string, integer, or float.
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
        properties: NominalProperties | None = None,
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
            properties: String or numeric properties for the created run.
                Integers are stored as floats.
            labels: Sequence of labels to use on the created run.
            links: Link metadata to add to the created run.
            attachments: Attachments to associate with the created run.

        Returns:
            Returns the created run

        Raises:
            TypeError: If a property value is a boolean or is not a string, integer, or float.
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
            NominalNotFoundError: If no readable dataset has this data scope name.
        """
        named = self._scopes().named(data_scope_name)
        dataset = resolve_datasets(self._clients, named.datasets).get(data_scope_name)
        if dataset is None:
            raise NominalDataScopeNotFoundError(
                f"No dataset with data scope name '{data_scope_name}' found for this asset"
            )
        return dataset

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
            NominalNotFoundError: If no readable video has this data scope name.
        """
        named = self._scopes().named(data_scope_name)
        video = resolve_videos(self._clients, named.videos).get(data_scope_name)
        if video is None:
            raise NominalDataScopeNotFoundError(
                f"No video with data scope name '{data_scope_name}' found for this asset"
            )
        return video

    def list_datasets(self) -> Sequence[tuple[str, Dataset]]:
        """List the datasets associated with this asset.

        Returns:
            (data_scope_name, dataset) pairs. Data scopes whose dataset is missing or not readable are left out.
        """
        return list(resolve_datasets(self._clients, self._scopes().datasets).items())

    def list_videos(self) -> Sequence[tuple[str, Video]]:
        """List the videos associated with this asset.

        Returns:
            (data_scope_name, video) pairs. Data scopes whose video is missing or not readable are left out.
        """
        return list(resolve_videos(self._clients, self._scopes().videos).items())

    def _iter_list_attachments(self) -> Iterable[Attachment]:
        asset = self._get_latest_api()
        for a in _get_accessible_attachments(self._clients, asset.attachments):
            yield Attachment._from_proto(self._clients, a)

    def list_attachments(self) -> Sequence[Attachment]:
        """List the attachments associated with this asset.

        Attachments that the caller cannot access are left out.
        """
        return list(self._iter_list_attachments())

    def list_runs(self) -> Sequence[Run]:
        """List all runs associated with this Asset.

        Raises:
            NominalError: If a run search request fails.
        """
        return [
            Run._from_proto(self._clients, run) for run in search_runs_by_asset_paginated(self._clients.run, self.rid)
        ]

    def search_events(
        self,
        *,
        search_text: str | None = None,
        after: str | datetime.datetime | IntegralNanosecondsUTC | None = None,
        before: str | datetime.datetime | IntegralNanosecondsUTC | None = None,
        labels: Iterable[str] | None = None,
        properties: NominalProperties | None = None,
        created_by_rid: str | None = None,
        workbook_rid: str | None = None,
        data_review_rid: str | None = None,
        assignee_rid: str | None = None,
        event_type: EventType | None = None,
        priority: Priority | None = None,
        origin_types: Iterable[SearchEventOriginType] | None = None,
        archive_status: ArchiveStatusFilter = ArchiveStatusFilter.NOT_ARCHIVED,
    ) -> Sequence[Event]:
        """Search for events associated with this Asset. See nominal.core.event._search_events for details.

        Args:
            search_text: Searches for a string in the event's metadata.
            after: Filters to end times after this time, exclusive.
            before: Filters to start times before this time, exclusive.
            labels: Labels that must all be present on an event.
            properties: Property equalities that must all match. Strings and numbers match their
                respective types. Integer values are converted to floats.
            created_by_rid: Search for events created by this author.
            workbook_rid: Search for events on this workbook.
            data_review_rid: Search for events from this data review.
            assignee_rid: Search for events assigned to this user.
            event_type: Search for events of this verbosity level.
            priority: Filter events by disposition priority. None leaves priority unrestricted.
            origin_types: Search for events created by these origin types.
            archive_status: Filter by archive status. Defaults to NOT_ARCHIVED.

        Raises:
            TypeError: If a property value is a boolean or is not a string, integer, or float.
        """
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
            priority=priority,
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

        Raises:
            NominalError: If removing the attachments fails.
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

        Note:
            This does not update the instance in place; call `refresh()` to see the change reflected.

        Raises:
            NominalError: If the archive request fails.
        """
        with translate_grpc_errors():
            self._clients.assets.Archive(asset_pb2.ArchiveRequest(asset_rid=self.rid))

    def unarchive(self) -> None:
        """Unarchive this asset, allowing it to be viewed in the UI.

        Note:
            This does not update the instance in place; call `refresh()` to see the change reflected.

        Raises:
            NominalError: If the unarchive request fails.
        """
        with translate_grpc_errors():
            self._clients.assets.Unarchive(asset_pb2.UnarchiveRequest(asset_rid=self.rid))

    @classmethod
    def _from_proto(cls, clients: _Clients, asset: asset_pb2.Asset) -> Self:
        return cls(
            rid=asset.rid,
            name=asset.title,
            description=asset.description if asset.HasField("description") else None,
            properties=properties_from_proto(asset.typed_properties),
            labels=tuple(asset.labels),
            created_at=asset.created_at.ToNanoseconds(),
            is_archived=asset.is_archived,
            _clients=clients,
            created_by_rid=asset.created_by if asset.HasField("created_by") else None,
            type_rids=tuple(asset.types),
        )


def _get_assets(clients: Asset._Clients, rids: Sequence[str]) -> Mapping[str, asset_pb2.Asset]:
    """The assets with the given rids, keyed by rid. Rids that do not resolve are absent from the result.

    Raises:
        NominalError: If the retrieval request fails.
    """
    with translate_grpc_errors():
        response = clients.assets.GetAssets(asset_pb2.GetAssetsRequest(rids=list(rids)))
    return dict(response.responses)


def _get_asset(clients: Asset._Clients, rid: str) -> asset_pb2.Asset:
    """The asset with the given rid.

    Raises:
        NominalNotFoundError: If no asset has that rid.
        ValueError: If multiple assets are returned.
        NominalError: If the retrieval request fails.
    """
    assets = _get_assets(clients, [rid])
    if rid not in assets:
        raise NominalNotFoundError(f"no asset found with RID {rid!r}")
    if len(assets) > 1:
        raise ValueError(f"multiple assets found with RID {rid!r}: {assets!r}")
    return assets[rid]


def _resolve_scopes(clients: Asset._Clients, scopes: DataScopes) -> Mapping[str, ScopeType]:
    """The readable datasets and videos behind these scopes, with one request per type that has any."""
    return {
        **resolve_datasets(clients, scopes.datasets),
        **resolve_videos(clients, scopes.videos),
    }


# Moving to bottom to deal with circular dependencies
from nominal.core.run import Run, _create_run  # noqa: E402
