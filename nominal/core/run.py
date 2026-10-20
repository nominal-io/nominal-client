from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING, Iterable, Mapping, Protocol, Self, Sequence, cast

from typing_extensions import deprecated

from nominal.core._dataset_scope_ingest import _DatasetScopeIngestMixin
from nominal.core._event_types import EventType, SearchEventOriginType
from nominal.core._scope_resolution import DataScopes, resolve_datasets, resolve_videos
from nominal.core._utils.api_tools import (
    HasRid,
    Link,
    LinkDict,
    RefreshableGrpcMixin,
    create_proto_links,
    rid_from_instance_or_string,
)
from nominal.core._utils.api_types import NominalProperties
from nominal.core._utils.frontend_urls import run_url
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.core._utils.query_tools import ArchiveStatusFilter, AssetMatch
from nominal.core.attachment import Attachment, _get_accessible_attachments
from nominal.core.comment import Comment
from nominal.core.dataset import Dataset
from nominal.core.event import Event, _create_event, _search_events
from nominal.core.video import Video
from nominal.core.workbook import Workbook, _search_workbooks
from nominal.exceptions import LegacyVideoDeprecationWarning, NominalDataScopeNotFoundError
from nominal.protos.asset.v2 import asset_pb2_grpc
from nominal.protos.comments.v1 import comments_pb2, comments_pb2_grpc
from nominal.protos.run.v1 import run_service_pb2, run_service_pb2_grpc
from nominal.protos.spatial.v2 import spatial_pb2_grpc
from nominal.ts import IntegralNanosecondsDuration, IntegralNanosecondsUTC, _SecondsNanos, _to_proto_duration

if TYPE_CHECKING:
    from nominal.core.asset import Asset


@dataclass(frozen=True)
class Run(HasRid, RefreshableGrpcMixin[run_service_pb2.Run], _DatasetScopeIngestMixin):
    """A view of asset and dataset data within a time interval."""

    rid: str
    name: str
    description: str
    properties: NominalProperties
    labels: Sequence[str]
    links: Sequence[LinkDict]
    start: IntegralNanosecondsUTC
    end: IntegralNanosecondsUTC | None
    run_number: int
    assets: Sequence[str]
    created_at: IntegralNanosecondsUTC
    is_archived: bool

    _clients: _Clients = field(repr=False)
    author_rid: str | None = field(default=None, repr=False)

    class _Clients(
        Attachment._Clients,
        Dataset._Clients,
        Event._Clients,
        Video._Clients,
        Workbook._Clients,
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
        """Returns a link to the page for this Run in the Nominal app"""
        return run_url(self._clients, self.rid)

    def _get_latest_api(self) -> run_service_pb2.Run:
        return _get_run(self._clients.run, self.rid)

    def update(
        self,
        *,
        name: str | None = None,
        start: datetime | IntegralNanosecondsUTC | None = None,
        end: datetime | IntegralNanosecondsUTC | None = None,
        description: str | None = None,
        properties: NominalProperties | None = None,
        labels: Sequence[str] | None = None,
        links: Sequence[str | Link | LinkDict] | None = None,
        assets: Sequence[Asset | str] | None = None,
    ) -> Self:
        """Replace run metadata.
        Updates the current instance, and returns it.
        Only the metadata passed in will be replaced, the rest will remain untouched.

        Links can be URLs, tuples of (URL, name), or dicts of {url=URL, title=name}.

        Note:
            This replaces the metadata rather than appending it. To append to labels or properties, merge them before
            calling this method. E.g.:

            ```python
            new_labels = ["new-label-a", "new-label-b"]
            for old_label in run.labels:
                new_labels.append(old_label)
            run = run.update(labels=new_labels)
            ```

        Note:
            When `assets` is provided it fully replaces the run's asset list. To append an asset, merge with
            the existing list first:

            ```python
            run = run.update(assets=[*run.assets, new_asset])
            ```

        Raises:
            NominalError: If the update request fails.
        """
        request = run_service_pb2.UpdateRunRequest(
            rid=self.rid,
            description=description,
            labels=None if labels is None else run_service_pb2.LabelSet(labels=labels),
            properties=None if properties is None else run_service_pb2.PropertyMap(properties=properties),
            start_time=None if start is None else _SecondsNanos.from_flexible(start).to_run_proto(),
            end_time=None if end is None else _SecondsNanos.from_flexible(end).to_run_proto(),
            title=name,
            assets=[] if assets is None else [rid_from_instance_or_string(a) for a in assets],
            links=None
            if links is None
            else run_service_pb2.LinkList(links=create_proto_links(links, run_service_pb2.Link)),
        )
        with translate_grpc_errors():
            response = self._clients.run.UpdateRun(request)
        return self._refresh_from_api(response.run)

    def add_comment(self, content: str) -> Comment:
        """Post a markdown comment to this run's discussion.

        Args:
            content: Markdown content for the comment.

        Returns:
            The created `Comment`.

        Raises:
            NominalError: If the comments service request fails.
        """
        request = comments_pb2.CreateCommentRequest(
            parent=comments_pb2.CommentParent(
                resource=comments_pb2.CommentParentResource(
                    resource_type=comments_pb2.ResourceType.RUN,
                    resource_rid=self.rid,
                )
            ),
            content=content,
            attachments=[],
        )
        with translate_grpc_errors():
            response = self._clients.comments.CreateComment(request)
        return Comment._from_proto(response.comment)

    def _get_dataset_scope(self, data_scope_name: str) -> tuple[Dataset, Mapping[str, str]]:
        api_run = self._get_latest_api()
        if len(api_run.assets) > 1:
            raise RuntimeError("Can't retrieve dataset scopes on multi-asset runs")
        named = DataScopes.from_data_scopes(api_run.asset_data_scopes).named(data_scope_name)
        dataset = resolve_datasets(self._clients, named.datasets).get(data_scope_name)
        if dataset is None:
            raise NominalDataScopeNotFoundError(f"No such data scope found with data_scope_name {data_scope_name}")
        return dataset, named.datasets[data_scope_name].series_tags

    def _scopes(self) -> DataScopes:
        """This run's own data sources by ref name, read from one fetch of the run."""
        return DataScopes.from_data_sources(self._get_latest_api().data_sources)

    def remove_data_sources(
        self,
        *,
        ref_names: Sequence[str] | None = None,
        data_sources: Sequence[Dataset | Video | str] | None = None,
    ) -> None:
        """Remove data sources from this run.

        The list data_sources can contain Dataset or Video instances, or rids as string.
        A spatial can be removed by passing its rid.

        Raises:
            NominalError: If retrieving or updating the run's data sources fails.
        """
        ref_names = ref_names or []
        data_source_rids = {rid_from_instance_or_string(ds) for ds in data_sources or [] if ds}
        api_run = self._get_latest_api()
        data_sources_to_keep = {}
        for ref_name, source in api_run.data_sources.items():
            if ref_name in ref_names:
                continue
            if any(
                rid in data_source_rids
                for rid in (
                    source.data_source.dataset,
                    source.data_source.connection,
                    source.data_source.video,
                    source.data_source.spatial,
                    source.data_source.log_set,
                )
            ):
                continue
            data_sources_to_keep[ref_name] = run_service_pb2.CreateRunDataSource(
                data_source=source.data_source if source.HasField("data_source") else None,
                series_tags=source.series_tags,
                offset=source.offset if source.HasField("offset") else None,
            )
        request = run_service_pb2.UpdateRunRequest(
            rid=self.rid,
            data_sources=run_service_pb2.CreateRunDataSourceMap(data_sources=data_sources_to_keep),
        )
        with translate_grpc_errors():
            response = self._clients.run.UpdateRun(request)
        self._refresh_from_api(response.run)

    def create_event(
        self,
        name: str,
        type: EventType,
        start: datetime | IntegralNanosecondsUTC,
        duration: timedelta | IntegralNanosecondsDuration = 0,
        *,
        description: str | None = None,
        properties: NominalProperties | None = None,
        labels: Iterable[str] = (),
    ) -> Event:
        """Create an event associated with all associated assets of this run at a given point in time.

        Args:
            name: Name of the event
            type: Verbosity level of the event.
            start: Starting timestamp of the event
            duration: Duration of the event, or 0 for an event without duration.
            description: Optionally, a human readable description of the event to create
            properties: Key-value pairs to use as properties on the created event
            labels: Sequence of labels to use on the created event.

        Returns:
            The created event that is associated with all of the assets associated with this run..
        """
        return _create_event(
            self._clients,
            name=name,
            type=type,
            start=start,
            duration=duration,
            description=description,
            assets=self.assets,
            properties=properties,
            labels=labels,
        )

    def search_events(
        self,
        *,
        search_text: str | None = None,
        after: str | datetime | IntegralNanosecondsUTC | None = None,
        before: str | datetime | IntegralNanosecondsUTC | None = None,
        labels: Iterable[str] | None = None,
        properties: NominalProperties | None = None,
        created_by_rid: str | None = None,
        workbook_rid: str | None = None,
        data_review_rid: str | None = None,
        assignee_rid: str | None = None,
        event_type: EventType | None = None,
        origin_types: Iterable[SearchEventOriginType] | None = None,
        archive_status: ArchiveStatusFilter = ArchiveStatusFilter.NOT_ARCHIVED,
    ) -> Sequence[Event]:
        """Search for events associated with any of the assets of this run.

        The run's assets are matched as a union: an event is included if it is associated with at
        least one of the run's assets. The remaining filters are ANDed together with that asset
        filter and with each other.

        Args:
            search_text: Searches for a string in the event's metadata.
            after: Filters to end times after this time, exclusive.
            before: Filters to start times before this time, exclusive.
            labels: A list of labels that must ALL be present on an event to be included.
            properties: A mapping of key-value pairs that must ALL be present on an event to be included.
            created_by_rid: Rid of the author that must be present on an event to be included.
            workbook_rid: Search for events on the given workbook.
            data_review_rid: Search for events from the given data review.
            assignee_rid: Search for events with the given assignee.
            event_type: Search for events of the given type.
            origin_types: Search for events created by any of the given origin types.
            archive_status: Filter by archive status. Defaults to NOT_ARCHIVED.

        Returns:
            Events associated with any of this run's assets that match all of the provided filters.
            Returns an empty sequence if the run has no associated assets.
        """
        if not self.assets:
            return []
        return _search_events(
            self._clients,
            search_text=search_text,
            after=after,
            before=before,
            asset_rids=list(self.assets),
            asset_match=AssetMatch.ANY,
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

    def add_dataset(
        self,
        ref_name: str,
        dataset: Dataset | str,
        *,
        series_tags: Mapping[str, str] | None = None,
        offset: timedelta | IntegralNanosecondsDuration | None = None,
    ) -> None:
        """Add a dataset to this run.

        Datasets map "ref names" (their name within the run) to a Dataset (or dataset rid). The same type of datasets
        should use the same ref name across runs, since checklists and templates use ref names to reference datasets.

        Args:
            ref_name: Logical name for the data scope within the run
            dataset: Dataset to add to the run
            series_tags: Key-value tags to pre-filter the dataset with before adding to the run.
            offset: Add the dataset to the run with a pre-baked offset

        Raises:
            NominalError: If adding the data source fails.
        """
        self.add_datasets({ref_name: dataset}, series_tags=series_tags, offset=offset)

    def add_datasets(
        self,
        datasets: Mapping[str, Dataset | str],
        *,
        series_tags: Mapping[str, str] | None = None,
        offset: timedelta | IntegralNanosecondsDuration | None = None,
    ) -> None:
        """Add multiple datasets to this run.

        Datasets map "ref names" (their name within the run) to a Dataset (or dataset rid). The same type of datasets
        should use the same ref name across runs, since checklists and templates use ref names to reference datasets.

        Args:
            datasets: Mapping of logical names to datasets to add to the run
            series_tags: Key-value tags to pre-filter the datasets with before adding to the run.
            offset: Add the datasets to the run with a pre-baked offset

        Raises:
            NominalError: If adding the data sources fails.
        """
        self._add_data_sources(
            {
                ref_name: run_service_pb2.DataSource(dataset=rid_from_instance_or_string(dataset))
                for ref_name, dataset in datasets.items()
            },
            series_tags=series_tags,
            offset=offset,
        )

    @deprecated(
        "Attaching a standalone `Video` to a run is deprecated in favor of video channels on a dataset. Attach the "
        "dataset that carries the video channels with `Run.add_dataset` instead.",
        category=LegacyVideoDeprecationWarning,
    )
    def add_video(self, ref_name: str, video: Video | str) -> None:
        """Add a video to a run via video object or RID.

        Raises:
            NominalError: If adding the data source fails.
        """
        self._add_data_sources({ref_name: run_service_pb2.DataSource(video=rid_from_instance_or_string(video))})

    def _add_data_sources(
        self,
        data_sources: Mapping[str, run_service_pb2.DataSource],
        *,
        series_tags: Mapping[str, str] | None = None,
        offset: timedelta | IntegralNanosecondsDuration | None = None,
    ) -> None:
        """Add data sources to this run by ref name, all with the same series tags and offset.

        Raises:
            NominalError: If adding the data sources fails.
        """
        request = run_service_pb2.AddDataSourcesToRunRequest(
            run_rid=self.rid,
            data_sources={
                ref_name: run_service_pb2.CreateRunDataSource(
                    data_source=data_source,
                    series_tags={**series_tags} if series_tags else {},
                    offset=None if offset is None else _to_proto_duration(offset, run_service_pb2.Duration),
                )
                for ref_name, data_source in data_sources.items()
            },
        )
        with translate_grpc_errors():
            self._clients.run.AddDataSourcesToRun(request)

    def add_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        """Add attachments that have already been uploaded to this run.

        `attachments` can be `Attachment` instances, or attachment RIDs.

        Raises:
            NominalError: If adding the attachments fails.
        """
        rids = [rid_from_instance_or_string(a) for a in attachments]
        request = run_service_pb2.UpdateRunAttachmentRequest(
            rid=self.rid, attachments_to_add=rids, attachments_to_remove=[]
        )
        with translate_grpc_errors():
            self._clients.run.UpdateRunAttachment(request)

    def list_datasets(self) -> Sequence[tuple[str, Dataset]]:
        """List the datasets associated with this run.

        Returns:
            (ref_name, dataset) pairs. Refs whose dataset is missing or not readable are left out.
        """
        return list(resolve_datasets(self._clients, self._scopes().datasets).items())

    def list_videos(self) -> Sequence[tuple[str, Video]]:
        """List the videos associated with this run.

        Returns:
            (ref_name, video) pairs. Refs whose video is missing or not readable are left out.
        """
        return list(resolve_videos(self._clients, self._scopes().videos).items())

    def get_dataset(self, ref_name: str) -> Dataset:
        """Get a dataset for this run by its ref name.

        Args:
            ref_name: Name of the run datasource reference to resolve.

        Returns:
            Dataset associated with the ref name.

        Raises:
            NominalNotFoundError: If no readable dataset has this ref name.
        """
        named = self._scopes().named(ref_name)
        dataset = resolve_datasets(self._clients, named.datasets).get(ref_name)
        if dataset is None:
            raise NominalDataScopeNotFoundError(f"No dataset with ref name '{ref_name}' found for this run")
        return dataset

    @deprecated(
        "Resolving a standalone `Video` ref is deprecated in favor of video channels on a dataset. Use "
        "`Run.get_dataset` with the ref name, then `Dataset.list_video_files` to reach the video files.",
        category=LegacyVideoDeprecationWarning,
    )
    def get_video(self, ref_name: str) -> Video:
        """Get a video for this run by its ref name.

        Args:
            ref_name: Name of the run datasource reference to resolve.

        Returns:
            Video associated with the ref name.

        Raises:
            NominalNotFoundError: If no readable video has this ref name.
        """
        named = self._scopes().named(ref_name)
        video = resolve_videos(self._clients, named.videos).get(ref_name)
        if video is None:
            raise NominalDataScopeNotFoundError(f"No video with ref name '{ref_name}' found for this run")
        return video

    def _iter_list_attachments(self) -> Iterable[Attachment]:
        run = self._get_latest_api()
        for a in _get_accessible_attachments(self._clients, run.attachments):
            yield Attachment._from_proto(self._clients, a)

    def list_attachments(self) -> Sequence[Attachment]:
        """List a sequence of Attachments associated with this Run.

        Attachments that the caller cannot access are left out.
        """
        return list(self._iter_list_attachments())

    def _iter_list_assets(self) -> Iterable["Asset"]:
        from nominal.core.asset import Asset, _get_assets

        clients = cast(Asset._Clients, self._clients)
        run = self._get_latest_api()
        assets = _get_assets(clients, run.assets)
        for a in assets.values():
            yield Asset._from_proto(clients, a)

    def list_assets(self) -> Sequence["Asset"]:
        """List assets associated with this run."""
        return list(self._iter_list_assets())

    def remove_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        """Remove attachments from this run.
        Does not remove the attachments from Nominal.

        `attachments` can be `Attachment` instances, or attachment RIDs.

        Raises:
            NominalError: If removing the attachments fails.
        """
        rids = [rid_from_instance_or_string(a) for a in attachments]
        request = run_service_pb2.UpdateRunAttachmentRequest(
            rid=self.rid, attachments_to_add=[], attachments_to_remove=rids
        )
        with translate_grpc_errors():
            self._clients.run.UpdateRunAttachment(request)

    def search_workbooks(
        self,
        *,
        exact_match: str | None = None,
        search_text: str | None = None,
        labels: Sequence[str] | None = None,
        properties: NominalProperties | None = None,
        asset_rid: str | None = None,
        created_by_rid: str | None = None,
        include_drafts: bool = False,
        archive_status: ArchiveStatusFilter = ArchiveStatusFilter.NOT_ARCHIVED,
    ) -> Sequence[Workbook]:
        """Search for workbooks associated with this Run. See nominal.core.NominalClient.search_workbooks for details"""
        return _search_workbooks(
            self._clients,
            exact_match=exact_match,
            search_text=search_text,
            labels=labels,
            properties=properties,
            run_rid=self.rid,
            asset_rid=asset_rid,
            author_rid=created_by_rid,
            include_drafts=include_drafts,
            archive_status=archive_status,
        )

    def archive(self) -> None:
        """Archive this run.
        Archived runs are not deleted, but are hidden from the UI.

        Note:
            This does not update the instance in place; call `refresh()` to see the change reflected.

        Raises:
            NominalError: If the archive request fails.
        """
        with translate_grpc_errors():
            self._clients.run.ArchiveRun(run_service_pb2.ArchiveRunRequest(rid=self.rid))

    def unarchive(self) -> None:
        """Unarchive this run, allowing it to appear on the UI.

        Note:
            This does not update the instance in place; call `refresh()` to see the change reflected.

        Raises:
            NominalError: If the unarchive request fails.
        """
        with translate_grpc_errors():
            self._clients.run.UnarchiveRun(run_service_pb2.UnarchiveRunRequest(rid=self.rid))

    @classmethod
    def _from_proto(cls, clients: _Clients, run: run_service_pb2.Run) -> Self:
        return cls(
            rid=run.rid,
            name=run.title,
            description=run.description,
            properties=MappingProxyType(dict(run.properties)),
            labels=tuple(run.labels),
            links=tuple(
                (dict(url=link.url, title=link.title) if link.HasField("title") else dict(url=link.url))
                for link in run.links
            ),
            start=_SecondsNanos.from_run_proto(run.start_time).to_nanoseconds(),
            end=(_SecondsNanos.from_run_proto(run.end_time).to_nanoseconds() if run.HasField("end_time") else None),
            run_number=run.run_number,
            assets=tuple(run.assets),
            created_at=run.created_at.ToNanoseconds(),
            is_archived=run.is_archived,
            _clients=clients,
            author_rid=run.author_rid if run.HasField("author_rid") else None,
        )


def _create_run(
    clients: Run._Clients,
    *,
    name: str,
    start: datetime | IntegralNanosecondsUTC,
    end: datetime | IntegralNanosecondsUTC | None,
    description: str | None,
    properties: NominalProperties | None,
    labels: Sequence[str] | None,
    links: Sequence[str | Link | LinkDict] | None,
    attachments: Iterable[Attachment] | Iterable[str] | None,
    asset_rids: Sequence[str] | None,
) -> Run:
    """Create a run.

    Raises:
        NominalConfigError: If no default workspace can be resolved.
        NominalError: If the creation request fails.
    """
    request = run_service_pb2.CreateRunRequest(
        attachments=[rid_from_instance_or_string(a) for a in attachments or ()],
        description=description or "",
        labels=[] if labels is None else list(labels),
        links=[] if links is None else create_proto_links(links, run_service_pb2.Link),
        properties={} if properties is None else dict(properties),
        start_time=_SecondsNanos.from_flexible(start).to_run_proto(),
        title=name,
        end_time=None if end is None else _SecondsNanos.from_flexible(end).to_run_proto(),
        assets=[] if asset_rids is None else list(asset_rids),
        workspace=clients.resolve_default_workspace_rid(),
    )
    with translate_grpc_errors():
        response = clients.run.CreateRun(request)
    return Run._from_proto(clients, response.run)


def _get_run(service: run_service_pb2_grpc.RunServiceStub, rid: str) -> run_service_pb2.Run:
    """Retrieve the run with the given RID.

    Raises:
        NominalNotFoundError: If no run has that RID.
        NominalError: If the retrieval request fails.
    """
    with translate_grpc_errors():
        return service.GetRun(run_service_pb2.GetRunRequest(rid=rid)).run
