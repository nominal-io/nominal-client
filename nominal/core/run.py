from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Iterable, Mapping, Protocol, Sequence

from nominal_api import (
    scout_run_api,
)
from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import (
    HasRid,
    Link,
    LinkDict,
    RefreshableMixin,
    create_links,
    rid_from_instance_or_string,
)
from nominal.core.asset import Asset, ScopeTypeSpecifier, _filter_scope_rids, _filter_scopes
from nominal.core.attachment import Attachment, _iter_get_attachments
from nominal.core.connection import Connection, _get_connections
from nominal.core.dataset import Dataset, _create_dataset, _DatasetWrapper, _get_dataset, _get_datasets
from nominal.core.event import Event, EventType, _create_event
from nominal.core.video import Video, _create_video, _get_video
from nominal.core.workbook import Workbook
from nominal.core.workbook_template import WorkbookTemplate
from nominal.ts import IntegralNanosecondsDuration, IntegralNanosecondsUTC, _SecondsNanos, _to_api_duration


@dataclass(frozen=True)
class Run(HasRid, RefreshableMixin[scout_run_api.Run], _DatasetWrapper):
    rid: str
    name: str
    description: str
    properties: Mapping[str, str]
    labels: Sequence[str]
    links: Sequence[LinkDict]
    start: IntegralNanosecondsUTC
    end: IntegralNanosecondsUTC | None
    run_number: int
    assets: Sequence[str]
    created_at: IntegralNanosecondsUTC

    _clients: _Clients = field(repr=False)

    class _Clients(
        Asset._Clients,
        Event._Clients,
        HasScoutParams,
        Protocol,
    ):
        pass

    @property
    def nominal_url(self) -> str:
        """Returns a link to the page for this Run in the Nominal app"""
        return f"{self._clients.app_base_url}/runs/{self.run_number}"

    def _get_latest_api(self) -> scout_run_api.Run:
        return self._clients.run.get_run(self._clients.auth_header, self.rid)

    def update(
        self,
        *,
        name: str | None = None,
        start: datetime | IntegralNanosecondsUTC | None = None,
        end: datetime | IntegralNanosecondsUTC | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
        links: Sequence[str | Link | LinkDict] | None = None,
    ) -> Self:
        """Replace run metadata.
        Updates the current instance, and returns it.
        Only the metadata passed in will be replaced, the rest will remain untouched.

        Links can be URLs, tuples of (URL, name), or dicts of {url=URL, title=name}.

        Note: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
        calling this method. E.g.:

            new_labels = ["new-label-a", "new-label-b"]
            for old_label in run.labels:
                new_labels.append(old_label)
            run = run.update(labels=new_labels)
        """
        request = scout_run_api.UpdateRunRequest(
            description=description,
            labels=None if labels is None else list(labels),
            properties=None if properties is None else dict(properties),
            start_time=None if start is None else _SecondsNanos.from_flexible(start).to_scout_run_api(),
            end_time=None if end is None else _SecondsNanos.from_flexible(end).to_scout_run_api(),
            title=name,
            assets=[],
            links=None if links is None else create_links(links),
        )
        updated_run = self._clients.run.update_run(self._clients.auth_header, request, self.rid)
        return self._refresh_from_api(updated_run)

    def _scope_rids(self, scope_type: ScopeTypeSpecifier) -> Mapping[str, str]:
        if len(self.assets) > 1:
            raise RuntimeError("Can't retrieve scope rids on multi-asset runs")

        run = self._get_latest_api()
        return _filter_scope_rids(run.asset_data_scopes, scope_type)

    def _get_dataset_scope(self, data_scope_name: str) -> tuple[Dataset, Mapping[str, str]]:
        if len(self.assets) > 1:
            raise RuntimeError("Can't retrieve dataset scopes on multi-asset runs")

        run = self._get_latest_api()
        ds_scopes = {scope.data_scope_name: scope for scope in _filter_scopes(run.asset_data_scopes, "dataset")}

        data_scope = ds_scopes.get(data_scope_name)
        if data_scope is None:
            raise ValueError(f"No such data scope found on asset {self.rid} with data_scope_name {data_scope_name}")
        elif data_scope.data_source.dataset is None:
            raise ValueError(f"Datascope {data_scope_name} on asset {self.rid} is not a dataset!")

        dataset = Dataset._from_conjure(
            self._clients,
            _get_dataset(self._clients.auth_header, self._clients.catalog, data_scope.data_source.dataset),
        )
        return dataset, data_scope.series_tags

    def remove_data_sources(
        self,
        *,
        ref_names: Sequence[str] | None = None,
        data_sources: Sequence[Connection | Dataset | Video | str] | None = None,
    ) -> None:
        """Remove data sources from this run.

        The list data_sources can contain Connection, Dataset, Video instances, or rids as string.
        """
        ref_names = ref_names or []
        data_source_rids = {rid_from_instance_or_string(ds) for ds in data_sources or []}

        conjure_run = self._get_latest_api()

        data_sources_to_keep = {
            ref_name: scout_run_api.CreateRunDataSource(
                data_source=rds.data_source,
                series_tags=rds.series_tags,
                offset=rds.offset,
            )
            for ref_name, rds in conjure_run.data_sources.items()
            if ref_name not in ref_names
            and (rds.data_source.dataset or rds.data_source.connection or rds.data_source.video) not in data_source_rids
        }

        updated_run = self._clients.run.update_run(
            self._clients.auth_header,
            scout_run_api.UpdateRunRequest(
                assets=[],
                data_sources=data_sources_to_keep,
            ),
            self.rid,
        )
        self._refresh_from_api(updated_run)

    def get_or_create_dataset(
        self,
        data_scope_name: str,
        *,
        name: str | None = None,
        description: str | None = None,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
    ) -> Dataset:
        try:
            return self.get_dataset(data_scope_name)
        except ValueError:
            enriched_dataset = _create_dataset(
                self._clients.auth_header,
                self._clients.catalog,
                name=name or data_scope_name,
                description=description,
                properties=properties,
                labels=labels,
                workspace_rid=self._clients.workspace_rid,
            )
            dataset = Dataset._from_conjure(self._clients, enriched_dataset)
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
        try:
            return self.get_video(data_scope_name)
        except ValueError:
            raw_video = _create_video(
                self._clients.auth_header,
                self._clients.video,
                name or data_scope_name,
                description=description,
                properties=properties,
                labels=labels,
                workspace_rid=self._clients.workspace_rid,
            )
            video = Video._from_conjure(self._clients, raw_video)
            self.add_video(data_scope_name, video)
            return video

    def create_event(
        self,
        name: str,
        type: EventType,
        start: datetime | IntegralNanosecondsUTC,
        duration: timedelta | IntegralNanosecondsDuration = 0,
        *,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Iterable[str] = (),
    ) -> Event:
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

    def create_workbook(
        self,
        template: WorkbookTemplate,
        *,
        title: str | None = None,
        description: str | None = None,
    ) -> Workbook:
        return template.create_workbook(title=title, description=description, run=self)

    def get_dataset(self, data_scope_name: str) -> Dataset:
        """Retrieve a dataset by data scope name, or raise ValueError if one is not found."""
        if len(self.assets) > 1:
            raise RuntimeError(
                f"Cannot retrieve dataset with refname {data_scope_name}-- multiple assets associated! "
                "Retrieve the dataset from the desired asset directly."
            )

        maybe_rid = self._scope_rids("dataset").get(data_scope_name)
        if maybe_rid is None:
            raise ValueError(f"Data scope {data_scope_name} on run {self.rid} does not exist!")
        else:
            return Dataset._from_conjure(
                self._clients, _get_dataset(self._clients.auth_header, self._clients.catalog, maybe_rid)
            )

    def get_video(self, data_scope_name: str) -> Video:
        """Retrieve a video by data scope name, or raise ValueError if one is not found."""
        if len(self.assets) > 1:
            raise RuntimeError(
                f"Cannot retrieve video with refname {data_scope_name}-- multiple assets associated! "
                "Retrieve the video from the desired asset directly."
            )

        maybe_rid = self._scope_rids("video").get(data_scope_name)
        if maybe_rid is None:
            raise ValueError(f"Data scope {data_scope_name} on run {self.rid} does not exist!")
        else:
            return Video._from_conjure(self._clients, _get_video(self._clients, maybe_rid))

    def get_connection(self, data_scope_name: str) -> Connection:
        """Retrieve a connection by data scope name, or raise ValueError if one is not found."""
        if len(self.assets) > 1:
            raise RuntimeError(
                f"Cannot retrieve connection with refname {data_scope_name}-- multiple assets associated! "
                "retrieve the connection from the desired directly."
            )

        maybe_rid = self._scope_rids("connection").get(data_scope_name)
        if maybe_rid is None:
            raise ValueError(f"Data scope {data_scope_name} on run {self.rid} does not exist!")
        else:
            found_connections = _get_connections(self._clients, [maybe_rid])
            if len(found_connections) != 1:
                raise ValueError(f"Expected exactly one connection, received: {len(found_connections)}")
            return Connection._from_conjure(self._clients, found_connections[0])

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
        """
        if len(self.assets) > 1:
            raise RuntimeError(
                f"Cannot add datasets {datasets.keys()} to run {self.rid}-- multiple assets associated! "
                "Add the datasets to the desired asset directly."
            )

        data_sources = {
            ref_name: scout_run_api.CreateRunDataSource(
                data_source=scout_run_api.DataSource(dataset=rid_from_instance_or_string(dataset)),
                series_tags={**series_tags} if series_tags else {},
                offset=None if offset is None else _to_api_duration(offset),
            )
            for ref_name, dataset in datasets.items()
        }
        self._clients.run.add_data_sources_to_run(self._clients.auth_header, data_sources, self.rid)

    def add_connection(
        self,
        ref_name: str,
        connection: Connection | str,
        *,
        series_tags: Mapping[str, str] | None = None,
        offset: timedelta | IntegralNanosecondsDuration | None = None,
    ) -> None:
        """Add a connection to this run.

        Ref_name maps "ref name" (the name within the run) to a Connection (or connection rid). The same type of
        connection should use the same ref name across runs, since checklists and templates use ref names to reference
        connections.

        Args:
            ref_name: Logical name for the connection to add to the run
            connection: Connection to add to the run
            series_tags: Key-value tags to pre-filter the connection with before adding to the run.
            offset: Add the connection to the run with a pre-baked offset
        """
        if len(self.assets) > 1:
            raise RuntimeError(
                f"Cannot add connection {ref_name} to run {self.rid}-- multiple assets associated! "
                "Add the connection to the desired asset directly."
            )
        data_sources = {
            ref_name: scout_run_api.CreateRunDataSource(
                data_source=scout_run_api.DataSource(connection=rid_from_instance_or_string(connection)),
                series_tags={**series_tags} if series_tags else {},
                offset=None if offset is None else _to_api_duration(offset),
            )
        }
        self._clients.run.add_data_sources_to_run(self._clients.auth_header, data_sources, self.rid)

    def add_video(self, ref_name: str, video: Video | str) -> None:
        """Add a video to a run via video object or RID."""
        if len(self.assets) > 1:
            raise RuntimeError(
                f"Cannot add video {ref_name} to run {self.rid}-- multiple assets associated! "
                "Add the video to the desired asset directly."
            )

        request = scout_run_api.CreateRunDataSource(
            data_source=scout_run_api.DataSource(video=rid_from_instance_or_string(video)),
            series_tags={},
            offset=None,
        )
        self._clients.run.add_data_sources_to_run(self._clients.auth_header, {ref_name: request}, self.rid)

    def add_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        """Add attachments that have already been uploaded to this run.

        `attachments` can be `Attachment` instances, or attachment RIDs.
        """
        rids = [rid_from_instance_or_string(a) for a in attachments]
        request = scout_run_api.UpdateAttachmentsRequest(attachments_to_add=rids, attachments_to_remove=[])
        self._clients.run.update_run_attachment(self._clients.auth_header, request, self.rid)

    def _iter_list_datasets(self) -> Iterable[tuple[str, Dataset]]:
        dataset_rids_by_ref_name = self._scope_rids("dataset")
        datasets_by_rids = {
            ds.rid: Dataset._from_conjure(self._clients, ds)
            for ds in _get_datasets(self._clients.auth_header, self._clients.catalog, dataset_rids_by_ref_name.values())
        }
        for ref_name, rid in dataset_rids_by_ref_name.items():
            dataset = datasets_by_rids[rid]
            yield (ref_name, dataset)

    def list_datasets(self) -> Sequence[tuple[str, Dataset]]:
        """List the datasets associated with this run.
        Returns (ref_name, dataset) pairs for each dataset.
        """
        return list(self._iter_list_datasets())

    def _iter_list_videos(self) -> Iterable[tuple[str, Video]]:
        video_rids_by_ref_name = self._scope_rids("video")
        for ref_name, rid in video_rids_by_ref_name.items():
            yield ref_name, Video._from_conjure(self._clients, _get_video(self._clients, rid))

    def list_videos(self) -> Sequence[tuple[str, Video]]:
        """List a sequence of refname, Video tuples associated with this Run."""
        return list(self._iter_list_videos())

    def _iter_list_connections(self) -> Iterable[tuple[str, Connection]]:
        conn_rids_by_ref_name = self._scope_rids("connection")
        for ref_name, rid in conn_rids_by_ref_name.items():
            found_connections = _get_connections(self._clients, [rid])
            if len(found_connections) != 1:
                raise RuntimeError(
                    f"Expected to find exactly one connection with rid {rid}, found {len(found_connections)}"
                )

            yield ref_name, Connection._from_conjure(self._clients, found_connections[0])

    def list_connections(self) -> Sequence[tuple[str, Connection]]:
        """List the connections associated with this run.
        Returns (ref_name, connection) pairs for each connection
        """
        return list(self._iter_list_connections())

    def _iter_list_attachments(self) -> Iterable[Attachment]:
        run = self._get_latest_api()
        for a in _iter_get_attachments(self._clients.auth_header, self._clients.attachment, run.attachments):
            yield Attachment._from_conjure(self._clients, a)

    def list_attachments(self) -> Sequence[Attachment]:
        """List a sequence of Attachments associated with this Run."""
        return list(self._iter_list_attachments())

    def remove_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        """Remove attachments from this run.
        Does not remove the attachments from Nominal.

        `attachments` can be `Attachment` instances, or attachment RIDs.
        """
        rids = [rid_from_instance_or_string(a) for a in attachments]
        request = scout_run_api.UpdateAttachmentsRequest(attachments_to_add=[], attachments_to_remove=rids)
        self._clients.run.update_run_attachment(self._clients.auth_header, request, self.rid)

    def add_asset(self, asset: Asset | str) -> None:
        asset_rids = set(self.refresh().assets)
        asset_rids.add(rid_from_instance_or_string(asset))

        request = scout_run_api.UpdateRunRequest(assets=list(asset_rids))
        updated_run = self._clients.run.update_run(self._clients.auth_header, request, self.rid)
        self._refresh_from_api(updated_run)

    def _iter_list_assets(self) -> Iterable[Asset]:
        run = self._get_latest_api()
        assets = self._clients.assets.get_assets(self._clients.auth_header, run.assets)
        for a in assets.values():
            yield Asset._from_conjure(self._clients, a)

    def list_assets(self) -> Sequence[Asset]:
        """List assets associated with this run."""
        return list(self._iter_list_assets())

    def archive(self) -> None:
        """Archive this run.
        Archived runs are not deleted, but are hidden from the UI.
        """
        self._clients.run.archive_run(self._clients.auth_header, self.rid)

    def unarchive(self) -> None:
        """Unarchive this run, allowing it to appear on the UI."""
        self._clients.run.unarchive_run(self._clients.auth_header, self.rid)

    @classmethod
    def _from_conjure(cls, clients: _Clients, run: scout_run_api.Run) -> Self:
        return cls(
            rid=run.rid,
            name=run.title,
            description=run.description,
            properties=MappingProxyType(run.properties),
            labels=tuple(run.labels),
            links=tuple(
                (dict(url=link.url, title=link.title) if link.title is not None else dict(url=link.url))
                for link in run.links
            ),
            start=_SecondsNanos.from_scout_run_api(run.start_time).to_nanoseconds(),
            end=(_SecondsNanos.from_scout_run_api(run.end_time).to_nanoseconds() if run.end_time else None),
            run_number=run.run_number,
            assets=[rid for rid in run.asset_data_scopes_map],
            created_at=_SecondsNanos.from_flexible(run.created_at).to_nanoseconds(),
            _clients=clients,
        )


def _create_run(
    clients: Run._Clients,
    *,
    name: str,
    start: datetime | IntegralNanosecondsUTC,
    end: datetime | IntegralNanosecondsUTC | None,
    description: str | None,
    properties: Mapping[str, str] | None,
    labels: Sequence[str],
    links: Sequence[str | Link | LinkDict],
    attachments: Iterable[Attachment] | Iterable[str],
    asset_rids: Sequence[str],
) -> Run:
    """Create a run."""
    request = scout_run_api.CreateRunRequest(
        attachments=[rid_from_instance_or_string(a) for a in attachments],
        data_sources={},
        description=description or "",
        labels=list(labels),
        links=create_links(links),
        properties={} if properties is None else dict(properties),
        start_time=_SecondsNanos.from_flexible(start).to_scout_run_api(),
        title=name,
        end_time=None if end is None else _SecondsNanos.from_flexible(end).to_scout_run_api(),
        assets=list(asset_rids),
        workspace=clients.workspace_rid,
    )
    response = clients.run.create_run(clients.auth_header, request)
    return Run._from_conjure(clients, response)
