from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Iterable, Mapping, Protocol, Sequence, cast

from nominal_api import (
    scout,
    scout_run_api,
)
from typing_extensions import Self, deprecated

from nominal._utils import update_dataclass
from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils import HasRid, Link, create_links, rid_from_instance_or_string
from nominal.core.asset import Asset
from nominal.core.attachment import Attachment, _iter_get_attachments
from nominal.core.connection import Connection, _get_connections
from nominal.core.dataset import Dataset, _get_datasets
from nominal.core.log import LogSet, _get_log_set
from nominal.core.video import Video, _get_video
from nominal.ts import IntegralNanosecondsDuration, IntegralNanosecondsUTC, _SecondsNanos, _to_api_duration


@dataclass(frozen=True)
class Run(HasRid):
    rid: str
    name: str
    description: str
    properties: Mapping[str, str]
    labels: Sequence[str]
    start: IntegralNanosecondsUTC
    end: IntegralNanosecondsUTC | None
    run_number: int
    assets: Sequence[str]

    _clients: _Clients = field(repr=False)

    class _Clients(
        Asset._Clients,
        HasScoutParams,
        Protocol,
    ):
        @property
        def run(self) -> scout.RunService: ...

    @property
    def nominal_url(self) -> str:
        """Returns a link to the page for this Run in the Nominal app"""
        return f"{self._clients.app_base_url}/runs/{self.run_number}"

    def _list_datasource_rids(
        self, datasource_type: str | None = None, property_name: str | None = None
    ) -> Mapping[str, str]:
        enriched_run = self._clients.run.get_run(self._clients.auth_header, self.rid)
        datasource_rids_by_ref_name = {}
        for ref_name, source in enriched_run.data_sources.items():
            if datasource_type is not None and source.data_source.type != datasource_type:
                continue

            rid = cast(
                str, getattr(source.data_source, source.data_source.type if property_name is None else property_name)
            )
            datasource_rids_by_ref_name[ref_name] = rid

        return datasource_rids_by_ref_name

    @deprecated(
        "LogSets are deprecated and will be removed in a future version. "
        "Add logs to an existing dataset with dataset.write_logs instead."
    )
    def add_log_set(self, ref_name: str, log_set: LogSet | str) -> None:
        """Add a log set to this run.

        Log sets map "ref names" (their name within the run) to a Log set (or log set rid).
        """
        self.add_log_sets({ref_name: log_set})

    @deprecated(
        "LogSets are deprecated and will be removed in a future version. "
        "Add logs to an existing dataset with dataset.write_logs instead."
    )
    def add_log_sets(self, log_sets: Mapping[str, LogSet | str]) -> None:
        """Add multiple log sets to this run.

        Log sets map "ref names" (their name within the run) to a Log set (or log set rid).
        """
        data_sources = {
            ref_name: scout_run_api.CreateRunDataSource(
                data_source=scout_run_api.DataSource(log_set=rid_from_instance_or_string(log_set)),
                series_tags={},
                offset=None,
            )
            for ref_name, log_set in log_sets.items()
        }
        self._clients.run.add_data_sources_to_run(self._clients.auth_header, data_sources, self.rid)

    def _iter_list_log_sets(self) -> Iterable[tuple[str, LogSet]]:
        log_set_rids_by_ref_name = self._list_datasource_rids("logSet", property_name="log_set")
        log_sets_by_rids = {
            rid: LogSet._from_conjure(
                self._clients,
                _get_log_set(self._clients, rid),
            )
            for rid in log_set_rids_by_ref_name.values()
        }
        for ref_name, rid in log_set_rids_by_ref_name.items():
            log_set = log_sets_by_rids[rid]
            yield (ref_name, log_set)

    @deprecated(
        "LogSets are deprecated and will be removed in a future version. "
        "Logs should be stored as a log channel in a Nominal datasource instead."
    )
    def list_log_sets(self) -> Sequence[tuple[str, LogSet]]:
        """List the log_sets associated with this run.
        Returns (ref_name, logset) pairs for each logset.
        """
        return list(self._iter_list_log_sets())

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
        data_sources = {
            ref_name: scout_run_api.CreateRunDataSource(
                data_source=scout_run_api.DataSource(dataset=rid_from_instance_or_string(dataset)),
                series_tags={**series_tags} if series_tags else {},
                offset=None if offset is None else _to_api_duration(offset),
            )
            for ref_name, dataset in datasets.items()
        }
        self._clients.run.add_data_sources_to_run(self._clients.auth_header, data_sources, self.rid)

    def _iter_list_datasets(self) -> Iterable[tuple[str, Dataset]]:
        dataset_rids_by_ref_name = self._list_datasource_rids("dataset")
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

    def add_video(self, ref_name: str, video: Video | str) -> None:
        """Add a video to a run via video object or RID."""
        request = scout_run_api.CreateRunDataSource(
            data_source=scout_run_api.DataSource(video=rid_from_instance_or_string(video)),
            series_tags={},
            offset=None,
        )
        self._clients.run.add_data_sources_to_run(self._clients.auth_header, {ref_name: request}, self.rid)

    def _iter_list_videos(self) -> Iterable[tuple[str, Video]]:
        video_rids_by_ref_name = self._list_datasource_rids("video")
        videos_by_rids = {
            rid: Video._from_conjure(
                self._clients,
                _get_video(self._clients, rid),
            )
            for rid in video_rids_by_ref_name.values()
        }
        for ref_name, rid in video_rids_by_ref_name.items():
            video = videos_by_rids[rid]
            yield (ref_name, video)

    def list_videos(self) -> Sequence[tuple[str, Video]]:
        """List a sequence of refname, Video tuples associated with this Run."""
        return list(self._iter_list_videos())

    def add_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        """Add attachments that have already been uploaded to this run.

        `attachments` can be `Attachment` instances, or attachment RIDs.
        """
        rids = [rid_from_instance_or_string(a) for a in attachments]
        request = scout_run_api.UpdateAttachmentsRequest(attachments_to_add=rids, attachments_to_remove=[])
        self._clients.run.update_run_attachment(self._clients.auth_header, request, self.rid)

    def remove_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        """Remove attachments from this run.
        Does not remove the attachments from Nominal.

        `attachments` can be `Attachment` instances, or attachment RIDs.
        """
        rids = [rid_from_instance_or_string(a) for a in attachments]
        request = scout_run_api.UpdateAttachmentsRequest(attachments_to_add=[], attachments_to_remove=rids)
        self._clients.run.update_run_attachment(self._clients.auth_header, request, self.rid)

    def _iter_list_attachments(self) -> Iterable[Attachment]:
        run = self._clients.run.get_run(self._clients.auth_header, self.rid)
        for a in _iter_get_attachments(self._clients.auth_header, self._clients.attachment, run.attachments):
            yield Attachment._from_conjure(self._clients, a)

    def list_attachments(self) -> Sequence[Attachment]:
        """List a sequence of Attachments associated with this Run."""
        return list(self._iter_list_attachments())

    def _iter_list_assets(self) -> Iterable[Asset]:
        run = self._clients.run.get_run(self._clients.auth_header, self.rid)
        assets = self._clients.assets.get_assets(self._clients.auth_header, run.assets)
        for a in assets.values():
            yield Asset._from_conjure(self._clients, a)

    def list_assets(self) -> Sequence[Asset]:
        """List assets associated with this run."""
        return list(self._iter_list_assets())

    def archive(self) -> None:
        """Archive this run.
        Archived runs are not deleted, but are hidden from the UI.

        NOTE: currently, it is not possible (yet) to unarchive a run once archived.
        """
        self._clients.run.archive_run(self._clients.auth_header, self.rid)

    def update(
        self,
        *,
        name: str | None = None,
        start: datetime | IntegralNanosecondsUTC | None = None,
        end: datetime | IntegralNanosecondsUTC | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
        links: Sequence[str] | Sequence[Link] | None = None,
    ) -> Self:
        """Replace run metadata.
        Updates the current instance, and returns it.
        Only the metadata passed in will be replaced, the rest will remain untouched.

        Links can be URLs or tuples of (URL, name).

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
        response = self._clients.run.update_run(self._clients.auth_header, request, self.rid)
        run = self.__class__._from_conjure(self._clients, response)
        update_dataclass(self, run, fields=self.__dataclass_fields__)
        return self

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

        conjure_run = self._clients.run.get_run(self._clients.auth_header, self.rid)

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

        response = self._clients.run.update_run(
            self._clients.auth_header,
            scout_run_api.UpdateRunRequest(
                assets=[],
                data_sources=data_sources_to_keep,
            ),
            self.rid,
        )
        run = self.__class__._from_conjure(self._clients, response)
        update_dataclass(self, run, fields=self.__dataclass_fields__)

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
        data_sources = {
            ref_name: scout_run_api.CreateRunDataSource(
                data_source=scout_run_api.DataSource(connection=rid_from_instance_or_string(connection)),
                series_tags={**series_tags} if series_tags else {},
                offset=None if offset is None else _to_api_duration(offset),
            )
        }
        self._clients.run.add_data_sources_to_run(self._clients.auth_header, data_sources, self.rid)

    def _iter_list_connections(self) -> Iterable[tuple[str, Connection]]:
        conn_rids_by_ref_name = self._list_datasource_rids("connection")
        connections_by_rids = {
            conn.rid: Connection._from_conjure(self._clients, conn)
            for conn in _get_connections(self._clients, list(conn_rids_by_ref_name.values()))
        }

        for ref_name, rid in conn_rids_by_ref_name.items():
            connection = connections_by_rids[rid]
            yield (ref_name, connection)

    def list_connections(self) -> Sequence[tuple[str, Connection]]:
        """List the connections associated with this run.
        Returns (ref_name, connection) pairs for each connection
        """
        return list(self._iter_list_connections())

    @classmethod
    def _from_conjure(cls, clients: _Clients, run: scout_run_api.Run) -> Self:
        return cls(
            rid=run.rid,
            name=run.title,
            description=run.description,
            properties=MappingProxyType(run.properties),
            labels=tuple(run.labels),
            start=_SecondsNanos.from_scout_run_api(run.start_time).to_nanoseconds(),
            end=(_SecondsNanos.from_scout_run_api(run.end_time).to_nanoseconds() if run.end_time else None),
            run_number=run.run_number,
            assets=tuple(run.assets),
            _clients=clients,
        )
