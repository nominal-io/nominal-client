from __future__ import annotations

import abc
import datetime
import logging
from typing import Iterable, Literal, Mapping, Protocol, Sequence, TypeAlias

from typing_extensions import deprecated

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils import HasRid, rid_from_instance_or_string
from nominal.core.attachment import Attachment
from nominal.core.connection import Connection, _iter_connections
from nominal.core.dataset import Dataset, _get_datasets
from nominal.core.datasource import DataSource
from nominal.core.log import LogSet, _get_log_set
from nominal.core.video import Video, _get_video

ScopeType: TypeAlias = "Connection | Dataset | LogSet | Video"
ScopeTypeSpecifier: TypeAlias = Literal["dataset", "video", "connection", "logset"]

logger = logging.getLogger(__name__)


class _DatasourceContainerClients(
    Attachment._Clients,
    DataSource._Clients,
    LogSet._Clients,
    Video._Clients,
    HasScoutParams,
    Protocol,
):
    pass


class DatasourceContainer(abc.ABC):
    def __init__(self, clients: _DatasourceContainerClients):
        """Initialize datasource container

        Args:
            clients: Clientsbunch to use for requests
        """
        self.__clients = clients

    @abc.abstractmethod
    def _add_data_scope(
        self,
        scope_name: str,
        scope: HasRid | str,
        scope_type: ScopeTypeSpecifier,
        *,
        series_tags: Mapping[str, str] | None = None,
        offset: datetime.timedelta | None = None,
    ) -> None:
        """Add data source to the underlying container

        Args:
            scope_name: Datascope name to add the source with
            scope: Instance or rid of the datasource to add to the underlying container
            scope_type: Type of datasource being added
            series_tags: Mapping of tag key-value pairs to filter data with before adding to the container
            offset: Time offset to add the data source with, relative to the container start time.
        """

    def _add_data_scopes(
        self,
        scopes: Mapping[str, HasRid | str],
        scope_type: ScopeTypeSpecifier,
        *,
        series_tags: Mapping[str, str] | None = None,
        offset: datetime.timedelta | None = None,
    ) -> None:
        for scope_name, scope in scopes.items():
            self._add_data_scope(scope_name, scope, scope_type, series_tags=series_tags, offset=offset)

    def data_scopes(self) -> Iterable[tuple[str, ScopeType]]:
        yield from self.datasets()
        yield from self.connections()
        yield from self.videos()
        yield from self.logsets()

    def list_data_scopes(self) -> Sequence[tuple[str, ScopeType]]:
        return list(self.data_scopes())

    def get_data_scope(self, data_scope_name: str) -> ScopeType:
        for name, scope in self.data_scopes():
            if name == data_scope_name:
                return scope

        raise ValueError(f"No such data scope found with name {data_scope_name}")

    @abc.abstractmethod
    def remove_data_scopes(
        self,
        *,
        names: Sequence[str] | None = None,
        scopes: Sequence[ScopeType | str] | None = None,
    ) -> None:
        """Remove data scopes from this container.

        Args:
            names: data scope names to remove
            scopes: rids or scope objects to remove
        """

    @deprecated("Use `remove_data_scopes` instead")
    def remove_data_sources(
        self,
        *,
        ref_names: Sequence[str] | None = None,
        data_sources: Sequence[Connection | Dataset | Video | str] | None = None,
    ) -> None:
        """Remove data sources from this run.

        The list data_sources can contain Connection, Dataset, Video instances, or rids as string.
        """
        self.remove_data_scopes(names=ref_names, scopes=data_sources)

    @abc.abstractmethod
    def _rids_by_scope_name(self, stype: ScopeTypeSpecifier) -> Mapping[str, str]:
        """Return a mapping of data scope name => data scope rid"""

    def add_dataset(
        self,
        data_scope_name: str,
        dataset: Dataset | str,
        *,
        series_tags: Mapping[str, str] | None = None,
        offset: datetime.timedelta | None = None,
    ) -> None:
        """Add dataset to datasource container by the given datascope name"""
        self._add_data_scope(data_scope_name, dataset, "dataset", series_tags=series_tags, offset=offset)

    def add_datasets(
        self,
        datasets: Mapping[str, Dataset | str],
        *,
        series_tags: Mapping[str, str] | None = None,
        offset: datetime.timedelta | None = None,
    ) -> None:
        """Add datasets to datasource container by the given datascope names"""
        self._add_data_scopes(datasets, "dataset", series_tags=series_tags, offset=offset)

    def datasets(self) -> Iterable[tuple[str, Dataset]]:
        """Iterate over datasource (name, Dataset) pairs"""
        rids_by_name = self._rids_by_scope_name(stype="dataset")
        if not rids_by_name:
            return

        scope_rid_names = {rid: name for name, rid in rids_by_name.items()}
        for dataset in _get_datasets(self.__clients.auth_header, self.__clients.catalog, scope_rid_names):
            scope_name = rids_by_name.get(dataset.rid)
            if scope_name is None:
                continue

            yield scope_name, Dataset._from_conjure(self.__clients, dataset)

    def list_datasets(self) -> Sequence[tuple[str, Dataset]]:
        """List datasource (name, Dataset) pairs"""
        return list(self.datasets())

    def get_dataset(self, data_scope_name: str) -> Dataset:
        scope = self.get_data_scope(data_scope_name)
        if isinstance(scope, Dataset):
            return scope
        else:
            raise ValueError(f"Data scope {data_scope_name} is not a dataset: ({type(scope)})")

    def add_connection(
        self,
        data_scope_name: str,
        connection: Connection | str,
        *,
        series_tags: Mapping[str, str] | None = None,
        offset: datetime.timedelta | None = None,
    ) -> None:
        """Add connection to datasource container by the given datascope name"""
        self._add_data_scope(data_scope_name, connection, "connection", series_tags=series_tags, offset=offset)

    def add_connections(
        self,
        connections: Mapping[str, Connection | str],
        *,
        series_tags: Mapping[str, str] | None = None,
        offset: datetime.timedelta | None = None,
    ) -> None:
        """Add connections to datasource container by the given datascope names"""
        self._add_data_scopes(connections, "connection", series_tags=series_tags, offset=offset)

    def connections(self) -> Iterable[tuple[str, Connection]]:
        """Iterate over datasource (name, Connection) pairs"""
        rids_by_name = self._rids_by_scope_name(stype="connection")
        if not rids_by_name:
            return

        scope_rid_names = {rid: name for name, rid in rids_by_name.items()}
        for dataset in _iter_connections(self.__clients, scope_rid_names):
            scope_name = rids_by_name.get(dataset.rid)
            if scope_name is None:
                continue

            yield scope_name, Connection._from_conjure(self.__clients, dataset)

    def list_connections(self) -> Sequence[tuple[str, Connection]]:
        """List datasource (name, Connection) pairs"""
        return list(self.connections())

    def get_connection(self, data_scope_name: str) -> Connection:
        scope = self.get_data_scope(data_scope_name)
        if isinstance(scope, Connection):
            return scope
        else:
            raise ValueError(f"Data scope {data_scope_name} is not a connection: ({type(scope)})")

    def add_video(
        self,
        data_scope_name: str,
        video: Video | str,
    ) -> None:
        """Add video to datasource container by the given datascope name."""
        # TODO(drake): support tags, offset
        self._add_data_scope(data_scope_name, video, "video")

    def add_videos(
        self,
        videos: Mapping[str, Video | str],
    ) -> None:
        """Add videos to datasource container by the given datascope names"""
        self._add_data_scopes(videos, "video")

    def videos(self) -> Iterable[tuple[str, Video]]:
        """Iterate over datasource (name, Video) pairs"""
        rids_by_name = self._rids_by_scope_name(stype="video")
        for scope_name, rid in rids_by_name.items():
            raw_video = _get_video(self.__clients, rid)
            yield scope_name, Video._from_conjure(self.__clients, raw_video)

    def list_videos(self) -> Sequence[tuple[str, Video]]:
        """List datasource (name, Video) pairs"""
        return list(self.videos())

    def get_video(self, data_scope_name: str) -> Video:
        scope = self.get_data_scope(data_scope_name)
        if isinstance(scope, Video):
            return scope
        else:
            raise ValueError(f"Data scope {data_scope_name} is not a video: ({type(scope)})")

    @deprecated(
        "LogSets are deprecated and will be removed in a future version. "
        "Logs should be stored as a log channel in a Nominal datasource instead."
    )
    def add_log_set(
        self,
        data_scope_name: str,
        log_set: LogSet | str,
    ) -> None:
        """Add logset to datasource container by the given datascope name"""
        # TODO(drake): support tags, offset
        self._add_data_scope(data_scope_name, log_set, "logset")

    @deprecated(
        "LogSets are deprecated and will be removed in a future version. "
        "Logs should be stored as a log channel in a Nominal datasource instead."
    )
    def add_log_sets(
        self,
        log_sets: Mapping[str, LogSet | str],
    ) -> None:
        """Add log_sets to datasource container by the given datascope names"""
        self._add_data_scopes(log_sets, "logset")

    @deprecated(
        "LogSets are deprecated and will be removed in a future version. "
        "Logs should be stored as a log channel in a Nominal datasource instead."
    )
    def logsets(self) -> Iterable[tuple[str, LogSet]]:
        """Iterate over datasource (name, LogSet) pairs"""
        rids_by_name = self._rids_by_scope_name(stype="logset")
        for scope_name, rid in rids_by_name.items():
            raw_log_set = _get_log_set(self.__clients, rid)
            yield scope_name, LogSet._from_conjure(self.__clients, raw_log_set)

    @deprecated(
        "LogSets are deprecated and will be removed in a future version. "
        "Logs should be stored as a log channel in a Nominal datasource instead."
    )
    def list_logsets(self) -> Sequence[tuple[str, LogSet]]:
        """List datasource (name, LogSet) pairs"""
        return list(self.logsets())

    @deprecated(
        "LogSets are deprecated and will be removed in a future version. "
        "Logs should be stored as a log channel in a Nominal datasource instead."
    )
    def get_logset(self, data_scope_name: str) -> LogSet:
        scope = self.get_data_scope(data_scope_name)
        if isinstance(scope, LogSet):
            return scope
        else:
            raise ValueError(f"Data scope {data_scope_name} is not a logset: ({type(scope)})")

    @abc.abstractmethod
    def archive(self) -> None:
        """Archive this container, hiding it from the UI"""

    @abc.abstractmethod
    def unarchive(self) -> None:
        """Unarchive this container, allowing it to be shown on the UI"""

    @abc.abstractmethod
    def add_attachment(self, attachment: Attachment | str) -> None: ...

    def add_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        for attachment in attachments:
            self.add_attachment(attachment)

    @abc.abstractmethod
    def attachments(self) -> Iterable[Attachment]: ...

    def list_attachments(self) -> Sequence[Attachment]:
        return list(self.attachments())

    def remove_attachment(self, attachment: Attachment | str) -> None:
        self.remove_attachments([rid_from_instance_or_string(attachment)])

    @abc.abstractmethod
    def remove_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None: ...
