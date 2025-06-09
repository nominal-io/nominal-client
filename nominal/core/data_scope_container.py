from __future__ import annotations

import datetime
import logging
from typing import Iterable, Literal, Mapping, Protocol, Sequence

from typing_extensions import TypeAlias, deprecated

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils import HasRid, rid_from_instance_or_string
from nominal.core.attachment import Attachment
from nominal.core.connection import Connection, _get_connections
from nominal.core.dataset import Dataset, _get_dataset
from nominal.core.datasource import DataSource
from nominal.core.log import LogSet, _get_log_set
from nominal.core.video import Video, _get_video

ScopeType: TypeAlias = "Connection | Dataset | LogSet | Video"
ScopeTypeSpecifier: TypeAlias = Literal["dataset", "video", "connection", "logset"]

logger = logging.getLogger(__name__)


class _HasDataScopes(Protocol):
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
        ...

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

    def _rids_by_scope_name(self, stype: ScopeTypeSpecifier) -> Mapping[str, str]:
        """Return a mapping of data scope name => data scope rid"""
        ...


class _DatasetContainer(_HasDataScopes, Protocol):
    class _Clients(
        DataSource._Clients,
        HasScoutParams,
        Protocol,
    ): ...

    # Require that confirming classes have some clientsbunch
    # that inherits from this one
    _clients: _Clients

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

        for scope_name, rid in rids_by_name.items():
            try:
                dataset = _get_dataset(self._clients.auth_header, self._clients.catalog, rid)
                yield scope_name, Dataset._from_conjure(self._clients, dataset)
            except Exception:
                logger.exception("Failed to get dataset '%s' with rid '%s'", scope_name, rid)

    def list_datasets(self) -> Sequence[tuple[str, Dataset]]:
        """List datasource (name, Dataset) pairs"""
        return list(self.datasets())

    def get_dataset(self, data_scope_name: str) -> Dataset:
        for scope_name, dataset in self.datasets():
            if scope_name == data_scope_name:
                return dataset

        raise ValueError(f"No dataset found with name {data_scope_name}")


class _ConnectionContainer(_HasDataScopes, Protocol):
    class _Clients(
        DataSource._Clients,
        HasScoutParams,
        Protocol,
    ): ...

    # Require that confirming classes have some clientsbunch
    # that inherits from this one
    _clients: _Clients

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

        for scope_name, rid in rids_by_name.items():
            try:
                connection = _get_connections(self._clients, [rid])[0]
                yield scope_name, Connection._from_conjure(self._clients, connection)
            except Exception:
                logger.exception("Failed to get connection '%s' with rid '%s'", scope_name, rid)

    def list_connections(self) -> Sequence[tuple[str, Connection]]:
        """List datasource (name, Connection) pairs"""
        return list(self.connections())

    def get_connection(self, data_scope_name: str) -> Connection:
        for scope_name, connection in self.connections():
            if scope_name == data_scope_name:
                return connection

        raise ValueError(f"No connection found with name {data_scope_name}")


class _VideoContainer(_HasDataScopes, Protocol):
    class _Clients(
        Video._Clients,
        HasScoutParams,
        Protocol,
    ): ...

    _clients: _Clients

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
            try:
                raw_video = _get_video(self._clients, rid)
                yield scope_name, Video._from_conjure(self._clients, raw_video)
            except Exception:
                logger.exception("Failed to get video '%s' with rid '%s'", scope_name, rid)

    def list_videos(self) -> Sequence[tuple[str, Video]]:
        """List datasource (name, Video) pairs"""
        return list(self.videos())

    def get_video(self, data_scope_name: str) -> Video:
        for scope_name, video in self.videos():
            if scope_name == data_scope_name:
                return video

        raise ValueError(f"No video found with name {data_scope_name}")


class _LogsetContainer(_HasDataScopes, Protocol):
    class _Clients(
        LogSet._Clients,
        HasScoutParams,
        Protocol,
    ): ...

    _clients: _Clients

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
            try:
                raw_log_set = _get_log_set(self._clients, rid)
                yield scope_name, LogSet._from_conjure(self._clients, raw_log_set)
            except Exception:
                logger.exception("Failed to get logset '%s' with rid '%s'", scope_name, rid)

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
        for scope_name, logset in self.logsets():
            if scope_name == data_scope_name:
                return logset

        raise ValueError(f"No logset found with name {data_scope_name}")


class _AttachmentContainer(Protocol):
    class _Clients(
        Attachment._Clients,
        HasScoutParams,
        Protocol,
    ): ...

    _clients: _Clients

    def add_attachment(self, attachment: Attachment | str) -> None: ...

    def add_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        for attachment in attachments:
            self.add_attachment(attachment)

    def attachments(self) -> Iterable[Attachment]: ...

    def list_attachments(self) -> Sequence[Attachment]:
        return list(self.attachments())

    def remove_attachment(self, attachment: Attachment | str) -> None:
        self.remove_attachments([rid_from_instance_or_string(attachment)])

    def remove_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None: ...


class _DataScopeContainer(
    _DatasetContainer,
    _ConnectionContainer,
    _VideoContainer,
    _LogsetContainer,
    _AttachmentContainer,
    Protocol,
):
    class _Clients(
        _DatasetContainer._Clients,
        _ConnectionContainer._Clients,
        _VideoContainer._Clients,
        _LogsetContainer._Clients,
        _AttachmentContainer._Clients,
        HasScoutParams,
        Protocol,
    ): ...

    # Require that confirming classes have some clientsbunch
    # that inherits from this one
    _clients: _Clients

    ####################
    # Abstract Methods #
    ####################

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
        ...

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
        ...

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

    def _rids_by_scope_name(self, stype: ScopeTypeSpecifier) -> Mapping[str, str]:
        """Return a mapping of data scope name => data scope rid"""
        ...

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
