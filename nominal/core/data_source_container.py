from __future__ import annotations

import abc
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

SourceType: TypeAlias = Connection | Dataset | LogSet | Video
SourceTypeSpecifier: TypeAlias = Literal["dataset", "video", "connection", "logset"]

logger = logging.getLogger(__name__)


class _HasDataSources(abc.ABC):
    """Baseline class for API containers that can handle adding and listing datasources."""

    class _Clients(
        Attachment._Clients,
        DataSource._Clients,
        LogSet._Clients,
        Video._Clients,
        HasScoutParams,
        Protocol,
    ):
        """Required clients for working with datasources."""

    # Require that confirming classes have some clientsbunch
    # that inherits from this one
    @property
    @abc.abstractmethod
    def _clients(self) -> _Clients: ...

    @abc.abstractmethod
    def _rids_by_source_name(self, stype: SourceTypeSpecifier) -> Mapping[str, str]:
        """Retrieve a mapping of scop names to rids.

        Args:
            stype: source type specifier

        Returns:
            Mapping of datasource name => datasource rid for the given source type
        """

    @abc.abstractmethod
    def _add_data_source(
        self,
        source_name: str,
        source: HasRid | str,
        source_type: SourceTypeSpecifier,
        *,
        series_tags: Mapping[str, str] | None = None,
        offset: datetime.timedelta | None = None,
    ) -> None:
        """Add datasource to the underlying container.

        Args:
            source_name: datasource name to add the source with
            source: Instance or rid of the datasource to add to the underlying container
            source_type: Type of datasource being added
            series_tags: Mapping of tag key-value pairs to filter data with before adding to the container
            offset: Time offset to add the data source with, relative to the container start time
        """

    def _add_data_sources(
        self,
        sources: Mapping[str, HasRid | str],
        source_type: SourceTypeSpecifier,
        *,
        series_tags: Mapping[str, str] | None = None,
        offset: datetime.timedelta | None = None,
    ) -> None:
        """Add multiple datasources to the underlying container.

        Args:
            sources: Mapping of datasource names to (instances of or rids to) datasources
            source_type: Type of datasource being added
            series_tags: Mapping of tag key-value pairs to filter data with before adding to the container
            offset: Time offset to add the data source with, relative to the container start time
        """
        for source_name, source in sources.items():
            self._add_data_source(source_name, source, source_type, series_tags=series_tags, offset=offset)


class _DatasetContainer(_HasDataSources):
    """Base class for API containers that support adding and listing datasets."""

    def add_dataset(
        self,
        data_source_name: str,
        dataset: Dataset | str,
        *,
        series_tags: Mapping[str, str] | None = None,
        offset: datetime.timedelta | None = None,
    ) -> None:
        """Add dataset to the API container by the given datasource name

        Args:
            data_source_name: Name of the datasource to add the dataset to the container with
            dataset: Dataset (or rid) to add to the container
            series_tags: Mapping of tag key-value pairs to filter data with before adding to the container
            offset: Time offset to add the data source with, relative to the container start time
        """
        self._add_data_source(data_source_name, dataset, "dataset", series_tags=series_tags, offset=offset)

    def add_datasets(
        self,
        datasets: Mapping[str, Dataset | str],
        *,
        series_tags: Mapping[str, str] | None = None,
        offset: datetime.timedelta | None = None,
    ) -> None:
        """Add datasets to API container by the given datasource names

        Args:
            datasets: Mapping of datasource name => dataset (or rid) to add to the container
            series_tags: Mapping of tag key-value pairs to filter data with before adding to the container
            offset: Time offset to add the data source with, relative to the container start time
        """
        self._add_data_sources(datasets, "dataset", series_tags=series_tags, offset=offset)

    def datasets(self) -> Iterable[tuple[str, Dataset]]:
        """Iterate over (datasource name, Dataset) pairs within the API container."""
        rids_by_name = self._rids_by_source_name(stype="dataset")
        if not rids_by_name:
            return

        for source_name, rid in rids_by_name.items():
            try:
                dataset = _get_dataset(self._clients.auth_header, self._clients.catalog, rid)
                yield source_name, Dataset._from_conjure(self._clients, dataset)
            except Exception:
                logger.exception("Failed to get dataset '%s' with rid '%s'", source_name, rid)

    def list_datasets(self) -> Sequence[tuple[str, Dataset]]:
        """List (datasource name, Dataset) pairs within the API container."""
        return list(self.datasets())

    def get_dataset(self, data_source_name: str) -> Dataset:
        """Get the dataset contained in the API container with the given datasource name.

        Args:
            data_source_name: Name of the datasource containing the dataset to return

        Returns:
            Dataset with the given datasource name

        Raises:
            ValueError: No dataset found with the given datasource name
        """
        for source_name, dataset in self.datasets():
            if source_name == data_source_name:
                return dataset

        raise ValueError(f"No dataset found with name {data_source_name}")


class _ConnectionContainer(_HasDataSources):
    """Protocol for API containers that can handle adding and listing connections."""

    def add_connection(
        self,
        data_source_name: str,
        connection: Connection | str,
        *,
        series_tags: Mapping[str, str] | None = None,
        offset: datetime.timedelta | None = None,
    ) -> None:
        """Add connection to the API container by the given datasource name

        Args:
            data_source_name: Name of the datasource to add the connection to the container with
            connection: Connection (or rid) to add to the container
            series_tags: Mapping of tag key-value pairs to filter data with before adding to the container
            offset: Time offset to add the data source with, relative to the container start time
        """
        self._add_data_source(data_source_name, connection, "connection", series_tags=series_tags, offset=offset)

    def add_connections(
        self,
        connections: Mapping[str, Connection | str],
        *,
        series_tags: Mapping[str, str] | None = None,
        offset: datetime.timedelta | None = None,
    ) -> None:
        """Add connections to API container by the given datasource names

        Args:
            connections: Mapping of datasource name => connection (or rid) to add to the container
            series_tags: Mapping of tag key-value pairs to filter data with before adding to the container
            offset: Time offset to add the data source with, relative to the container start time
        """
        self._add_data_sources(connections, "connection", series_tags=series_tags, offset=offset)

    def connections(self) -> Iterable[tuple[str, Connection]]:
        """Iterate over (datasource name, Connection) pairs within the API container."""
        rids_by_name = self._rids_by_source_name(stype="connection")
        if not rids_by_name:
            return

        for source_name, rid in rids_by_name.items():
            try:
                connection = _get_connections(self._clients, [rid])[0]
                yield source_name, Connection._from_conjure(self._clients, connection)
            except Exception:
                logger.exception("Failed to get connection '%s' with rid '%s'", source_name, rid)

    def list_connections(self) -> Sequence[tuple[str, Connection]]:
        """List (datasource name, Connection) pairs within the API container."""
        return list(self.connections())

    def get_connection(self, data_source_name: str) -> Connection:
        """Get the connection contained in the API container with the given datasource name.

        Args:
            data_source_name: Name of the datasource containing the connection to return

        Returns:
            Connection with the given datasource name

        Raises:
            ValueError: No connection found with the given datasource name
        """
        for source_name, connection in self.connections():
            if source_name == data_source_name:
                return connection

        raise ValueError(f"No connection found with name {data_source_name}")


class _VideoContainer(_HasDataSources):
    """Base class for API containers that can handle adding and listing videos."""

    def add_video(
        self,
        data_source_name: str,
        video: Video | str,
    ) -> None:
        """Add video to the API container by the given datasource name

        Args:
            data_source_name: Name of the datasource to add the video to the container with
            video: Video (or rid) to add to the container

        TODO(drake): support tags, offset
        """
        self._add_data_source(data_source_name, video, "video")

    def add_videos(
        self,
        videos: Mapping[str, Video | str],
    ) -> None:
        """Add videos to API container by the given datasource names

        Args:
            videos: Mapping of datasource name => video (or rid) to add to the container

        TODO(drake): support tags, offset
        """
        self._add_data_sources(videos, "video")

    def videos(self) -> Iterable[tuple[str, Video]]:
        """Iterate over (datasource name, Video) pairs within the API container."""
        rids_by_name = self._rids_by_source_name(stype="video")
        for source_name, rid in rids_by_name.items():
            try:
                raw_video = _get_video(self._clients, rid)
                yield source_name, Video._from_conjure(self._clients, raw_video)
            except Exception:
                logger.exception("Failed to get video '%s' with rid '%s'", source_name, rid)

    def list_videos(self) -> Sequence[tuple[str, Video]]:
        """List (datasource name, Video) pairs within the API container."""
        return list(self.videos())

    def get_video(self, data_source_name: str) -> Video:
        """Get the video contained in the API container with the given datasource name.

        Args:
            data_source_name: Name of the datasource containing the video to return

        Returns:
            Video with the given datasource name

        Raises:
            ValueError: No video found with the given datasource name
        """
        for source_name, video in self.videos():
            if source_name == data_source_name:
                return video

        raise ValueError(f"No video found with name {data_source_name}")


class _LogsetContainer(_HasDataSources):
    """Base class for API containers that can handle adding and listing logsets."""

    @deprecated(
        "LogSets are deprecated and will be removed in a future version. "
        "Logs should be stored as a log channel in a Nominal datasource instead."
    )
    def add_log_set(
        self,
        data_source_name: str,
        log_set: LogSet | str,
    ) -> None:
        """Add logset to the API container by the given datasource name

        Args:
            data_source_name: Name of the datasource to add the logset to the container with
            log_set: LogSet (or rid) to add to the container

        TODO(drake): support tags, offset
        """
        self._add_data_source(data_source_name, log_set, "logset")

    @deprecated(
        "LogSets are deprecated and will be removed in a future version. "
        "Logs should be stored as a log channel in a Nominal datasource instead."
    )
    def add_log_sets(
        self,
        log_sets: Mapping[str, LogSet | str],
    ) -> None:
        """Add logsets to API container by the given datasource names

        Args:
            log_sets: Mapping of datasource name => logset (or rid) to add to the container

        TODO(drake): support tags, offset
        """
        self._add_data_sources(log_sets, "logset")

    @deprecated(
        "LogSets are deprecated and will be removed in a future version. "
        "Logs should be stored as a log channel in a Nominal datasource instead."
    )
    def logsets(self) -> Iterable[tuple[str, LogSet]]:
        """Iterate over (datasource name, LogSet) pairs within the API container."""
        rids_by_name = self._rids_by_source_name(stype="logset")
        for source_name, rid in rids_by_name.items():
            try:
                raw_log_set = _get_log_set(self._clients, rid)
                yield source_name, LogSet._from_conjure(self._clients, raw_log_set)
            except Exception:
                logger.exception("Failed to get logset '%s' with rid '%s'", source_name, rid)

    @deprecated(
        "LogSets are deprecated and will be removed in a future version. "
        "Logs should be stored as a log channel in a Nominal datasource instead."
    )
    def list_logsets(self) -> Sequence[tuple[str, LogSet]]:
        """List (datasource name, LogSet) pairs within the API container."""
        return list(self.logsets())

    @deprecated(
        "LogSets are deprecated and will be removed in a future version. "
        "Logs should be stored as a log channel in a Nominal datasource instead."
    )
    def get_logset(self, data_source_name: str) -> LogSet:
        """Get the logset contained in the API container with the given datasource name.

        Args:
            data_source_name: Name of the datasource containing the logset to return

        Returns:
            LogSet with the given datasource name

        Raises:
            ValueError: No logset found with the given datasource name
        """
        for source_name, logset in self.logsets():
            if source_name == data_source_name:
                return logset

        raise ValueError(f"No logset found with name {data_source_name}")


class _AttachmentContainer(_HasDataSources):
    """Base class for API containers that can handle adding, listing, and removing attachments."""

    @abc.abstractmethod
    def add_attachment(self, attachment: Attachment | str) -> None:
        """Add the attachment to the API container.

        Args:
            attachment: Attachment (or rid) to add to the container
        """

    def add_attachments(self, attachments: Iterable[Attachment | str]) -> None:
        """Add the attachments to the API container.

        Args:
            attachments: Attachments (or rids) to add to the container
        """
        for attachment in attachments:
            self.add_attachment(attachment)

    @abc.abstractmethod
    def attachments(self) -> Iterable[Attachment]:
        """Iterate through attachments contained within the API container."""

    def list_attachments(self) -> Sequence[Attachment]:
        """List attachments contained within the API container."""
        return list(self.attachments())

    def remove_attachment(self, attachment: Attachment | str) -> None:
        """Remove attachment from the API container.

        Args:
            attachment: Attachment (or rid) to remove from the API container.
        """
        self.remove_attachments([rid_from_instance_or_string(attachment)])

    @abc.abstractmethod
    def remove_attachments(self, attachments: Iterable[Attachment | str]) -> None:
        """Remove attachments from the API container.

        Args:
            attachments: Attachments (or rids) to remove from the API container.
        """


class _DataSourceContainer(
    _DatasetContainer,
    _ConnectionContainer,
    _VideoContainer,
    _LogsetContainer,
    _AttachmentContainer,
):
    """Base class for API containers that can handle adding, listing, and removing datasources."""

    def data_sources(self) -> Iterable[tuple[str, SourceType]]:
        """Iterate over (datasource name, datasource) pairs stored within the API container."""
        yield from self.datasets()
        yield from self.connections()
        yield from self.videos()
        yield from self.logsets()

    def list_data_sources(self) -> Sequence[tuple[str, SourceType]]:
        """List (datasource name, datasource) pairs stored within the API container."""
        return list(self.data_sources())

    def get_data_source(self, data_source_name: str) -> SourceType:
        """Get the datasource stored within the API container with the given name.

        Args:
            data_source_name: Name of the datasource to retrieve from the API container.

        Returns:
            datasource stored within the API container with the given name

        Returns:
            ValueError: no such datasource found by the given name.
        """
        for name, source in self.data_sources():
            if name == data_source_name:
                return source

        raise ValueError(f"No such datasource found with name {data_source_name}")

    @abc.abstractmethod
    def remove_data_sources(
        self,
        *,
        names: Sequence[str] | None = None,
        sources: Sequence[SourceType | str] | None = None,
    ) -> None:
        """Remove datasources from this container.

        Args:
            names: datasource names to remove
            sources: rids or source objects to remove
        """
