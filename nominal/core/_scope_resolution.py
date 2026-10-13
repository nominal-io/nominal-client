"""Internal model of the named data scopes on an asset or run, and their resolution to SDK resources.

Nothing here is public API: `Asset`, `Run`, and the SDK's own tooling read scopes through it, and each public
method raises its own error when a scope does not resolve. A scope resolves when its resource exists and the
caller can read it; the batch endpoints do not distinguish the two.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from nominal.core.connection import Connection, _get_connections
from nominal.core.dataset import Dataset, _get_datasets
from nominal.core.datasource import DataSource
from nominal.core.video import Video, _get_videos
from nominal.protos.asset.v2 import asset_pb2
from nominal.protos.run.v1 import run_service_pb2


@dataclass(frozen=True)
class DataScopeRef:
    """The resource behind one named data scope, and the series tags it pre-filters with."""

    rid: str
    series_tags: Mapping[str, str]


@dataclass(frozen=True)
class DataScopes:
    """An asset's or run's named data scopes by resource type, read from one payload.

    Names are unique within an asset or run. Log-set sources have no SDK resource and are left out.
    """

    datasets: Mapping[str, DataScopeRef]
    connections: Mapping[str, DataScopeRef]
    videos: Mapping[str, DataScopeRef]
    spatials: Mapping[str, DataScopeRef]

    @classmethod
    def from_data_scopes(cls, scopes: Iterable[asset_pb2.DataScope | run_service_pb2.DataScope]) -> DataScopes:
        """Read an asset's data scopes, or the asset scopes a run inherits."""
        return cls._from_sources((scope.data_scope_name, scope.data_source, scope.series_tags) for scope in scopes)

    @classmethod
    def from_data_sources(cls, sources: Mapping[str, run_service_pb2.RunDataSource]) -> DataScopes:
        """Read a run's own data sources by ref name."""
        return cls._from_sources((name, source.data_source, source.series_tags) for name, source in sources.items())

    @classmethod
    def _from_sources(
        cls,
        sources: Iterable[tuple[str, asset_pb2.DataSource | run_service_pb2.DataSource, Mapping[str, str]]],
    ) -> DataScopes:
        datasets: dict[str, DataScopeRef] = {}
        connections: dict[str, DataScopeRef] = {}
        videos: dict[str, DataScopeRef] = {}
        spatials: dict[str, DataScopeRef] = {}
        for name, source, tags in sources:
            match source.WhichOneof("data_source"):
                case "dataset":
                    datasets[name] = DataScopeRef(source.dataset, dict(tags))
                case "connection":
                    connections[name] = DataScopeRef(source.connection, dict(tags))
                case "video":
                    videos[name] = DataScopeRef(source.video, dict(tags))
                case "spatial":
                    spatials[name] = DataScopeRef(source.spatial, dict(tags))
        return cls(datasets=datasets, connections=connections, videos=videos, spatials=spatials)

    def __contains__(self, name: object) -> bool:
        """Whether a dataset, connection, video, or spatial scope has this name."""
        return any(name in scopes for scopes in (self.datasets, self.connections, self.videos, self.spatials))

    def named(self, name: str) -> DataScopes:
        """Only the scope with this name, if there is one."""

        def only(scopes: Mapping[str, DataScopeRef]) -> Mapping[str, DataScopeRef]:
            return {name: scopes[name]} if name in scopes else {}

        return DataScopes(
            datasets=only(self.datasets),
            connections=only(self.connections),
            videos=only(self.videos),
            spatials=only(self.spatials),
        )


def resolve_datasets(clients: Dataset._Clients, scopes: Mapping[str, DataScopeRef]) -> Mapping[str, Dataset]:
    """The readable datasets behind these scopes by scope name, fetched in one request."""
    rids = [scope.rid for scope in scopes.values()]
    by_rid = {dataset.rid: dataset for dataset in _get_datasets(clients.auth_header, clients.catalog, rids)}
    return {
        name: Dataset._from_conjure(clients, by_rid[scope.rid]) for name, scope in scopes.items() if scope.rid in by_rid
    }


def resolve_connections(clients: DataSource._Clients, scopes: Mapping[str, DataScopeRef]) -> Mapping[str, Connection]:
    """The readable connections behind these scopes by scope name, fetched in one request."""
    rids = [scope.rid for scope in scopes.values()]
    by_rid = {connection.rid: connection for connection in _get_connections(clients, rids)}
    return {
        name: Connection._from_conjure(clients, by_rid[scope.rid])
        for name, scope in scopes.items()
        if scope.rid in by_rid
    }


def resolve_videos(clients: Video._Clients, scopes: Mapping[str, DataScopeRef]) -> Mapping[str, Video]:
    """The readable videos behind these scopes by scope name, fetched in one request."""
    rids = [scope.rid for scope in scopes.values()]
    by_rid = {video.rid: video for video in _get_videos(clients, rids)}
    return {
        name: Video._from_conjure(clients, by_rid[scope.rid]) for name, scope in scopes.items() if scope.rid in by_rid
    }
