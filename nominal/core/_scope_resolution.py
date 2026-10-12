"""Read the named data scopes of an asset or run, and resolve them to SDK resources.

A scope resolves when its resource exists and the caller can read it; the backend does not distinguish the two.
Listing leaves out scopes that do not resolve, and looking one up raises `NominalDataScopeNotFoundError`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence, TypeVar

from nominal.core.connection import Connection, _get_connections
from nominal.core.dataset import Dataset, _get_datasets
from nominal.core.datasource import DataSource
from nominal.core.video import Video, _get_videos
from nominal.exceptions import NominalDataScopeNotFoundError
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


T = TypeVar("T")


def only_resolved(resolved: Sequence[tuple[str, T]], *, noun: str, name: str, owner: str) -> T:
    """The resource behind a single named scope.

    Raises:
        NominalDataScopeNotFoundError: If the scope did not resolve.
    """
    if not resolved:
        raise NominalDataScopeNotFoundError(f"No {noun} named {name!r} found on {owner}")
    return resolved[0][1]


def resolve_dataset_scope(
    clients: Dataset._Clients, scopes: DataScopes, name: str, *, owner: str
) -> tuple[Dataset, Mapping[str, str]]:
    """Resolve a named dataset scope to its Dataset and series tags.

    Raises:
        NominalNotFoundError: If no readable dataset has this scope name.
    """
    named = scopes.named(name).datasets
    dataset = only_resolved(resolve_datasets(clients, named), noun="dataset", name=name, owner=owner)
    return dataset, named[name].series_tags


def resolve_datasets(clients: Dataset._Clients, scopes: Mapping[str, DataScopeRef]) -> Sequence[tuple[str, Dataset]]:
    """Resolve dataset scopes in one request, leaving out any that do not resolve."""
    datasets = _get_datasets(clients.auth_header, clients.catalog, [scope.rid for scope in scopes.values()])
    by_rid = {dataset.rid: dataset for dataset in datasets}
    return [
        (name, Dataset._from_conjure(clients, by_rid[scope.rid]))
        for name, scope in scopes.items()
        if scope.rid in by_rid
    ]


def resolve_connections(
    clients: DataSource._Clients, scopes: Mapping[str, DataScopeRef]
) -> Sequence[tuple[str, Connection]]:
    """Resolve connection scopes in one request, leaving out any that do not resolve."""
    by_rid = {connection.rid: connection for connection in _get_connections(clients, [s.rid for s in scopes.values()])}
    return [
        (name, Connection._from_conjure(clients, by_rid[scope.rid]))
        for name, scope in scopes.items()
        if scope.rid in by_rid
    ]


def resolve_videos(clients: Video._Clients, scopes: Mapping[str, DataScopeRef]) -> Sequence[tuple[str, Video]]:
    """Resolve video scopes in one request, leaving out any that do not resolve."""
    by_rid = {video.rid: video for video in _get_videos(clients, [scope.rid for scope in scopes.values()])}
    return [
        (name, Video._from_conjure(clients, by_rid[scope.rid])) for name, scope in scopes.items() if scope.rid in by_rid
    ]
