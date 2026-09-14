"""Listing and resolution of the data scopes attached to an `Asset` or a `Run`.

An `Asset` names its data sources with a data scope name and reads them from `data_scopes`;
a `Run` names them with a ref name and reads them from `data_sources`. Once either has grouped
its scopes into `{type: {name: rid}}` the remaining work is identical, so `DataScopeMixin` holds
it and each class supplies only the grouping.

This sits at core level rather than under `_utils`: it imports the concrete data source classes,
so it layers above them rather than beside the transport helpers, and `DataScopeMixin` supplies
public API to `Asset` and `Run` the way `_DatasetWrapper` does from `dataset.py`.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import Callable, ClassVar, Iterable, Mapping, Protocol, Sequence, TypeAlias, get_args

from nominal_api import scout_run_api

from nominal.core._utils.api_tools import ScopeTypeSpecifier, extract_scope_rid, pair_by_rid
from nominal.core.connection import Connection, _get_connections
from nominal.core.dataset import Dataset, _get_datasets
from nominal.core.datasource import DataSource
from nominal.core.video import Video, _get_videos

ScopeType: TypeAlias = Connection | Dataset | Video


class ScopeClients(DataSource._Clients, Video._Clients, Protocol):
    """The clients needed to resolve any scope type. `Asset._Clients` and `Run._Clients` both satisfy this."""


def resolve_datasets(clients: ScopeClients, rids_by_name: Mapping[str, str]) -> Sequence[tuple[str, Dataset]]:
    """Resolve dataset RIDs in one request. Names whose dataset did not come back are omitted."""
    datasets = _get_datasets(clients.auth_header, clients.catalog, rids_by_name.values())
    return pair_by_rid(rids_by_name, [Dataset._from_conjure(clients, dataset) for dataset in datasets])


def resolve_connections(clients: ScopeClients, rids_by_name: Mapping[str, str]) -> Sequence[tuple[str, Connection]]:
    """Resolve connection RIDs in one request. Names whose connection did not come back are omitted."""
    connections = _get_connections(clients, rids_by_name.values())
    return pair_by_rid(rids_by_name, [Connection._from_conjure(clients, connection) for connection in connections])


def resolve_videos(clients: ScopeClients, rids_by_name: Mapping[str, str]) -> Sequence[tuple[str, Video]]:
    """Resolve video RIDs in one request. Names whose video did not come back are omitted."""
    videos = _get_videos(clients, rids_by_name.values())
    return pair_by_rid(rids_by_name, [Video._from_conjure(clients, video) for video in videos])


_Resolver: TypeAlias = Callable[[ScopeClients, Mapping[str, str]], Sequence[tuple[str, ScopeType]]]


@dataclass(frozen=True)
class _ScopeKind:
    """Everything that varies between scope types: what to call one, and how to resolve its RIDs."""

    noun: str
    resolve: _Resolver


# The one place a scope type is enumerated. `noun` is what an error message calls it, which is
# the type's own name except for spatial.
# The scope types core can resolve into a `ScopeType`. Deliberately a subset of `SCOPE_TYPES`:
# a spatial scope groups like any other, but `Spatial` lives in `nominal.experimental.spatial`
# and core cannot name it, so it has no resolver here.
_SCOPE_KINDS: Mapping[ScopeTypeSpecifier, _ScopeKind] = {
    "dataset": _ScopeKind("dataset", resolve_datasets),
    "connection": _ScopeKind("connection", resolve_connections),
    "video": _ScopeKind("video", resolve_videos),
}

RESOLVABLE_SCOPE_TYPES: tuple[ScopeTypeSpecifier, ...] = tuple(_SCOPE_KINDS)

# Every type a scope can have on the wire, resolvable or not. `group_scope_rids` keys by these,
# so a caller that knows how to resolve a type core doesn't — `nominal.experimental.spatial` —
# reads its RIDs from the same grouping.
SCOPE_TYPES: tuple[ScopeTypeSpecifier, ...] = get_args(ScopeTypeSpecifier)


class _NamedDataSource(Protocol):
    """A scope or ref carrying a `DataSource` union. `DataScope` and `RunDataSource` both match."""

    @property
    def data_source(self) -> scout_run_api.DataSource: ...


def group_scope_rids(
    named_sources: Iterable[tuple[str, _NamedDataSource]],
) -> Mapping[ScopeTypeSpecifier, Mapping[str, str]]:
    """Group `(name, source)` pairs into `{type: {name: rid}}` in one pass.

    An asset carries the name inside the scope and a run carries it as the mapping key, so both
    hand over pairs and neither needs a grouping of its own. Every type in `SCOPE_TYPES` is a key,
    mapping to an empty dict when none are attached, so callers index directly.
    """
    by_type: dict[str, dict[str, str]] = {}
    for name, source in named_sources:
        rid = extract_scope_rid(source.data_source)
        if rid is not None:
            by_type.setdefault(source.data_source.type.lower(), {})[name] = rid
    return {scope_type: by_type.get(scope_type, {}) for scope_type in SCOPE_TYPES}


class DataScopeMixin(abc.ABC):
    """Listing and resolution of the data scopes attached to an `Asset` or a `Run`.

    Subclasses implement `_scope_rids_by_type` and set `_scope_name_label` / `_parent_label`
    for error messages. The public `get_*` and `get_data_scope` methods stay on the subclass so
    each keeps its own parameter name (`data_scope_name` on an asset, `ref_name` on a run); they
    delegate their bodies to `_scope_rid` and `_resolve_named_scope` here.
    """

    # static typing for required fields
    _clients: ScopeClients
    rid: str

    _scope_name_label: ClassVar[str]
    """What this class calls the name of a scope, for error messages."""

    _parent_label: ClassVar[str]
    """What this class is, for error messages."""

    @abc.abstractmethod
    def _scope_rids_by_type(self) -> Mapping[ScopeTypeSpecifier, Mapping[str, str]]:
        """Every scope attached to this object as `{type: {name: rid}}`, from a single payload.

        Every type in `SCOPE_TYPES` is a key, mapping to an empty dict when none are attached, so
        callers index directly rather than defaulting.

        Grouping in one pass is what keeps a listing to one request for the parent: resolving
        each type separately would re-fetch it once per type.
        """

    def _scope_rid(self, scope_type: ScopeTypeSpecifier, name: str) -> str:
        """RID of the named scope of this type, or raise ValueError if there is no such scope."""
        rid = self._scope_rids_by_type()[scope_type].get(name)
        if rid is None:
            raise ValueError(
                f"No {_SCOPE_KINDS[scope_type].noun} with {self._scope_name_label} '{name}' "
                f"found for this {self._parent_label}"
            )  # message preserved verbatim from the eight per-class getters it replaced
        return rid

    def _resolve_named_scope(self, name: str) -> ScopeType:
        """Resolve one scope by name, whatever its type.

        Narrowing to the named scope first means only its own type issues a request; the other
        three resolve no RIDs and so make no call at all.
        """
        by_type = self._scope_rids_by_type()
        for scope_type in RESOLVABLE_SCOPE_TYPES:
            rid = by_type[scope_type].get(name)
            if rid is None:
                continue

            resolved = _SCOPE_KINDS[scope_type].resolve(self._clients, {name: rid})
            if resolved:
                _, scope = resolved[0]
                return scope

            # The scope is attached, so an empty resolution means its data source came back
            # omitted rather than absent. Saying "no such data scope" would send the caller
            # looking for a typo that isn't there.
            raise ValueError(
                f"Data scope {name} on {self._parent_label} {self.rid} could not be resolved: its data source "
                "was not found, or you are not authorized to read it"
            )

        if name in by_type["spatial"]:
            raise ValueError(
                f"Data scope {name} on {self._parent_label} {self.rid} is a spatial, which core does not "
                f"resolve. Use nominal.experimental.spatial.get_spatial_from_{self._parent_label}."
            )

        raise ValueError(
            f"No such data scope found on {self._parent_label} {self.rid} with {self._scope_name_label} {name}"
        )

    def list_data_scopes(self) -> Sequence[tuple[str, ScopeType]]:
        """List every data scope attached to this object, of every type.

        Data sources that are not found, or that you are not authorized to read, are omitted.
        Use the singular `get_*` methods to surface the error for one scope.

        Returns:
            (name, scope) pairs, where the name is a data scope name on an asset and a ref name
            on a run, and scope is a dataset, connection, or video.

            Spatials are not included: they live in `nominal.experimental.spatial`, whose
            `list_spatials_in_asset` / `list_spatials_in_run` list them.
        """
        by_type = self._scope_rids_by_type()
        return [
            pair
            for scope_type in RESOLVABLE_SCOPE_TYPES
            for pair in _SCOPE_KINDS[scope_type].resolve(self._clients, by_type[scope_type])
        ]

    def list_datasets(self) -> Sequence[tuple[str, Dataset]]:
        """List the datasets attached to this object.

        Datasets that are not found, or that you are not authorized to read, are omitted.
        Use `get_dataset` to surface the error for one name.

        Returns:
            (name, dataset) pairs, where the name is a data scope name on an asset and a ref
            name on a run.
        """
        return resolve_datasets(self._clients, self._scope_rids_by_type()["dataset"])

    def list_connections(self) -> Sequence[tuple[str, Connection]]:
        """List the connections attached to this object.

        Connections that are not found, or that you are not authorized to read, are omitted.
        Use `get_connection` to surface the error for one name.

        Returns:
            (name, connection) pairs, where the name is a data scope name on an asset and a ref
            name on a run.
        """
        return resolve_connections(self._clients, self._scope_rids_by_type()["connection"])

    def list_videos(self) -> Sequence[tuple[str, Video]]:
        """List the videos attached to this object.

        Videos that are not found, or that you are not authorized to read, are omitted.
        Use `get_video` to surface the error for one name.

        Returns:
            (name, video) pairs, where the name is a data scope name on an asset and a ref name
            on a run.
        """
        return resolve_videos(self._clients, self._scope_rids_by_type()["video"])
