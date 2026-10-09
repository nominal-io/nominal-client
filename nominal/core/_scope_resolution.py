"""Read resource references and resolve named dataset scopes."""

from __future__ import annotations

from typing import Iterable, Mapping

from nominal.core._utils.api_tools import ScopeTypeSpecifier
from nominal.core.dataset import Dataset, _get_dataset
from nominal.protos.asset.v2 import asset_pb2
from nominal.protos.run.v1 import run_service_pb2


def group_scope_rids(
    sources: Iterable[tuple[str, asset_pb2.DataSource | run_service_pb2.DataSource]],
) -> Mapping[ScopeTypeSpecifier, Mapping[str, str]]:
    """Group supported resource RIDs by type and local name from one parent payload."""
    grouped: dict[ScopeTypeSpecifier, dict[str, str]] = {
        "dataset": {},
        "connection": {},
        "video": {},
        "spatial": {},
    }
    for name, source in sources:
        match source.WhichOneof("data_source"):
            case "dataset":
                grouped["dataset"][name] = source.dataset
            case "connection":
                grouped["connection"][name] = source.connection
            case "video":
                grouped["video"][name] = source.video
            case "spatial":
                grouped["spatial"][name] = source.spatial
    return grouped


def lookup_dataset_scope(
    scopes: Iterable[asset_pb2.DataScope | run_service_pb2.DataScope],
    name: str,
) -> tuple[str, Mapping[str, str]] | None:
    """Find the first dataset-backed scope with this name and copy its tags."""
    for scope in scopes:
        if scope.data_scope_name == name and scope.data_source.WhichOneof("data_source") == "dataset":
            return scope.data_source.dataset, dict(scope.series_tags)
    return None


def resolve_dataset_scope(
    clients: Dataset._Clients,
    scopes: Iterable[asset_pb2.DataScope | run_service_pb2.DataScope],
    name: str,
) -> tuple[Dataset, Mapping[str, str]]:
    """Resolve a named scope to its backing Dataset and tags.

    Raises:
        ValueError: If the named scope is absent or the catalog does not return exactly one dataset.
    """
    scope = lookup_dataset_scope(scopes, name)
    if scope is None:
        raise ValueError(f"No such data scope found with data_scope_name {name}")
    rid, tags = scope
    return Dataset._from_conjure(clients, _get_dataset(clients.auth_header, clients.catalog, rid)), tags
