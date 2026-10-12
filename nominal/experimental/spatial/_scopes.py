"""Attach spatials to the core resources that hold data scopes.

`Asset` and `Run` do not know about spatials: the type lives here, in experimental,
so these are free functions taking the core resource rather than methods on it.
They read the same data-scope plumbing the core `add_*` / `list_*` methods use, so
a spatial added here shows up in the platform exactly as any other data source does.
"""

from __future__ import annotations

from typing import Mapping, Sequence

from nominal.core import Asset, Run
from nominal.core._scope_resolution import DataScopeRef, only_resolved
from nominal.core._utils.api_tools import rid_from_instance_or_string
from nominal.experimental.spatial._spatial import Spatial, _get_spatials
from nominal.protos.asset.v2 import asset_pb2
from nominal.protos.run.v1 import run_service_pb2


def add_spatial_to_asset(asset: Asset, data_scope_name: str, spatial: Spatial | str) -> None:
    """Add a spatial to an asset under a data scope name.

    Assets map "data_scope_name" (the name within the asset for the data) to a spatial
    (or a spatial rid). The same kind of spatial should use the same data scope name
    across assets, since checklists and templates reference data by scope name.

    Args:
        asset: Asset to add the spatial to.
        data_scope_name: Name for the data within the asset.
        spatial: Spatial, or spatial rid, to add.
    """
    asset._add_data_scope(data_scope_name, asset_pb2.DataSource(spatial=rid_from_instance_or_string(spatial)))


def get_spatial_from_asset(asset: Asset, data_scope_name: str) -> Spatial:
    """Retrieve a spatial from an asset by data scope name.

    Args:
        asset: Asset to resolve the scope on.
        data_scope_name: Name of the asset data scope to resolve.

    Returns:
        The spatial associated with the data scope name.

    Raises:
        NominalNotFoundError: If no readable spatial has this data scope name.
    """
    return only_resolved(
        _resolve_spatials(asset._clients, asset._scopes().named(data_scope_name).spatials),
        noun="spatial",
        name=data_scope_name,
        owner=f"asset {asset.rid}",
    )


def list_spatials_in_asset(asset: Asset) -> Sequence[tuple[str, Spatial]]:
    """List the spatials associated with an asset.

    `Asset.list_data_scopes` does not include these: it returns only the scope types
    core knows about.

    Returns:
        (data_scope_name, spatial) pairs. Data scopes whose spatial is missing or not readable are left out.
    """
    return _resolve_spatials(asset._clients, asset._scopes().spatials)


def add_spatial_to_run(run: Run, ref_name: str, spatial: Spatial | str) -> None:
    """Add a spatial to a run under a ref name.

    Args:
        run: Run to add the spatial to.
        ref_name: Name for the data source within the run.
        spatial: Spatial, or spatial rid, to add.
    """
    run._add_data_sources({ref_name: run_service_pb2.DataSource(spatial=rid_from_instance_or_string(spatial))})


def get_spatial_from_run(run: Run, ref_name: str) -> Spatial:
    """Retrieve a spatial from a run by its ref name.

    Args:
        run: Run to resolve the reference on.
        ref_name: Name of the run datasource reference to resolve.

    Returns:
        The spatial associated with the ref name.

    Raises:
        NominalNotFoundError: If no readable spatial has this ref name.
    """
    return only_resolved(
        _resolve_spatials(run._clients, run._scopes().named(ref_name).spatials),
        noun="spatial",
        name=ref_name,
        owner=f"run {run.rid}",
    )


def list_spatials_in_run(run: Run) -> Sequence[tuple[str, Spatial]]:
    """List the spatials associated with a run.

    Returns:
        (ref_name, spatial) pairs. Refs whose spatial is missing or not readable are left out.
    """
    return _resolve_spatials(run._clients, run._scopes().spatials)


def _resolve_spatials(clients: Spatial._Clients, scopes: Mapping[str, DataScopeRef]) -> Sequence[tuple[str, Spatial]]:
    """Resolve spatial scopes in one request, leaving out any that do not resolve."""
    by_rid = {spatial.rid: spatial for spatial in _get_spatials(clients, [scope.rid for scope in scopes.values()])}
    return [
        (name, Spatial._from_conjure(clients, by_rid[scope.rid]))
        for name, scope in scopes.items()
        if scope.rid in by_rid
    ]
