"""Attach spatials to the core resources that hold data scopes.

`Asset` and `Run` do not know about spatials: the type lives here, in experimental,
so these are free functions taking the core resource rather than methods on it.
They read the same data-scope plumbing the core `add_*` / `list_*` methods use, so
a spatial added here shows up in the platform exactly as any other data source does.
"""

from __future__ import annotations

from typing import Sequence

from nominal_api import scout_asset_api, scout_run_api

from nominal.core import Asset, Run
from nominal.core._utils.api_tools import filter_scope_rids, rid_from_instance_or_string
from nominal.experimental.spatial._spatial import Spatial, _get_spatial


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
    request = scout_asset_api.AddDataScopesToAssetRequest(
        data_scopes=[
            scout_asset_api.CreateAssetDataScope(
                data_scope_name=data_scope_name,
                data_source=scout_run_api.DataSource(spatial=rid_from_instance_or_string(spatial)),
                series_tags={},
            ),
        ]
    )
    asset._clients.assets.add_data_scopes_to_asset(asset.rid, asset._clients.auth_header, request)


def get_spatial_from_asset(asset: Asset, data_scope_name: str) -> Spatial:
    """Retrieve a spatial from an asset by data scope name.

    Args:
        asset: Asset to resolve the scope on.
        data_scope_name: Name of the asset data scope to resolve.

    Returns:
        The spatial associated with the data scope name.

    Raises:
        ValueError: If no spatial data scope exists with the provided name.
    """
    rid = _spatial_scope_rids(asset).get(data_scope_name)
    if rid is None:
        raise ValueError(f"No spatial with data scope name '{data_scope_name}' found for asset {asset.rid}")
    return Spatial._from_conjure(asset._clients, _get_spatial(asset._clients, rid))


def list_spatials_in_asset(asset: Asset) -> Sequence[tuple[str, Spatial]]:
    """List the spatials associated with an asset.

    `Asset.list_data_scopes` does not include these: it returns only the scope types
    core knows about.

    Returns:
        (data_scope_name, spatial) pairs for each spatial scope.
    """
    return [
        (name, Spatial._from_conjure(asset._clients, _get_spatial(asset._clients, rid)))
        for name, rid in _spatial_scope_rids(asset).items()
    ]


def add_spatial_to_run(run: Run, ref_name: str, spatial: Spatial | str) -> None:
    """Add a spatial to a run under a ref name.

    Args:
        run: Run to add the spatial to.
        ref_name: Name for the data source within the run.
        spatial: Spatial, or spatial rid, to add.
    """
    request = scout_run_api.CreateRunDataSource(
        data_source=scout_run_api.DataSource(spatial=rid_from_instance_or_string(spatial)),
        series_tags={},
        offset=None,
    )
    run._clients.run.add_data_sources_to_run(run._clients.auth_header, {ref_name: request}, run.rid)


def get_spatial_from_run(run: Run, ref_name: str) -> Spatial:
    """Retrieve a spatial from a run by its ref name.

    Args:
        run: Run to resolve the reference on.
        ref_name: Name of the run datasource reference to resolve.

    Returns:
        The spatial associated with the ref name.

    Raises:
        ValueError: If no spatial reference exists with the provided name.
    """
    rid = _spatial_datasource_rids(run).get(ref_name)
    if rid is None:
        raise ValueError(f"No spatial with ref name '{ref_name}' found for run {run.rid}")
    return Spatial._from_conjure(run._clients, _get_spatial(run._clients, rid))


def list_spatials_in_run(run: Run) -> Sequence[tuple[str, Spatial]]:
    """List the spatials associated with a run.

    Returns:
        (ref_name, spatial) pairs for each spatial data source.
    """
    return [
        (name, Spatial._from_conjure(run._clients, _get_spatial(run._clients, rid)))
        for name, rid in _spatial_datasource_rids(run).items()
    ]


def _spatial_scope_rids(asset: Asset) -> dict[str, str]:
    """Spatial rids by data scope name, read from one fetch of the asset."""
    return dict(filter_scope_rids(asset._get_latest_api().data_scopes, "spatial"))


def _spatial_datasource_rids(run: Run) -> dict[str, str]:
    """Spatial rids by ref name, read from one fetch of the run."""
    return dict(run._list_datasource_rids("spatial"))
