from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest

if sys.version_info < (3, 13):
    pytest.skip("Migration module requires Python 3.13+ (TypeVar default parameter)", allow_module_level=True)

from nominal.experimental.migration.migration_state import MigrationState
from nominal.experimental.migration.migrator.asset_migrator import AssetCopyOptions, AssetMigrator
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.migrator.run_migrator import RunMigrator
from nominal.experimental.migration.resource_type import ResourceType


def _make_client(name: str) -> MagicMock:
    client = MagicMock(name=name)
    client._clients.workspace_rid = f"{name}-workspace"
    workspace = MagicMock()
    workspace.rid = f"{name}-workspace"
    client.get_workspace.return_value = workspace
    return client


def _make_run(*, rid: str, name: str | None = None) -> MagicMock:
    run = MagicMock()
    run.rid = rid
    run.name = name or rid
    run.start = "2026-04-10T00:00:00Z"
    run.end = "2026-04-10T01:00:00Z"
    run.description = f"description-{rid}"
    run.properties = {"rid": rid}
    run.labels = [rid]
    run.assets = []
    run.links = []
    run.list_attachments.return_value = []
    run.search_workbooks.return_value = []
    return run


def _make_asset(rid: str) -> MagicMock:
    asset = MagicMock()
    asset.rid = rid
    asset.name = f"asset-{rid}"
    asset.description = f"description-{rid}"
    asset.properties = {"rid": rid}
    asset.labels = [rid]
    asset.list_runs.return_value = []
    asset.search_workbooks.return_value = []
    asset._list_dataset_scopes.return_value = []
    asset.list_datasets.return_value = []
    asset.list_videos.return_value = []
    asset.search_events.return_value = []
    asset.search_data_reviews.return_value = []
    return asset


def test_default_destination_client_behavior_is_unchanged() -> None:
    destination_client = _make_client("destination")
    source_run = _make_run(rid="run-default")
    destination_run = MagicMock(rid="dest-run-default")
    destination_client.create_run.return_value = destination_run

    ctx = MigrationContext(destination_client=destination_client, migration_state=MigrationState())
    result = RunMigrator(ctx).copy_from(source_run)

    assert ctx.destination_client_for(source_run) is destination_client
    assert result is destination_run
    destination_client.create_run.assert_called_once()
    assert ctx.migration_state.get_mapped_rid(ResourceType.RUN, source_run.rid) == destination_run.rid


def test_custom_destination_client_resolver_can_vary_per_source_resource() -> None:
    default_client = _make_client("default")
    run_client_a = _make_client("run-a")
    run_client_b = _make_client("run-b")

    source_run_a = _make_run(rid="run-a")
    source_run_b = _make_run(rid="run-b")

    destination_run_a = MagicMock(rid="dest-run-a")
    destination_run_b = MagicMock(rid="dest-run-b")
    run_client_a.create_run.return_value = destination_run_a
    run_client_b.create_run.return_value = destination_run_b

    ctx = MigrationContext(
        destination_client=default_client,
        migration_state=MigrationState(),
        destination_client_resolver=lambda source_resource: run_client_a
        if source_resource.rid == source_run_a.rid
        else run_client_b,
    )
    migrator = RunMigrator(ctx)

    assert migrator.copy_from(source_run_a) is destination_run_a
    assert migrator.copy_from(source_run_b) is destination_run_b

    run_client_a.create_run.assert_called_once()
    run_client_b.create_run.assert_called_once()
    default_client.create_run.assert_not_called()
    assert ctx.migration_state.get_mapped_rid(ResourceType.RUN, source_run_a.rid) == destination_run_a.rid
    assert ctx.migration_state.get_mapped_rid(ResourceType.RUN, source_run_b.rid) == destination_run_b.rid


def test_nested_migrations_resolve_destination_client_per_child_resource() -> None:
    default_client = _make_client("default")
    asset_client = _make_client("asset")
    run_client = _make_client("run")

    source_asset = _make_asset("asset-source")
    source_run = _make_run(rid="run-source")
    source_asset.list_runs.return_value = [source_run]

    destination_asset = MagicMock(rid="asset-destination")
    destination_run = MagicMock(rid="run-destination")
    asset_client.create_asset.return_value = destination_asset
    run_client.create_run.return_value = destination_run
    run_client.get_run.return_value = destination_run

    ctx = MigrationContext(
        destination_client=default_client,
        migration_state=MigrationState(),
        destination_client_resolver=lambda source_resource: asset_client
        if source_resource.rid == source_asset.rid
        else run_client,
    )

    AssetMigrator(ctx).copy_from(
        source_asset,
        AssetCopyOptions(
            dataset_config=None,
            include_events=False,
            include_runs=True,
            include_video=False,
            include_checklists=False,
        ),
    )

    asset_client.create_asset.assert_called_once()
    run_client.create_run.assert_called_once()
    asset_client.create_run.assert_not_called()
    default_client.create_asset.assert_not_called()
    default_client.create_run.assert_not_called()
    run_client.get_run.assert_called_once_with(destination_run.rid)
    assert ctx.migration_state.get_mapped_rid(ResourceType.ASSET, source_asset.rid) == destination_asset.rid
    assert ctx.migration_state.get_mapped_rid(ResourceType.RUN, source_run.rid) == destination_run.rid
