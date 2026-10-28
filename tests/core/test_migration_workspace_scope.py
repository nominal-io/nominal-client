from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click import UsageError
from click.testing import CliRunner

from nominal.core import ArchiveStatusFilter, Workspace
from nominal.experimental.migration.config.migration_resources import AssetResources, MigrationResources
from nominal.experimental.migration.migration_cli import _require_resources_in_workspace, migrate_cmd

WORKSPACE = Workspace(rid="ri.workspace.a", id="a", org="ri.org")


def _resources(asset_rids: list[str], template_rids: list[str]) -> MigrationResources:
    return MigrationResources(
        source_assets={
            rid: AssetResources(asset=MagicMock(rid=rid), source_workbook_templates=[]) for rid in asset_rids
        },
        source_standalone_templates=[MagicMock(rid=rid) for rid in template_rids],
    )


def _source_client(asset_rids: list[str], run_asset_rids: list[str], template_rids: list[str]) -> MagicMock:
    client = MagicMock()
    client.search_assets.return_value = [MagicMock(rid=rid) for rid in asset_rids]
    client.search_runs.return_value = [MagicMock(assets=[rid]) for rid in run_asset_rids]
    client.search_workbook_templates.return_value = [MagicMock(rid=rid) for rid in template_rids]
    return client


def test_resources_in_workspace_accepts_assets_found_only_through_runs() -> None:
    """An asset created with a run is in scope even though asset search does not return it."""
    client = _source_client(asset_rids=["ri.asset.searched"], run_asset_rids=["ri.asset.run"], template_rids=["ri.t"])

    _require_resources_in_workspace(client, WORKSPACE, _resources(["ri.asset.searched", "ri.asset.run"], ["ri.t"]))


def test_resources_in_workspace_rejects_resources_outside_the_workspace() -> None:
    """Config assets and templates that the source workspace does not contain are named in the error."""
    client = _source_client(asset_rids=["ri.asset.in"], run_asset_rids=[], template_rids=[])

    with pytest.raises(UsageError, match="ri.asset.other, ri.t.other"):
        _require_resources_in_workspace(
            client, WORKSPACE, _resources(["ri.asset.in", "ri.asset.other"], ["ri.t.other"])
        )


def test_copy_requires_source_profile_pinned_to_a_workspace(tmp_path: Path) -> None:
    """`nom migrate copy` refuses to start when the source profile would fall back to the tenant default workspace."""
    config = tmp_path / "migration.yml"
    config.write_text(
        "migration:\n  name: scoped\n  include_dataset_files: false\n  preserve_dataset_uuid: false\n",
        encoding="utf-8",
    )
    unpinned_client = MagicMock()
    unpinned_client._clients.workspace_rid = None
    args = ["copy", "--source-profile", "source", "--destination-profile", "destination", "--config", str(config)]

    with (
        patch(
            "nominal.experimental.migration.migration_decorators.NominalClient.from_profile",
            return_value=unpinned_client,
        ),
        patch("nominal.experimental.migration.migration_cli.run_parallel_migration") as run_parallel_migration,
    ):
        result = CliRunner().invoke(migrate_cmd, args)

    assert result.exit_code != 0
    assert "--source-profile profile must set a workspace RID" in result.output
    run_parallel_migration.assert_not_called()


def test_prep_leaves_out_archived_assets_reached_through_runs(tmp_path: Path) -> None:
    """A run on an archived asset does not put that asset back into the generated config."""
    client = MagicMock()
    client.search_runs.return_value = [MagicMock(assets=["ri.asset.auto"]), MagicMock(assets=["ri.asset.archived"])]
    client.search_assets.side_effect = lambda archive_status=ArchiveStatusFilter.NOT_ARCHIVED: (
        [MagicMock(rid="ri.asset.archived")] if archive_status == ArchiveStatusFilter.ARCHIVED else []
    )
    output = tmp_path / "config.yml"

    with patch("nominal.cli.util.global_decorators.NominalClient.from_profile", return_value=client):
        result = CliRunner().invoke(
            migrate_cmd, ["prep", "--profile", "source", "--name", "scoped", "--output", str(output)]
        )

    assert result.exit_code == 0, result.output
    config = yaml.safe_load(output.read_text(encoding="utf-8"))
    assert config["migration"]["source_asset_rids"] == [{"asset_rid": "ri.asset.auto"}]
