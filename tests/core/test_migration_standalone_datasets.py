"""Standalone datasets: datasets migrated without an asset, listed under `standalone_dataset_rids`."""

from __future__ import annotations

from pathlib import Path
from typing import Callable
from unittest.mock import MagicMock, patch

import click
import pytest
import yaml
from click.testing import CliRunner

from nominal.core import NominalClient
from nominal.experimental.migration import migration_cli
from nominal.experimental.migration.config.migration_data_config import MigrationDatasetConfig
from nominal.experimental.migration.config.migration_resources import MigrationResources
from nominal.experimental.migration.migration_runner import MigrationRunner
from nominal.experimental.migration.migrator.dataset_migrator import DatasetCopyOptions, DatasetMigrator
from nominal.experimental.migration.parallel_migration_runner import run_parallel_migration


def _write_config(tmp_path: Path, **extra: object) -> Path:
    config_path = tmp_path / "migration.yml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "migration": {
                    "name": "standalone-datasets",
                    "include_dataset_files": False,
                    "preserve_dataset_uuid": True,
                    **extra,
                }
            }
        ),
        encoding="utf-8",
    )
    return config_path


def test_config_loads_standalone_datasets_from_source(tmp_path: Path) -> None:
    """Standalone datasets are fetched in one batch, kept in config order, and archived ones are skipped."""
    source_client = MagicMock()
    source_client.get_datasets.return_value = [
        MagicMock(rid="ds-b", is_archived=False),
        MagicMock(rid="ds-archived", is_archived=True),
        MagicMock(rid="ds-a", is_archived=False),
    ]
    config_path = _write_config(tmp_path, standalone_dataset_rids=["ds-a", "ds-archived", "ds-b"])

    _, resources, *_ = migration_cli._load_migration_config(source_client, config_path)

    assert [ds.rid for ds in resources.source_standalone_datasets] == ["ds-a", "ds-b"]
    assert resources.source_assets == {}
    source_client.get_datasets.assert_called_once_with(["ds-a", "ds-archived", "ds-b"])


@pytest.mark.parametrize("rids", ["ds-a", [""], [1]], ids=["string", "empty-string", "non-string"])
def test_config_rejects_malformed_standalone_dataset_rids(tmp_path: Path, rids: object) -> None:
    """standalone_dataset_rids must be a list of non-empty strings."""
    config_path = _write_config(tmp_path, standalone_dataset_rids=rids)

    with pytest.raises(click.UsageError, match="standalone_dataset_rids"):
        migration_cli._load_migration_config(MagicMock(), config_path)


def test_config_reports_missing_standalone_dataset(tmp_path: Path) -> None:
    """A standalone dataset RID the source cannot resolve is a usage error naming that RID."""
    source_client = MagicMock()
    source_client.get_datasets.return_value = [MagicMock(rid="ds-found", is_archived=False)]
    config_path = _write_config(tmp_path, standalone_dataset_rids=["ds-found", "ds-missing"])

    with pytest.raises(click.UsageError, match="ds-missing"):
        migration_cli._load_migration_config(source_client, config_path)


def _run_sequential(runner: MigrationRunner) -> None:
    runner.run_migration()


def _run_parallel(runner: MigrationRunner) -> None:
    run_parallel_migration(runner, max_workers=2)


@pytest.mark.parametrize("run", [_run_sequential, _run_parallel], ids=["sequential", "parallel"])
def test_runners_copy_standalone_datasets_with_config_options(
    tmp_path: Path, run: Callable[[MigrationRunner], None]
) -> None:
    """Both runners copy each standalone dataset with the config's file and UUID options, not the migrator defaults."""
    datasets = [MagicMock(rid="ds-a"), MagicMock(rid="ds-b")]
    runner = MigrationRunner(
        migration_resources=MigrationResources(
            source_assets={}, source_standalone_templates=[], source_standalone_datasets=datasets
        ),
        dataset_config=MigrationDatasetConfig(preserve_dataset_uuid=True, include_dataset_files=False),
        destination_client=MagicMock(),
        migration_state_path=tmp_path / "state.json",
    )

    with patch.object(DatasetMigrator, "copy_from") as copy_from:
        run(runner)

    expected_options = DatasetCopyOptions(include_files=False, preserve_uuid=True)
    assert sorted((call.args[0].rid, call.args[1]) for call in copy_from.call_args_list) == [
        ("ds-a", expected_options),
        ("ds-b", expected_options),
    ]


def test_prep_lists_orphaned_datasets_as_standalone_datasets(tmp_path: Path) -> None:
    """`nom migrate prep` lists datasets with no scope on a non-archived asset as standalone datasets."""
    scoped = MagicMock(rid="ds-scoped")
    orphans = [MagicMock(rid="ds-orphan-b"), MagicMock(rid="ds-orphan-a")]
    asset = MagicMock(rid="asset")
    asset.list_datasets.return_value = [("scope", scoped)]
    client = MagicMock()
    client.search_runs.return_value = []
    client.search_workbooks.return_value = []
    client.search_assets.return_value = [asset]
    client.get_asset.return_value = asset
    client.search_datasets.return_value = [scoped, *orphans]
    client.search_videos.return_value = []
    client.search_workbook_templates.return_value = []
    client.list_streaming_checklists.return_value = []
    client.search_containerized_extractors.return_value = []
    output_path = tmp_path / "migration.yml"

    with patch.object(NominalClient, "from_profile", return_value=client):
        result = CliRunner().invoke(
            migration_cli.migrate_cmd,
            ["prep", "--profile", "source", "--name", "prep", "--output", str(output_path)],
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    migration = yaml.safe_load(output_path.read_text(encoding="utf-8"))["migration"]
    assert migration["source_asset_rids"] == [{"asset_rid": "asset"}]
    assert migration["standalone_dataset_rids"] == ["ds-orphan-a", "ds-orphan-b"]
    # Only non-archived assets count, so a dataset whose only asset is archived is listed as orphaned.
    client.search_assets.assert_called_once_with()
