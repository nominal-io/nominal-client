"""Runner ordering around retention-sensitive run finalization."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from nominal.experimental.migration import migration_runner, parallel_migration_runner
from nominal.experimental.migration.config.migration_data_config import AssetInclusionConfig, MigrationDatasetConfig
from nominal.experimental.migration.config.migration_resources import AssetResources, MigrationResources
from nominal.experimental.migration.migration_runner import MigrationRunner
from nominal.experimental.migration.migrator.context import MigrationContext


@pytest.mark.parametrize("parallel", [False, True])
@pytest.mark.parametrize("asset_fails", [False, True])
def test_runs_finalize_only_after_successful_asset_work(
    parallel: bool, asset_fails: bool, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Both runners defer run finalization until asset work succeeds, leaving failed runs resumable."""
    asset = MagicMock(rid="asset-1")
    source = MagicMock(rid="run-1", assets=[], end=123)
    destination = MagicMock(assets=[], end=None)
    destination.refresh.return_value = destination
    completed = []

    def make_asset_migrator(ctx: MigrationContext) -> MagicMock:
        def copy_from(*args: object) -> None:
            assert ctx.defer_run_finalization
            ctx.pending_run_finalizations.put((source, destination, 123))
            if asset_fails:
                raise ValueError("asset failed")
            completed.append(asset.rid)

        return MagicMock(copy_from=copy_from)

    def update(*, end: int) -> None:
        assert completed == [asset.rid]
        assert end == 123

    destination.update.side_effect = update
    module = parallel_migration_runner if parallel else migration_runner
    monkeypatch.setattr(module, "AssetMigrator", make_asset_migrator)
    runner = MigrationRunner(
        migration_resources=MigrationResources(
            source_assets={asset.rid: AssetResources(asset, [])}, source_standalone_templates=[]
        ),
        dataset_config=MigrationDatasetConfig(preserve_dataset_uuid=False, include_dataset_files=False),
        destination_client=MagicMock(),
        asset_inclusion_config=AssetInclusionConfig(include_workbooks=False),
        migration_state_path=tmp_path / "state.json",
    )

    def run() -> None:
        if parallel:
            parallel_migration_runner.run_parallel_migration(runner, max_workers=2)
        else:
            runner.run_migration()

    if asset_fails:
        with pytest.raises((ValueError, RuntimeError), match="asset failed"):
            run()
        destination.update.assert_not_called()
    else:
        run()
        destination.update.assert_called_once()
