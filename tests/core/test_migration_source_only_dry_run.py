"""Dry runs with and without a destination profile."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner, Result

from nominal.core._scope_resolution import DataScopes
from nominal.experimental.migration import migration_cli
from nominal.experimental.migration.config.migration_data_config import MigrationDatasetConfig
from nominal.experimental.migration.config.migration_resources import AssetResources, MigrationResources
from nominal.experimental.migration.migration_runner import MigrationRunner
from nominal.experimental.migration.parallel_migration_runner import run_parallel_migration
from nominal.experimental.migration.resource_type import ResourceType
from nominal.protos.asset.v2 import asset_pb2

_PREDICTED = [
    (ResourceType.ASSET, "asset"),
    (ResourceType.DATASET, "dataset"),
    (ResourceType.RUN, "run"),
    (ResourceType.CHECKLIST, "review-checklist"),
    (ResourceType.DATA_REVIEW, "data-review"),
    (ResourceType.WORKBOOK_TEMPLATE, "template"),
    (ResourceType.CHECKLIST, "standalone-checklist"),
]


def _dry_run(
    tmp_path: Path, destination_client: MagicMock | None, destination_client_resolver: MagicMock | None = None
) -> list[str | None]:
    """Dry-run an asset with a dataset, run, and data review, plus a standalone template and checklist.

    Returns the RID recorded for each resource in `_PREDICTED`.
    """
    dataset = MagicMock(rid="dataset", is_archived=False)
    run = MagicMock(rid="run", is_archived=False)
    data_review = MagicMock(rid="data-review", run_rid=run.rid)
    data_review.get_checklist.return_value = MagicMock(rid="review-checklist")
    asset = MagicMock(rid="asset")
    asset._scopes.return_value = DataScopes.from_data_scopes(
        [asset_pb2.DataScope(data_scope_name="scope", data_source=asset_pb2.DataSource(dataset=dataset.rid))]
    )
    asset.list_datasets.return_value = [("scope", dataset)]
    asset.list_runs.return_value = [run]
    asset.search_data_reviews.return_value = [data_review]
    runner = MigrationRunner(
        migration_resources=MigrationResources(
            source_assets={asset.rid: AssetResources(asset=asset, source_workbook_templates=[])},
            source_standalone_templates=[MagicMock(rid="template")],
            source_standalone_checklists=[MagicMock(rid="standalone-checklist")],
        ),
        dataset_config=MigrationDatasetConfig(preserve_dataset_uuid=True, include_dataset_files=False),
        destination_client=destination_client,
        destination_client_resolver=destination_client_resolver,
        migration_state_path=tmp_path / "state.json",
        dry_run=True,
    )

    run_parallel_migration(runner, max_workers=2)

    return [runner.migration_state.get_mapped_rid(resource_type, rid) for resource_type, rid in _PREDICTED]


def test_source_only_dry_run_predicts_source_resources(tmp_path: Path) -> None:
    """Without a destination client, a dry run still predicts every source resource it would create."""
    assert _dry_run(tmp_path, destination_client=None) == [rid for _, rid in _PREDICTED]


def test_explicit_destination_dry_run_still_reads_the_destination(tmp_path: Path) -> None:
    """With a destination client, a dry run predicts the same resources and still resolves and reads the destination."""
    destination_client = MagicMock(name="destination")
    resolver = MagicMock(return_value=destination_client)

    assert _dry_run(tmp_path, destination_client, resolver) == [rid for _, rid in _PREDICTED]
    resolver.assert_called()
    destination_client.get_workspace.assert_called()


def _invoke_copy(tmp_path: Path, *args: str, config_extra: str = "") -> tuple[Result, MagicMock, MagicMock]:
    """Run `nom migrate copy` on an empty config, returning the result and the profile and migration spies."""
    config = tmp_path / "migration.yml"
    config.write_text(
        "migration:\n  name: dry-run\n  include_dataset_files: false\n  preserve_dataset_uuid: false\n" + config_extra,
        encoding="utf-8",
    )
    with (
        patch("nominal.experimental.migration.migration_decorators.NominalClient.from_profile") as from_profile,
        patch.object(
            migration_cli, "run_parallel_migration", wraps=migration_cli.run_parallel_migration
        ) as run_migration,
    ):
        result = CliRunner().invoke(
            migration_cli.migrate_cmd, ["copy", "--source-profile", "source", "--config", str(config), *args]
        )
    return result, from_profile, run_migration


def test_copy_without_destination_profile_fails_before_migrating(tmp_path: Path) -> None:
    """A real copy without --destination-profile is a usage error and starts no migration."""
    result, _, run_migration = _invoke_copy(tmp_path)

    assert result.exit_code == 2
    assert "--destination-profile is required unless --dry-run is set" in result.output
    run_migration.assert_not_called()


def test_source_only_dry_run_leaves_state_files_untouched(tmp_path: Path) -> None:
    """A source-only dry run creates only the source client and neither rewrites nor versions a state file."""
    state_path = tmp_path / "migration_state.json"
    state_text = '{"rid_mapping": {"ASSET": {"source-asset": "destination-asset"}}}'
    state_path.write_text(state_text, encoding="utf-8")

    result, from_profile, run_migration = _invoke_copy(tmp_path, "--migration-state-path", str(state_path), "--dry-run")

    assert result.exit_code == 0, result.output
    run_migration.assert_called_once()
    assert [call.args for call in from_profile.call_args_list] == [("source",)]
    assert sorted(path.name for path in tmp_path.iterdir()) == ["migration.yml", "migration_state.json"]
    assert state_path.read_text(encoding="utf-8") == state_text


def test_source_only_dry_run_with_demo_workbooks_skips_the_sandbox_check(tmp_path: Path) -> None:
    """Without a destination, a set_to_demo_workbook dry run warns that it cannot check the sandbox and still runs."""
    result, _, run_migration = _invoke_copy(tmp_path, "--dry-run", config_extra="  set_to_demo_workbook: true\n")

    assert result.exit_code == 0, result.output
    run_migration.assert_called_once()
    assert "not checking that the destination workspace is 'sandbox'" in result.output
