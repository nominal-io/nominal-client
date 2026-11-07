"""The sequential runner must behave like the parallel runner the CLI uses."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from nominal.experimental.migration.config.migration_data_config import MigrationDatasetConfig
from nominal.experimental.migration.config.migration_resources import AssetResources, MigrationResources
from nominal.experimental.migration.migration_runner import MigrationRunner

_MODULE = "nominal.experimental.migration.parallel_migration_runner"


def _runner(tmp_path: Path) -> MigrationRunner:
    return MigrationRunner(
        migration_resources=MigrationResources(
            source_assets={
                "asset-1": AssetResources(asset=MagicMock(rid="asset-1"), source_workbook_templates=[]),
                "asset-2": AssetResources(asset=MagicMock(rid="asset-2"), source_workbook_templates=[]),
            },
            source_standalone_templates=[MagicMock(rid="template")],
            source_standalone_checklists=[MagicMock(rid="checklist")],
            source_standalone_datasets=[MagicMock(rid="dataset")],
        ),
        dataset_config=MigrationDatasetConfig(preserve_dataset_uuid=True, include_dataset_files=False),
        destination_client=MagicMock(),
        migration_state_path=tmp_path / "state.json",
    )


def test_sequential_runner_copies_in_order_then_deferred_workbooks(tmp_path: Path) -> None:
    """Sequential runs keep the queue order and finish with the deferred-workbook step."""
    calls: list[str] = []
    with (
        patch(f"{_MODULE}.AssetMigrator.copy_from", lambda _self, asset, _options: calls.append(asset.rid)),
        patch(f"{_MODULE}.WorkbookTemplateMigrator.clone", lambda _self, template: calls.append(template.rid)),
        patch(f"{_MODULE}.ChecklistMigrator.copy_from", lambda _self, checklist, _options: calls.append(checklist.rid)),
        patch(f"{_MODULE}.DatasetMigrator.copy_from", lambda _self, dataset, _options: calls.append(dataset.rid)),
        patch.object(MigrationRunner, "migrate_deferred_workbooks", lambda _self, _ctx: calls.append("deferred")),
    ):
        _runner(tmp_path).run_migration()

    assert calls == ["asset-1", "asset-2", "template", "checklist", "dataset", "deferred"]


def test_sequential_runner_continues_past_failed_asset_and_raises_once(tmp_path: Path) -> None:
    """A failed asset does not stop later resources; one error naming it is raised at the end."""
    copied: list[str] = []

    def copy_asset(_self: object, asset: MagicMock, _options: object) -> None:
        if asset.rid == "asset-1":
            raise ValueError("boom")
        copied.append(asset.rid)

    with (
        patch(f"{_MODULE}.AssetMigrator.copy_from", copy_asset),
        patch(f"{_MODULE}.WorkbookTemplateMigrator.clone", lambda _self, template: copied.append(template.rid)),
        patch(
            f"{_MODULE}.ChecklistMigrator.copy_from", lambda _self, checklist, _options: copied.append(checklist.rid)
        ),
        patch(f"{_MODULE}.DatasetMigrator.copy_from", lambda _self, dataset, _options: copied.append(dataset.rid)),
        patch.object(MigrationRunner, "migrate_deferred_workbooks"),
    ):
        with pytest.raises(RuntimeError, match=r"1 failure\(s\).*boom") as exc_info:
            _runner(tmp_path).run_migration()

    assert isinstance(exc_info.value.__cause__, ValueError)
    assert copied == ["asset-2", "template", "checklist", "dataset"]
