"""Parallel helpers for resource migration."""

from __future__ import annotations

import concurrent.futures
import logging
from typing import Callable

from nominal.core.checklist import Checklist
from nominal.experimental.migration.config.migration_resources import AssetResources
from nominal.experimental.migration.migration_runner import MigrationRunner
from nominal.experimental.migration.migrator.asset_migrator import AssetCopyOptions, AssetMigrator
from nominal.experimental.migration.migrator.checklist_migrator import ChecklistCopyOptions, ChecklistMigrator
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.migrator.workbook_template_migrator import WorkbookTemplateMigrator
from nominal.experimental.migration.parallel_migration_executor import (
    MigrationTask,
    run_concurrent,
    validate_max_workers,
)
from nominal.experimental.migration.parallel_migration_state import ThreadSafeMigrationState

logger = logging.getLogger(__name__)


def _make_asset_fn(
    asset_resources: AssetResources, asset_migrator: AssetMigrator, asset_copy_options: AssetCopyOptions
) -> Callable[[], None]:
    def fn() -> None:
        asset_migrator.copy_from(asset_resources.asset, asset_copy_options)

    return fn


def _make_template_fn(template: object, template_migrator: WorkbookTemplateMigrator) -> Callable[[], None]:
    def fn() -> None:
        template_migrator.clone(template)  # type: ignore[arg-type]

    return fn


def _make_checklist_fn(checklist: Checklist, checklist_migrator: ChecklistMigrator) -> Callable[[], None]:
    def fn() -> None:
        # ChecklistMigrator.clone() raises NotImplementedError; use copy_from() to clone the definition.
        checklist_migrator.copy_from(checklist, ChecklistCopyOptions())

    return fn


def run_parallel_migration(runner: MigrationRunner, max_workers: int) -> None:
    """Run resource migration with a shared thread pool."""
    max_workers = validate_max_workers(max_workers)
    runner.migration_state = ThreadSafeMigrationState(rid_mapping=runner.migration_state.rid_mapping)

    ctx = MigrationContext(
        destination_client=runner.destination_client,
        migration_state=runner.migration_state,
        source_asset_rids=frozenset(runner.migration_resources.source_assets.keys()),
        dry_run=runner.dry_run,
    )
    if getattr(runner, "destination_client_resolver", None) is not None:
        setattr(ctx, "destination_client_resolver", runner.destination_client_resolver)
    asset_migrator = AssetMigrator(ctx)
    template_migrator = WorkbookTemplateMigrator(ctx)
    checklist_migrator = ChecklistMigrator(ctx)
    asset_tasks = [
        MigrationTask(
            rid=rid,
            label="asset",
            fn=_make_asset_fn(
                asset_resources,
                asset_migrator,
                AssetCopyOptions(
                    dataset_config=runner.dataset_config,
                    include_attachments=runner.asset_inclusion_config.include_attachments,
                    include_events=runner.asset_inclusion_config.include_events,
                    include_runs=runner.asset_inclusion_config.include_runs,
                    include_video=runner.asset_inclusion_config.include_video,
                    include_checklists=runner.asset_inclusion_config.include_checklists,
                    include_workbooks=runner.asset_inclusion_config.include_workbooks,
                    workbook_rids_allowlist=asset_resources.source_workbook_rids,
                ),
            ),
        )
        for rid, asset_resources in runner.migration_resources.source_assets.items()
    ]
    template_tasks = [
        MigrationTask(
            rid=template.rid,
            label="template",
            fn=_make_template_fn(template, template_migrator),
        )
        for template in runner.migration_resources.source_standalone_templates
    ]
    checklist_tasks = [
        MigrationTask(
            rid=checklist.rid,
            label="checklist",
            fn=_make_checklist_fn(checklist, checklist_migrator),
        )
        for checklist in runner.migration_resources.source_standalone_checklists
    ]
    tasks = asset_tasks + template_tasks + checklist_tasks

    logger.info(
        "Running migration with %d worker(s) across %d asset(s), %d template(s), and %d checklist(s)",
        max_workers,
        len(asset_tasks),
        len(template_tasks),
        len(checklist_tasks),
    )
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            run_concurrent(executor, tasks)
    finally:
        runner.save_state()

    logger.info("Completed parallel migration")
