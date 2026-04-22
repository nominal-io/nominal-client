"""Parallel helpers for resource migration."""

from __future__ import annotations

import concurrent.futures
import logging

from nominal.experimental.migration.migration_runner import MigrationRunner
from nominal.experimental.migration.migrator.asset_migrator import AssetCopyOptions, AssetMigrator
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.migrator.workbook_template_migrator import WorkbookTemplateMigrator
from nominal.experimental.migration.parallel_migration_executor import (
    MigrationTask,
    run_concurrent,
    validate_max_workers,
)
from nominal.experimental.migration.parallel_migration_state import ThreadSafeMigrationState

logger = logging.getLogger(__name__)


def run_parallel_migration(runner: MigrationRunner, max_workers: int) -> None:
    """Run resource migration with a shared thread pool."""
    max_workers = validate_max_workers(max_workers)
    runner.migration_state = ThreadSafeMigrationState(rid_mapping=runner.migration_state.rid_mapping)

    ctx = MigrationContext(destination_client=runner.destination_client, migration_state=runner.migration_state)
    if getattr(runner, "destination_client_resolver", None) is not None:
        setattr(ctx, "destination_client_resolver", runner.destination_client_resolver)
    asset_migrator = AssetMigrator(ctx)
    template_migrator = WorkbookTemplateMigrator(ctx)
    asset_copy_options = AssetCopyOptions(
        dataset_config=runner.dataset_config,
        include_attachments=True,
        include_events=True,
        include_runs=True,
        include_video=True,
        include_checklists=True,
    )

    asset_tasks = [
        MigrationTask(
            rid=rid,
            label="asset",
            fn=lambda ar=asset_resources: asset_migrator.copy_from(ar.asset, asset_copy_options),
        )
        for rid, asset_resources in runner.migration_resources.source_assets.items()
    ]
    template_tasks = [
        MigrationTask(
            rid=template.rid,
            label="template",
            fn=lambda template=template: template_migrator.clone(template),
        )
        for template in runner.migration_resources.source_standalone_templates
    ]
    tasks = asset_tasks + template_tasks

    logger.info(
        "Running migration with %d worker(s) across %d asset(s) and %d template(s)",
        max_workers,
        len(asset_tasks),
        len(template_tasks),
    )
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            run_concurrent(executor, tasks)
    finally:
        runner.save_state()

    logger.info("Completed parallel migration")
