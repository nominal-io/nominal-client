from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict

from nominal.core.asset import Asset
from nominal.core.run import Run
from nominal.core.workbook import Workbook
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.migrator.workbook_template_migrator import (
    WorkbookTemplateCopyOptions,
    WorkbookTemplateMigrator,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkbookCopyOptions(ResourceCopyOptions):
    destination_asset: Asset | None = None
    destination_run: Run | None = None


class WorkbookMigrator(Migrator[Workbook, Workbook, WorkbookCopyOptions]):
    def clone(self, source: Workbook) -> Workbook:
        raise NotImplementedError("Workbook clone is unsupported; use copy_from with destination asset/run.")

    def copy_from(self, source: Workbook, options: WorkbookCopyOptions) -> Workbook:
        if (options.destination_asset is None) == (options.destination_run is None):
            raise ValueError("Exactly one of destination_asset or destination_run must be provided.")

        log_extras = {
            "destination_client_workspace": self.ctx.destination_client.get_workspace(
                self.ctx.destination_client._clients.workspace_rid
            ).rid
        }
        logger.debug(
            "Copying workbook %s (rid: %s)",
            source.title,
            source.rid,
            extra=log_extras,
        )

        source_template = source._create_template_from_workbook()
        template_migrator = WorkbookTemplateMigrator(self.ctx)
        new_template = template_migrator.copy_from(
            source_template,
            WorkbookTemplateCopyOptions(include_content_and_layout=True),
        )
        if options.destination_asset is not None:
            new_workbook = new_template.create_workbook(
                asset=options.destination_asset,
                title=source.title,
                is_draft=source.is_draft(),
            )
        elif options.destination_run is not None:
            new_workbook = new_template.create_workbook(
                run=options.destination_run,
                title=source.title,
                is_draft=source.is_draft(),
            )

        new_template.archive()
        source_template.archive()
        logger.debug(
            "New workbook created: %s (rid: %s)",
            new_workbook.title,
            new_workbook.rid,
            extra=log_extras,
        )
        self.record_mapping("WORKBOOK", source.rid, new_workbook.rid)
        return new_workbook

    def copy_asset_and_run_workbooks(
        self,
        source_asset: Asset,
        destination_asset: Asset,
        run_mapping: Dict[str, str] | None = None,
    ) -> None:
        asset_workbooks = source_asset.search_workbooks(include_drafts=True)
        for workbook in asset_workbooks:
            if workbook.asset_rids and len(workbook.asset_rids) == 1:
                self.copy_from(workbook, WorkbookCopyOptions(destination_asset=destination_asset))

        if run_mapping:
            for source_run in source_asset.list_runs():
                if source_run.rid not in run_mapping:
                    logger.warning("Run %s not found in run mapping", source_run.rid)
                    continue
                destination_run = self.ctx.destination_client.get_run(run_mapping[source_run.rid])
                for workbook in source_run.search_workbooks(include_drafts=True):
                    if workbook.run_rids and len(workbook.run_rids) == 1:
                        self.copy_from(workbook, WorkbookCopyOptions(destination_run=destination_run))
