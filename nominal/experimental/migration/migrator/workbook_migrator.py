from __future__ import annotations

import logging
from dataclasses import dataclass

from nominal.core.asset import Asset
from nominal.core.run import Run
from nominal.core.workbook import Workbook
from nominal.core.workbook_template import WorkbookTemplate
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.migrator.workbook_template_migrator import (
    WorkbookTemplateCopyOptions,
    WorkbookTemplateMigrator,
)
from nominal.experimental.migration.resource_type import ResourceType

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkbookCopyOptions(ResourceCopyOptions):
    destination_asset: Asset | None = None
    destination_run: Run | None = None


class WorkbookMigrator(Migrator[Workbook, WorkbookCopyOptions]):
    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.WORKBOOK

    def clone(self, source: Workbook) -> Workbook:
        raise NotImplementedError("Workbook clone is unsupported; use copy_from with destination asset/run.")

    def default_copy_options(self) -> WorkbookCopyOptions | None:
        return None

    def _copy_from_impl(self, source: Workbook, options: WorkbookCopyOptions) -> Workbook:
        """This method copies content from an old workbook to a new workbook by use of templates, in order to
        modify hardcoded variables in workbook content. We do this by creating a template in the source
        client, copying the template to the destination client, creating a new workbook from the template in the
        destination client, and then archiving the template in both clients.
        """
        mapped_rid = self.ctx.migration_state.get_mapped_rid(self.resource_type, source.rid)
        if mapped_rid is not None:
            logger.debug("Skipping %s (rid: %s): already in migration state", self.resource_label, source.rid)
            return self.ctx.destination_client.get_workbook(mapped_rid)

        if (options.destination_asset is None) == (options.destination_run is None):
            raise ValueError("Exactly one of destination_asset or destination_run must be provided.")

        source_template = source._create_template_from_workbook()
        template_migrator = WorkbookTemplateMigrator(self.ctx)
        new_template = template_migrator.copy_from(
            source_template,
            WorkbookTemplateCopyOptions(include_content_and_layout=True),
        )
        new_workbook = self._create_destination_workbook(source, new_template, options)
        self.ctx.migration_state.record_mapping(self.resource_type, source.rid, new_workbook.rid)

        new_template.archive()
        source_template.archive()
        return new_workbook

    def _create_destination_workbook(
        self,
        source: Workbook,
        new_template: WorkbookTemplate,
        options: WorkbookCopyOptions,
    ) -> Workbook:
        if options.destination_asset is not None:
            return new_template.create_workbook(
                asset=options.destination_asset,
                title=source.title,
                is_draft=source.is_draft(),
            )

        destination_run = options.destination_run
        if destination_run is None:
            raise ValueError("Exactly one of destination_asset or destination_run must be provided.")

        return new_template.create_workbook(
            run=destination_run,
            title=source.title,
            is_draft=source.is_draft(),
        )

    def _get_resource_name(self, resource: Workbook) -> str:
        return resource.title
