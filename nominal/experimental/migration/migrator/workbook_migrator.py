from __future__ import annotations

from dataclasses import dataclass

from nominal.core.asset import Asset
from nominal.core.run import Run
from nominal.core.workbook import Workbook
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.migrator.workbook_template_migrator import (
    WorkbookTemplateCopyOptions,
    WorkbookTemplateMigrator,
)
from nominal.experimental.migration.resource_type import ResourceType


@dataclass(frozen=True)
class WorkbookCopyOptions(ResourceCopyOptions):
    destination_asset: Asset | None = None
    destination_run: Run | None = None


class WorkbookMigrator(Migrator[Workbook, WorkbookCopyOptions]):
    resource_type = ResourceType.WORKBOOK

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
        if (options.destination_asset is None) == (options.destination_run is None):
            raise ValueError("Exactly one of destination_asset or destination_run must be provided.")

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
        else:
            raise ValueError("Exactly one of destination_asset or destination_run must be provided.")

        new_template.archive()
        source_template.archive()
        return new_workbook

    def _get_resource_name(self, resource: Workbook) -> str:
        return resource.title

    def _get_resource_rid(self, resource: Workbook) -> str:
        return resource.rid
