from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Sequence

from nominal.core import NominalClient
from nominal.core._utils.api_tools import Link, LinkDict, rid_from_instance_or_string
from nominal.core._utils.api_types import NominalProperties
from nominal.core.asset import Asset
from nominal.core.attachment import Attachment
from nominal.core.run import Run
from nominal.experimental.migration.dry_run import DRY_RUN_PREFIX, would_create_message
from nominal.experimental.migration.migrator.attachment_migrator import AttachmentMigrator
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.resource_type import ResourceType
from nominal.ts import IntegralNanosecondsUTC

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunCopyOptions(ResourceCopyOptions):
    new_name: str | None = None
    new_start: datetime | IntegralNanosecondsUTC | None = None
    new_end: datetime | IntegralNanosecondsUTC | None = None
    new_description: str | None = None
    new_properties: NominalProperties | None = None
    new_labels: Sequence[str] | None = None
    new_links: Sequence[str | Link | LinkDict] | None = None
    new_attachments: Iterable[Attachment] | Iterable[str] | None = None
    new_assets: Sequence[Asset | str] | None = None


class RunMigrator(Migrator[Run, RunCopyOptions]):
    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.RUN

    def default_copy_options(self) -> RunCopyOptions:
        return RunCopyOptions()

    def _get_existing_destination_resource(self, destination_client: NominalClient, mapped_rid: str) -> Run:
        return destination_client.get_run(mapped_rid)

    def _copy_from_impl(self, source: Run, options: RunCopyOptions) -> Run:
        existing_run = self.get_existing_destination_resource(source)
        if existing_run is not None:
            # In a dry run, existing_run is the source run itself, so adding attachments to it
            # would write to the source.
            if options.new_attachments is None and not self.ctx.dry_run:
                self._add_missing_attachments(source, existing_run)
            if options.new_assets is not None:
                return self._ensure_assets_added(existing_run, options.new_assets)
            return existing_run

        destination_client = self.destination_client_for(source)

        attachments = options.new_attachments
        if attachments is None:
            attachments = AttachmentMigrator(self.ctx).copy_attachments(source.list_attachments(), f"run {source.rid}")

        if self.ctx.dry_run:
            logger.info(would_create_message(self.resource_type), source.name, source.rid)
            self.ctx.migration_state.record_mapping(self.resource_type, source.rid, source.rid)
            return source

        new_run = destination_client.create_run(
            name=options.new_name if options.new_name is not None else source.name,
            start=options.new_start if options.new_start is not None else source.start,
            end=options.new_end if options.new_end is not None else source.end,
            description=options.new_description if options.new_description is not None else source.description,
            properties=options.new_properties if options.new_properties is not None else source.properties,
            labels=self.ctx.destination_labels(source.labels, override=options.new_labels),
            assets=options.new_assets if options.new_assets is not None else source.assets,
            links=options.new_links if options.new_links is not None else source.links,
            attachments=attachments,
        )
        self.ctx.migration_state.record_mapping(self.resource_type, source.rid, new_run.rid)
        return new_run

    def _add_missing_attachments(self, source: Run, existing_run: Run) -> None:
        """Copy and attach source attachments the destination run lacks.

        A run is created even when some of its attachments fail to copy, so a rerun has to
        retry those here rather than treat the already-migrated run as complete.
        """
        source_attachments = source.list_attachments()
        if not source_attachments:
            return
        attached = set(existing_run._get_latest_api().attachments)
        missing = [
            a
            for a in source_attachments
            if self.ctx.migration_state.get_mapped_rid(ResourceType.ATTACHMENT, a.rid) not in attached
        ]
        new_attachments = AttachmentMigrator(self.ctx).copy_attachments(missing, f"run {source.rid}")
        if new_attachments:
            existing_run.add_attachments(new_attachments)

    def _ensure_assets_added(self, run: Run, new_assets: Sequence[Asset | str]) -> Run:
        """Add any assets from new_assets that are not already on run. Returns the (possibly updated) run.

        Raises:
            ValueError: if new_assets is empty, since runs must always have at least one asset.
        """
        if not new_assets:
            raise ValueError("new_assets must be non-empty; runs must have at least one asset.")
        existing_rids = set(run.assets)
        missing_rids = [
            rid_from_instance_or_string(a) for a in new_assets if rid_from_instance_or_string(a) not in existing_rids
        ]
        if not missing_rids:
            return run
        if self.ctx.dry_run:
            logger.info(f"{DRY_RUN_PREFIX} Would add %d asset(s) to existing run %s", len(missing_rids), run.rid)
            return run
        logger.debug("Adding %d missing asset(s) to existing run %s", len(missing_rids), run.rid)
        return run.update(assets=[*existing_rids, *missing_rids])

    def _get_resource_name(self, resource: Run) -> str:
        return resource.name
