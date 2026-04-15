from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Mapping, Sequence

from nominal.core import NominalClient
from nominal.core._utils.api_tools import Link, LinkDict
from nominal.core.asset import Asset
from nominal.core.attachment import Attachment
from nominal.core.run import Run
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
    new_properties: Mapping[str, str] | None = None
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
            return existing_run

        destination_client = self.destination_client_for(source)

        attachments = options.new_attachments
        if attachments is None:
            attachment_migrator = AttachmentMigrator(self.ctx)
            attachments = [attachment_migrator.copy_from(a) for a in source.list_attachments()]

        new_run = destination_client.create_run(
            name=options.new_name if options.new_name is not None else source.name,
            start=options.new_start if options.new_start is not None else source.start,
            end=options.new_end if options.new_end is not None else source.end,
            description=options.new_description if options.new_description is not None else source.description,
            properties=options.new_properties if options.new_properties is not None else source.properties,
            labels=options.new_labels if options.new_labels is not None else source.labels,
            assets=options.new_assets if options.new_assets is not None else source.assets,
            links=options.new_links if options.new_links is not None else source.links,
            attachments=attachments,
        )
        self.ctx.migration_state.record_mapping(self.resource_type, source.rid, new_run.rid)
        return new_run

    def _get_resource_name(self, resource: Run) -> str:
        return resource.name
