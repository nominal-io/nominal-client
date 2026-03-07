from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Mapping, Sequence

from nominal.core._utils.api_tools import Link, LinkDict
from nominal.core.asset import Asset
from nominal.core.attachment import Attachment
from nominal.core.run import Run
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.ts import IntegralNanosecondsUTC

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunCopyOptions(ResourceCopyOptions):
    new_name: str | None = None
    new_start: datetime | IntegralNanosecondsUTC | None = None
    new_end: datetime | IntegralNanosecondsUTC | None = None
    new_description: str | None = None
    new_properties: Mapping[str, str] | None = None
    new_labels: Sequence[str] = ()
    new_links: Sequence[str | Link | LinkDict] = ()
    new_attachments: Iterable[Attachment] | Iterable[str] = ()
    new_assets: Sequence[Asset | str] = ()


class RunMigrator(Migrator[Run, Run, RunCopyOptions]):
    def clone(self, source: Run) -> Run:
        return self.copy_from(source, RunCopyOptions())

    def copy_from(self, source: Run, options: RunCopyOptions) -> Run:
        log_extras = {
            "destination_client_workspace": self.ctx.destination_client.get_workspace(
                self.ctx.destination_client._clients.workspace_rid
            ).rid
        }
        logger.debug(
            "Copying run %s (rid: %s)",
            source.name,
            source.rid,
            extra=log_extras,
        )

        result = self.ctx.destination_client.create_run(
            name=options.new_name or source.name,
            start=options.new_start or source.start,
            end=options.new_end or source.end,
            description=options.new_description or source.description,
            properties=options.new_properties or source.properties,
            labels=options.new_labels or source.labels,
            assets=options.new_assets or source.assets,
            links=options.new_links or source.links,
            attachments=options.new_attachments or source.list_attachments(),
        )
        logger.debug("New run created: %s (rid: %s)", result.name, result.rid, extra=log_extras)
        self.record_mapping("RUN", source.rid, result.rid)
        return result
