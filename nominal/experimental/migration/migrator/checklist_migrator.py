from __future__ import annotations

import logging
from dataclasses import dataclass

from nominal_api import scout_checks_api

from nominal.core.checklist import Checklist
from nominal.experimental.checklist_utils.checklist_utils import (
    _create_checklist_with_content,
    _to_create_checklist_entries,
    _to_unresolved_checklist_variables,
)
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChecklistCopyOptions(ResourceCopyOptions):
    new_title: str | None = None
    new_commit_message: str | None = None
    new_assignee_rid: str | None = None
    new_description: str | None = None
    new_checks: list[scout_checks_api.CreateChecklistEntryRequest] | None = None
    new_properties: dict[str, str] | None = None
    new_labels: list[str] | None = None
    new_checklist_variables: list[scout_checks_api.UnresolvedChecklistVariable] | None = None
    new_is_published: bool | None = False


class ChecklistMigrator(Migrator[Checklist, Checklist, ChecklistCopyOptions]):
    def clone(self, source: Checklist) -> Checklist:
        raise NotImplementedError("Checklist does not support clone(); use copy_from().")

    def copy_from(self, source: Checklist, options: ChecklistCopyOptions) -> Checklist:
        log_extras = {"destination_client_workspace": self.ctx.destination_client._clients.workspace_rid}
        logger.debug("Copying checklist: %s", source.name, extra=log_extras)

        api_source_checklist = source._get_latest_api()

        new_checklist = _create_checklist_with_content(
            client=self.ctx.destination_client,
            commit_message=options.new_commit_message or api_source_checklist.commit.message,
            title=options.new_title or source.name,
            description=options.new_description or source.description,
            checks=options.new_checks or _to_create_checklist_entries(api_source_checklist.checks),
            properties=options.new_properties or api_source_checklist.metadata.properties,
            labels=options.new_labels or api_source_checklist.metadata.labels,
            checklist_variables=options.new_checklist_variables
            or _to_unresolved_checklist_variables(api_source_checklist.checklist_variables),
            is_published=options.new_is_published or api_source_checklist.metadata.is_published,
            workspace=self.ctx.destination_client.get_workspace(self.ctx.destination_client._clients.workspace_rid).rid,
        )

        logger.debug(
            "New checklist created %s: (rid: %s)",
            new_checklist.name,
            new_checklist.rid,
        )

        self.record_mapping("CHECKLIST", source.rid, new_checklist.rid)
        return new_checklist
