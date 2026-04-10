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
from nominal.experimental.migration.resource_type import ResourceType

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
    new_is_published: bool | None = None


class ChecklistMigrator(Migrator[Checklist, ChecklistCopyOptions]):
    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.CHECKLIST

    def clone(self, source: Checklist) -> Checklist:
        raise NotImplementedError("Checklist does not support clone(); use copy_from().")

    def default_copy_options(self) -> ChecklistCopyOptions:
        return ChecklistCopyOptions()

    def _copy_from_impl(self, source: Checklist, options: ChecklistCopyOptions) -> Checklist:
        destination_client = self.ctx.destination_client_for(source)
        mapped_rid = self.ctx.migration_state.get_mapped_rid(self.resource_type, source.rid)
        if mapped_rid is not None:
            logger.debug("Skipping %s (rid: %s): already in migration state", self.resource_label, source.rid)
            return destination_client.get_checklist(mapped_rid)

        api_source_checklist = source._get_latest_api()
        commit_message = (
            options.new_commit_message
            if options.new_commit_message is not None
            else api_source_checklist.commit.message
        )
        title = options.new_title if options.new_title is not None else source.name
        description = options.new_description if options.new_description is not None else source.description
        checks = (
            options.new_checks
            if options.new_checks is not None
            else _to_create_checklist_entries(api_source_checklist.checks)
        )
        properties = (
            options.new_properties if options.new_properties is not None else api_source_checklist.metadata.properties
        )
        labels = options.new_labels if options.new_labels is not None else api_source_checklist.metadata.labels
        checklist_variables = (
            options.new_checklist_variables
            if options.new_checklist_variables is not None
            else _to_unresolved_checklist_variables(api_source_checklist.checklist_variables)
        )
        is_published = (
            options.new_is_published
            if options.new_is_published is not None
            else api_source_checklist.metadata.is_published
        )
        workspace_rid = destination_client.get_workspace(destination_client._clients.workspace_rid).rid

        new_checklist = _create_checklist_with_content(
            client=destination_client,
            commit_message=commit_message,
            title=title,
            description=description,
            checks=checks,
            properties=properties,
            labels=labels,
            checklist_variables=checklist_variables,
            is_published=is_published,
            workspace=workspace_rid,
        )
        self.ctx.migration_state.record_mapping(self.resource_type, source.rid, new_checklist.rid)
        return new_checklist

    def _get_resource_name(self, resource: Checklist) -> str:
        return resource.name
