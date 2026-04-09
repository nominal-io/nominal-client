from __future__ import annotations

import logging
from typing import cast

from nominal.core._clientsbunch import ClientsBunch
from nominal.core.attachment import Attachment
from nominal.core.filetype import FileType, FileTypes
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.resource_type import ResourceType

logger = logging.getLogger(__name__)


class AttachmentMigrator(Migrator[Attachment, ResourceCopyOptions]):
    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.ATTACHMENT

    def default_copy_options(self) -> ResourceCopyOptions:
        return ResourceCopyOptions()

    def migrate_by_rid(self, source_clients: ClientsBunch, attachment_rid: str) -> Attachment:
        """Migrate an attachment identified by its RID.

        Checks migration state first to avoid re-fetching already-migrated attachments.
        This is a convenience for callers (e.g. workbook migrator) that discover attachment
        RIDs as strings rather than having pre-constructed Attachment objects.
        """
        mapped_rid = self.ctx.migration_state.get_mapped_rid(self.resource_type, attachment_rid)
        if mapped_rid is not None:
            logger.debug("Skipping %s (rid: %s): already in migration state", self.resource_label, attachment_rid)
            return self.ctx.destination_client.get_attachment(mapped_rid)
        raw = source_clients.attachment.get(source_clients.auth_header, attachment_rid)
        source_attachment = Attachment._from_conjure(source_clients, raw)
        return self.copy_from(source_attachment)

    def _copy_from_impl(self, source: Attachment, options: ResourceCopyOptions) -> Attachment:
        mapped_rid = self.ctx.migration_state.get_mapped_rid(self.resource_type, source.rid)
        if mapped_rid is not None:
            logger.debug("Skipping %s (rid: %s): already in migration state", self.resource_label, source.rid)
            return self.ctx.destination_client.get_attachment(mapped_rid)

        source_clients = cast(ClientsBunch, source._clients)
        raw = source_clients.attachment.get(source_clients.auth_header, source.rid)
        content = source_clients.attachment.get_content(source_clients.auth_header, source.rid)
        file_type = FileType("", raw.file_type) if raw.file_type else FileTypes.BINARY
        new_attachment = self.ctx.destination_client.create_attachment_from_io(
            content,
            raw.title,
            file_type,
            description=raw.description,
            properties=raw.properties,
            labels=raw.labels,
        )
        self.ctx.migration_state.record_mapping(self.resource_type, source.rid, new_attachment.rid)
        return new_attachment

    def _get_resource_name(self, resource: Attachment) -> str:
        return resource.name
