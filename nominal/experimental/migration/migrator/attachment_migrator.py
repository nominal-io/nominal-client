from __future__ import annotations

import logging
from functools import partial
from typing import Iterable

from nominal.core import NominalClient
from nominal.core._clientsbunch import ClientsBunch
from nominal.core.attachment import Attachment, _get_attachment
from nominal.core.filetype import FileType, FileTypes
from nominal.experimental.migration.dry_run import would_create_message
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.resource_type import ResourceType
from nominal.experimental.migration.utils.retry_utils import retry_transient

logger = logging.getLogger(__name__)


class AttachmentMigrator(Migrator[Attachment, ResourceCopyOptions]):
    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.ATTACHMENT

    def default_copy_options(self) -> ResourceCopyOptions:
        return ResourceCopyOptions()

    def migrate_by_rid(self, source_clients: ClientsBunch, attachment_rid: str) -> Attachment:
        """Migrate an attachment identified by its RID.

        This is a convenience for callers (e.g. workbook migrator) that discover
        attachment RIDs as strings rather than having pre-constructed Attachment
        objects. We still materialize the source attachment so resolver-based
        destination routing stays consistent for already-mapped attachments.
        """
        source_attachment = Attachment._from_proto(source_clients, _get_attachment(source_clients, attachment_rid))
        existing_attachment = self.get_existing_destination_resource(source_attachment)
        if existing_attachment is not None:
            return existing_attachment
        return self.copy_from(source_attachment)

    def copy_attachments(self, source_attachments: Iterable[Attachment], owner: str) -> list[Attachment]:
        """Copy the non-archived attachments of an asset or run, returning the ones that copied.

        One bad attachment must not abort the owner's migration and every sibling resource
        behind it. Transient failures are retried in place; an attachment that still fails is
        left unmapped and recorded as a skip for the end-of-run summary, so a rerun re-attempts it.
        ``owner`` names the owning resource, such as ``"asset <rid>"``, in log messages and in the
        skip entry, so an attachment shared by several owners is tracked for each one.
        """
        copied: list[Attachment] = []
        for source_attachment in source_attachments:
            if source_attachment.is_archived:
                logger.info(
                    "Skipping archived attachment '%s' (rid: %s) on %s",
                    source_attachment.name,
                    source_attachment.rid,
                    owner,
                )
                continue
            skip_key = f"{source_attachment.rid} on {owner}"
            # Retry the whole copy: the source streams into the upload, so a dropped connection restarts it.
            # A create can commit and still raise, leaving an unlinked duplicate; the API has no idempotency key.
            try:
                copied.append(
                    retry_transient(
                        partial(self.copy_from, source_attachment),
                        description=f"copy of attachment {source_attachment.rid}",
                    )
                )
            except Exception as error:
                logger.exception("Failed to copy attachment (rid: %s) on %s", source_attachment.rid, owner)
                self.ctx.migration_state.set_skip(ResourceType.ATTACHMENT, skip_key, f"copy failed: {error}")
            else:
                # Cleared only here: no other copy of this attachment links it to this owner.
                self.ctx.migration_state.set_skip(ResourceType.ATTACHMENT, skip_key, None)
        return copied

    def _get_existing_destination_resource(self, destination_client: NominalClient, mapped_rid: str) -> Attachment:
        return destination_client.get_attachment(mapped_rid)

    def _copy_from_impl(self, source: Attachment, options: ResourceCopyOptions) -> Attachment:
        existing_attachment = self.get_existing_destination_resource(source)
        if existing_attachment is not None:
            return existing_attachment

        if self.ctx.dry_run:
            logger.info(would_create_message(self.resource_type), source.name, source.rid)
            self.ctx.migration_state.record_mapping(self.resource_type, source.rid, source.rid)
            return source

        destination_client = self.destination_client_for(source)
        raw = _get_attachment(source._clients, source.rid)
        file_type = FileType("", raw.file_type) if raw.file_type else FileTypes.BINARY
        with source.get_contents() as content:
            new_attachment = destination_client.create_attachment_from_io(
                content,
                raw.title,
                file_type,
                description=raw.description,
                properties=raw.properties,
                labels=self.ctx.destination_labels(raw.labels),
            )
        self.ctx.migration_state.record_mapping(self.resource_type, source.rid, new_attachment.rid)
        return new_attachment

    def _get_resource_name(self, resource: Attachment) -> str:
        return resource.name
