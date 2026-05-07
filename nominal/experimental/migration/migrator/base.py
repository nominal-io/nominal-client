from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, TypeVar

from nominal.core import NominalClient
from nominal.core._utils.api_tools import HasRid
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.resource_type import ResourceType

Resource = TypeVar("Resource", bound=HasRid)
CopyOptions = TypeVar("CopyOptions", bound="ResourceCopyOptions", default="ResourceCopyOptions")


@dataclass(frozen=True)
class ResourceCopyOptions:
    """Base type for resource-specific copy options."""


class Migrator(ABC, Generic[Resource, CopyOptions]):
    """Abstract base class for migration operations.

    Subclasses may support only one of `clone` or `copy_from`. Unsupported
    operations should raise `NotImplementedError`.
    """

    @property
    @abstractmethod
    def resource_type(self) -> ResourceType:
        """The resource type handled by this migrator."""

    def __init__(self, ctx: MigrationContext) -> None:
        """Constructs a Migrator with the given MigrationContext.

        Args:
            ctx: The MigrationContext to use for this migrator.
        """
        self.ctx = ctx

    def clone(self, source: Resource) -> Resource:
        return self.copy_from(source)

    def destination_client_for(self, source: Resource) -> NominalClient:
        return self.ctx.destination_client_for(source)

    def get_existing_destination_resource(self, source: Resource) -> Resource | None:
        mapped_rid = self.ctx.migration_state.get_mapped_rid(self.resource_type, source.rid)
        if mapped_rid is None:
            return None

        logger = logging.getLogger(type(self).__module__)
        logger.debug("Skipping %s (rid: %s): already in migration state", self.resource_label, source.rid)
        return self._get_existing_destination_resource(self.destination_client_for(source), mapped_rid)

    def copy_from(self, source: Resource, options: CopyOptions | None = None) -> Resource:
        resolved_options = self.default_copy_options() if options is None else options
        if resolved_options is None:
            raise NotImplementedError(f"{type(self).__name__} requires explicit copy options.")
        source_rid = source.rid
        destination_client = self.destination_client_for(source)

        logger = logging.getLogger(type(self).__module__)
        log_extras = {
            "destination_client_workspace": destination_client.get_workspace(
                destination_client._clients.workspace_rid
            ).rid
        }

        if self.use_singleflight():
            return self.ctx.run_singleflight(
                source_resource=source,
                source_rid=source_rid,
                fn=lambda: self._copy_from(source, resolved_options, source_rid, logger, log_extras),
            )

        return self._copy_from(source, resolved_options, source_rid, logger, log_extras)

    def record_mapping(self, resource_type: ResourceType, old_rid: str, new_rid: str) -> None:
        self.ctx.record_mapping(resource_type=resource_type, old_rid=old_rid, new_rid=new_rid)

    @property
    def resource_label(self) -> str:
        return self.resource_type.value.lower().replace("_", " ")

    def use_singleflight(self) -> bool:
        """Whether concurrent callers should be deduped for a given source RID."""
        return True

    @abstractmethod
    def default_copy_options(self) -> CopyOptions | None:
        """Returns the default copy options to use when copying a resource of this type.

        If None is returned, copy_from requires explicit options to be provided.
        """

    @abstractmethod
    def _copy_from_impl(self, source: Resource, options: CopyOptions) -> Resource:
        """Copies the given resource with the specified options.

        Args:
            source (Resource): The resource to copy.
            options (CopyOptions): The options to use for the copy.

        Returns:
            Resource: The new resource.
        """

    @abstractmethod
    def _get_existing_destination_resource(self, destination_client: NominalClient, mapped_rid: str) -> Resource:
        """Fetches an already-migrated resource from the destination client."""

    @abstractmethod
    def _get_resource_name(self, resource: Resource) -> str:
        """Gets the name of the given resource. Used for logging purposes.

        Args:
            resource: The resource to get the name of.
        """

    def _copy_from(
        self,
        source: Resource,
        resolved_options: CopyOptions,
        source_rid: str,
        logger: logging.Logger,
        log_extras: dict[str, str],
    ) -> Resource:
        logger.debug(
            "Copying %s %s (rid: %s)",
            self.resource_label,
            self._get_resource_name(source),
            source_rid,
            extra=log_extras,
        )
        already_mapped = self.ctx.migration_state.get_mapped_rid(self.resource_type, source_rid) is not None
        result = self._copy_from_impl(source, resolved_options)
        result_rid = result.rid
        logger.debug(
            "Found %s: %s (rid: %s)" if already_mapped else "New %s created: %s (rid: %s)",
            self.resource_label,
            self._get_resource_name(result),
            result_rid,
            extra=log_extras,
        )
        # Safety net: each _copy_from_impl should already call record_mapping immediately after
        # creating the resource (so a crash mid-migration doesn't cause duplicates on resume).
        # This call is always idempotent - it writes the same old->new mapping that _copy_from_impl
        # already wrote, so there is no risk of overwriting with a different value.
        self.record_mapping(self.resource_type, source_rid, result_rid)
        return result
