from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, TypeVar

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

    def copy_from(self, source: Resource, options: CopyOptions | None = None) -> Resource:
        resolved_options = self.default_copy_options() if options is None else options
        if resolved_options is None:
            raise NotImplementedError(f"{type(self).__name__} requires explicit copy options.")
        source_rid = source.rid

        logger = logging.getLogger(type(self).__module__)
        log_extras = {
            "destination_client_workspace": self.ctx.destination_client.get_workspace(
                self.ctx.destination_client._clients.workspace_rid
            ).rid
        }
        logger.debug(
            "Copying %s %s (rid: %s)",
            self.resource_label,
            self._get_resource_name(source),
            source_rid,
            extra=log_extras,
        )
        result = self._copy_from_impl(source, resolved_options)
        result_rid = result.rid
        logger.debug(
            "New %s created: %s (rid: %s)",
            self.resource_label,
            self._get_resource_name(result),
            result_rid,
            extra=log_extras,
        )
        self.record_mapping(self.resource_type, source_rid, result_rid)
        return result

    def record_mapping(self, resource_type: ResourceType, old_rid: str, new_rid: str) -> None:
        self.ctx.record_mapping(resource_type=resource_type, old_rid=old_rid, new_rid=new_rid)

    @property
    def resource_label(self) -> str:
        return self.resource_type.value.lower().replace("_", " ")

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
    def _get_resource_name(self, resource: Resource) -> str:
        """Gets the name of the given resource. Used for logging purposes.

        Args:
            resource: The resource to get the name of.
        """
