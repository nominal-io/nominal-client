from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, TypeVar

from nominal.experimental.migration.migrator.context import MigrationContext

SourceResource = TypeVar("SourceResource")
DestinationResource = TypeVar("DestinationResource")
CopyOptions = TypeVar("CopyOptions", bound="ResourceCopyOptions", default="ResourceCopyOptions")


@dataclass(frozen=True)
class ResourceCopyOptions:
    """Base type for resource-specific copy options."""


class Migrator(ABC, Generic[SourceResource, DestinationResource, CopyOptions]):
    """Abstract base class for migration operations.

    Subclasses may support only one of `clone` or `copy_from`. Unsupported
    operations should raise `NotImplementedError`.
    """

    def __init__(self, ctx: MigrationContext) -> None:
        """Constructs a Migrator with the given MigrationContext.

        Args:
            ctx: The MigrationContext to use for this migrator.
        """
        self.ctx = ctx

    @abstractmethod
    def clone(self, source: SourceResource) -> DestinationResource:
        raise NotImplementedError

    @abstractmethod
    def copy_from(self, source: SourceResource, options: CopyOptions) -> DestinationResource:
        raise NotImplementedError

    def record_mapping(self, resource_type: str, old_rid: str, new_rid: str) -> None:
        self.ctx.record_mapping(resource_type=resource_type, old_rid=old_rid, new_rid=new_rid)
