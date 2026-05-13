from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Protocol

from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams, RegistryService
from nominal.core._utils.api_tools import HasRid
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

if TYPE_CHECKING:
    from nominal_api_protos.nominal.registry.v1 import registry_pb2


class ContainerImageStatus(Enum):
    UNSPECIFIED = "CONTAINER_IMAGE_STATUS_UNSPECIFIED"
    PENDING = "CONTAINER_IMAGE_STATUS_PENDING"
    READY = "CONTAINER_IMAGE_STATUS_READY"
    FAILED = "CONTAINER_IMAGE_STATUS_FAILED"

    @classmethod
    def _from_proto(cls, proto_status: int) -> ContainerImageStatus:
        from nominal_api_protos.nominal.registry.v1 import registry_pb2

        try:
            return cls(registry_pb2.ContainerImageStatus.Name(proto_status))
        except (ValueError, KeyError):
            return cls.UNSPECIFIED


@dataclass(frozen=True)
class ContainerImage(HasRid):
    rid: str
    name: str
    tag: str
    status: ContainerImageStatus
    created_at: IntegralNanosecondsUTC
    size_bytes: int | None
    workspace_rid: str
    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def registry(self) -> RegistryService: ...

    def delete(self) -> None:
        """Delete this container image. Extractors referencing it will fail on subsequent ingests."""
        self._clients.registry.delete_image(self._clients.auth_header, self.rid, workspace_rid=self.workspace_rid)

    @classmethod
    def _from_proto(cls, clients: _Clients, image: registry_pb2.ContainerImage, workspace_rid: str) -> Self:
        created_at_ns = image.created_at.seconds * 1_000_000_000 + image.created_at.nanos
        return cls(
            rid=image.rid,
            name=image.name,
            tag=image.tag,
            status=ContainerImageStatus._from_proto(image.status),
            created_at=_SecondsNanos.from_flexible(created_at_ns).to_nanoseconds(),
            size_bytes=image.size_bytes if image.HasField("size_bytes") else None,
            workspace_rid=workspace_rid,
            _clients=clients,
        )
