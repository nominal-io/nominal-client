from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol

from typing_extensions import Self

from nominal.core._api_types import _ApiContainerImage
from nominal.core._clientsbunch import HasScoutParams, RegistryService
from nominal.core._utils.api_tools import HasRid
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


class ContainerImageStatus(Enum):
    """Lifecycle state of a container image in Nominal's registry."""

    UNSPECIFIED = "CONTAINER_IMAGE_STATUS_UNSPECIFIED"
    """Status is unset or unrecognized. Treat as an unknown state."""

    PENDING = "CONTAINER_IMAGE_STATUS_PENDING"
    """Tarball uploaded but is not ready for use yet."""

    READY = "CONTAINER_IMAGE_STATUS_READY"
    """Image is available in the registry and can be pulled."""

    FAILED = "CONTAINER_IMAGE_STATUS_FAILED"
    """Registry push failed. The image will not become available."""

    @classmethod
    def _from_grpc(cls, api_status: str) -> ContainerImageStatus:
        try:
            return cls(api_status)
        except ValueError:
            return cls.UNSPECIFIED


@dataclass(frozen=True)
class ContainerImage(HasRid):
    """A container image tarball stored in Nominal's registry.

    Create one via `NominalClient.upload_container_image_from_io`. The registry push is
    asynchronous: a freshly uploaded image may be returned in `PENDING` state and transition
    to `READY` (or `FAILED`) once the server finishes pushing the tarball to the internal
    OCI registry.
    """

    rid: str
    """Nominal resource identifier for this image."""

    name: str
    """Image name within the workspace (e.g. `my-extractor`)."""

    tag: str
    """Image tag (e.g. `v1.2.3`). Unique per `(workspace, name)`."""

    status: ContainerImageStatus
    """Current lifecycle state of the image."""

    created_at: IntegralNanosecondsUTC
    """Creation timestamp, in nanoseconds since the Unix epoch."""

    size_bytes: int | None
    """Size of the uploaded tarball in bytes, or `None` until the server populates it."""

    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def registry(self) -> RegistryService: ...

    def delete(self) -> None:
        """Delete this container image from Nominal's registry.

        Note: extractors that reference this image's RID will fail to pull on subsequent ingests.
        """
        self._clients.registry.delete_image(self._clients.auth_header, self.rid)

    @classmethod
    def _from_grpc(cls, clients: _Clients, image: _ApiContainerImage) -> Self:
        return cls(
            rid=image.rid,
            name=image.name,
            tag=image.tag,
            status=ContainerImageStatus._from_grpc(image.status),
            created_at=_SecondsNanos.from_flexible(image.created_at).to_nanoseconds(),
            size_bytes=image.size_bytes,
            _clients=clients,
        )
