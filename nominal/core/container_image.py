from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams, RegistryService
from nominal.core._utils.api_tools import HasRid
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos


class ContainerImageStatus(str, Enum):
    UNSPECIFIED = "CONTAINER_IMAGE_STATUS_UNSPECIFIED"
    PENDING = "CONTAINER_IMAGE_STATUS_PENDING"
    READY = "CONTAINER_IMAGE_STATUS_READY"
    FAILED = "CONTAINER_IMAGE_STATUS_FAILED"

    @classmethod
    def _parse(cls, raw: object) -> ContainerImageStatus:
        if isinstance(raw, str):
            try:
                return cls(raw)
            except ValueError:
                pass
        return cls.UNSPECIFIED


@dataclass(frozen=True)
class ContainerImage(HasRid):
    rid: str
    name: str
    tag: str
    status: ContainerImageStatus
    created_at: IntegralNanosecondsUTC
    size_bytes: int | None

    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def registry(self) -> RegistryService: ...

    @classmethod
    def _from_response(cls, clients: _Clients, image: dict[str, Any]) -> Self:
        raw_size = image.get("sizeBytes")
        size_bytes = int(raw_size) if raw_size is not None else None
        return cls(
            rid=str(image["rid"]),
            name=str(image.get("name", "")),
            tag=str(image.get("tag", "")),
            status=ContainerImageStatus._parse(image.get("status")),
            created_at=_SecondsNanos.from_flexible(image["createdAt"]).to_nanoseconds(),
            size_bytes=size_bytes,
            _clients=clients,
        )
