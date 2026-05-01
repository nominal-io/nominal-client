"""Typed Python mirrors of JSON payloads returned by gRPC-gateway-transcoded services.

Parsed at the HTTP boundary so downstream code works with typed fields instead of raw
dicts. Add new wire types here as more gRPC-transcoded services get Python clients.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class _ApiContainerImage:
    """Typed wire representation of a `nominal.registry.v1.ContainerImage` JSON payload."""

    rid: str
    name: str
    tag: str
    status: str
    created_at: str
    size_bytes: int | None

    @classmethod
    def _parse(cls, raw: Mapping[str, Any]) -> _ApiContainerImage:
        raw_size = raw.get("sizeBytes")
        return cls(
            rid=str(raw["rid"]),
            name=str(raw.get("name", "")),
            tag=str(raw.get("tag", "")),
            status=str(raw.get("status", "")),
            created_at=str(raw["createdAt"]),
            size_bytes=int(raw_size) if raw_size is not None else None,
        )
