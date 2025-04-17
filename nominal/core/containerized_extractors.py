from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RegistryAuth:
    """Authentication credentials for private Docker registries."""

    username: str
    password_secret_rid: str


@dataclass(frozen=True)
class FileExtractionInput:
    """Configuration for a file extraction input in a containerized extractor."""

    name: str
    description: str
    environment_variable: str
    regex: str
