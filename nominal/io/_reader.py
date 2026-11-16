"""The `Reader` interface and the `ReaderSpec` that describes a format without importing its code."""

from __future__ import annotations

import importlib
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from os import PathLike
from typing import Any

from nominal.io.schema import Chunk


class MissingDependencyError(ImportError):
    """A format's reader needs a package that isn't installed."""


class Reader(ABC):
    """Reads one file format into `~nominal.io.schema.Chunk` objects: columns of numpy arrays.

    Subclass it, then describe it with a `ReaderSpec`. Keep heavy imports inside the methods or the
    module the spec's ``factory`` points at, never in the module that holds the spec.
    """

    def sniff(self, path: str | PathLike[str]) -> bool | None:
        """Cheaply say whether ``path`` is this format, for when several formats share an extension.

        Look only at the file's first bytes or header, never the whole file, and don't raise.

        Returns:
            ``True`` if it is this format, ``False`` if it isn't, ``None`` if this reader can't tell.
        """
        return None

    @abstractmethod
    def read_chunks(self, path: str | PathLike[str], **options: Any) -> Iterator[Chunk]:
        """Yield the file as chunks, built with `~nominal.io.schema.ChunkBuilder`.

        Chunks needn't be sorted, in themselves or relative to each other; `nominal.io.read` sorts
        the result. A format that can't be read piecemeal yields one chunk. Don't import pandas or
        polars here: readers don't depend on a frame library.
        """


@dataclass(frozen=True)
class ReaderSpec:
    """What the registry knows about a format before loading its reader.

    Args:
        name: The format's name, as passed to ``read(path, format=name)``; unique across all formats.
        extensions: File suffixes it reads, lowercase with the dot, e.g. ``(".sigmf-meta", ".sigmf")``.
        factory: The `Reader` class, or ``"package.module:ClassName"`` to import it on first use.
        description: One line for `nominal.io.list_formats`.
        install_hint: What to run if the reader's dependencies are missing, e.g.
            ``"pip install nominal[wfdb]"``.
    """

    name: str
    extensions: tuple[str, ...]
    factory: str | Callable[[], Reader]
    description: str = ""
    install_hint: str | None = None

    def __post_init__(self) -> None:
        bad = [e for e in self.extensions if not e.startswith(".") or e != e.lower()]
        if not self.name or not self.extensions or bad:
            raise ValueError(f"ReaderSpec {self.name!r} needs a name and lowercase extensions like '.csv'; got {bad}")

    def load(self) -> Reader:
        """Import and construct the reader.

        Raises:
            MissingDependencyError: If importing it fails because a package isn't installed.
        """
        try:
            if isinstance(self.factory, str):
                module, _, attr = self.factory.partition(":")
                factory: Callable[[], Reader] = getattr(importlib.import_module(module), attr)
            else:
                factory = self.factory
            return factory()
        except ImportError as e:
            hint = f"; {self.install_hint}" if self.install_hint else ""
            raise MissingDependencyError(f"format {self.name!r} can't be loaded: {e}{hint}") from e
