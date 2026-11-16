"""Find readers, pick one for a file, and read it."""

from __future__ import annotations

import threading
import warnings
from collections.abc import Callable, Iterator
from importlib.metadata import entry_points
from os import PathLike
from pathlib import Path
from typing import Any, TypeVar, cast

from nominal.io._reader import MissingDependencyError, Reader, ReaderSpec
from nominal.io.schema import ENGINES, Chunk

ENTRY_POINT_GROUP = "nominal.io.readers"
"""Entry-point group a package uses to add formats; each entry's value is a `ReaderSpec`."""

_R = TypeVar("_R")

_lock = threading.Lock()
_specs: dict[str, ReaderSpec] = {}
_discovered = False
_readers: dict[str, Reader] = {}


def register(spec: ReaderSpec, *, replace: bool = False) -> None:
    """Add a format for this Python session.

    Packages should use the ``nominal.io.readers`` entry-point group instead; this is for readers
    defined in a notebook or a script.

    Raises:
        ValueError: If a format with this name exists and ``replace`` is false.
    """
    _discover()
    with _lock:
        if spec.name in _specs and not replace:
            raise ValueError(f"format {spec.name!r} is already registered; pass replace=True to override it")
        _specs[spec.name] = spec
        _readers.pop(spec.name, None)


def register_reader(
    name: str,
    extensions: tuple[str, ...] | list[str],
    *,
    description: str = "",
    install_hint: str | None = None,
    replace: bool = False,
) -> Callable[[_R], _R]:
    """Class decorator that registers a `Reader` subclass for this session.

    ::

        @register_reader("mylog", [".mylog"])
        class MyLog(Reader):
            def read_chunks(self, path, **options): ...
    """

    def decorate(cls: _R) -> _R:
        register(
            ReaderSpec(name, tuple(extensions), cast("Callable[[], Reader]", cls), description, install_hint),
            replace=replace,
        )
        return cls

    return decorate


def unregister(name: str) -> None:
    """Remove a format registered in this session (or discovered); mainly for tests."""
    _discover()
    with _lock:
        _specs.pop(name, None)
        _readers.pop(name, None)


def list_formats() -> list[dict[str, Any]]:
    """Describe every known format, without importing any reader.

    Returns:
        One dict per format, sorted by name, with ``name``, ``extensions``, ``description``
        and ``install_hint``.
    """
    _discover()
    return [
        {"name": s.name, "extensions": list(s.extensions), "description": s.description, "install_hint": s.install_hint}
        for s in sorted(_specs.values(), key=lambda s: s.name)
    ]


def get_reader(format: str) -> Reader:
    """Return the loaded reader for a format name.

    Raises:
        ValueError: If no such format is registered.
        MissingDependencyError: If its dependencies aren't installed.
    """
    _discover()
    try:
        spec = _specs[format]
    except KeyError:
        known = ", ".join(sorted(_specs)) or "none installed"
        raise ValueError(f"unknown format {format!r}; known formats: {known}") from None
    return _load(spec)


def resolve_format(path: str | PathLike[str], format: str | None = None) -> ReaderSpec:
    """Choose the format for ``path``.

    An explicit ``format`` wins. Otherwise formats claiming the longest matching extension
    compete; if there are several, each one's `~nominal.io.Reader.sniff` breaks the tie.

    Raises:
        ValueError: If no format matches, or several still do (the message names them).
    """
    _discover()
    if format is not None:
        get_reader(format)  # for the unknown-format error
        return _specs[format]
    name = Path(path).name.lower()
    matches = [
        (max(len(e) for e in s.extensions if name.endswith(e)), s)
        for s in _specs.values()
        if any(name.endswith(e) for e in s.extensions)
    ]
    if not matches:
        exts = sorted({e for s in _specs.values() for e in s.extensions})
        raise ValueError(f"no format reads {Path(path).name!r}; known extensions: {', '.join(exts) or 'none'}")
    longest = max(length for length, _ in matches)
    candidates = sorted((s for length, s in matches if length == longest), key=lambda s: s.name)
    if len(candidates) == 1:
        return candidates[0]
    verdicts: dict[str, bool | None] = {}
    for spec in candidates:
        try:
            verdicts[spec.name] = _load(spec).sniff(path)
        except MissingDependencyError:
            verdicts[spec.name] = None  # can't rule it in or out
    remaining = [s for s in candidates if verdicts[s.name] is not False]
    confirmed = [s for s in remaining if verdicts[s.name] is True]
    for pool in (confirmed, remaining):
        if len(pool) == 1:
            return pool[0]
    names = ", ".join(s.name for s in (confirmed or remaining or candidates))
    raise ValueError(f"can't tell which format {Path(path).name!r} is; candidates: {names}. Pass format=...")


def read_chunks(
    path: str | PathLike[str], *, format: str | None = None, engine: str | None = None, **options: Any
) -> Iterator[Any]:
    """Read a file as an iterator of chunks, so large files needn't fit in memory.

    Args:
        path: The file.
        format: A format name from `list_formats`; chosen from the file otherwise.
        engine: ``None`` to yield `~nominal.io.schema.Chunk` objects, or ``"pandas"`` or ``"polars"`` to
            yield DataFrames of that library.
        **options: Passed to the format's reader.

    Raises:
        ValueError: If the format can't be chosen, the engine is unknown, or a reader yields invalid data.
        MissingDependencyError: If the format's dependencies aren't installed.
    """
    if engine is not None and engine not in ENGINES:
        raise ValueError(f"unknown engine {engine!r}; expected one of {', '.join(ENGINES)}")
    reader = _load(resolve_format(path, format))
    for chunk in reader.read_chunks(path, **options):
        if not isinstance(chunk, Chunk):
            raise ValueError(f"{type(reader).__name__}.read_chunks yielded a {type(chunk).__name__}, not a Chunk")
        chunk.validate()
        yield chunk if engine is None else chunk.to_frame(engine)


def read(path: str | PathLike[str], *, format: str | None = None, engine: str = "pandas", **options: Any) -> Any:
    """Read a whole file as one frame, sorted by timestamp.

    Args:
        path: The file.
        format: A format name from `list_formats`; chosen from the file otherwise.
        engine: The frame library: ``"pandas"`` (the default) or ``"polars"``.
        **options: Passed to the format's reader.

    Returns:
        A DataFrame with one row per sample (see `nominal.io.schema`).
    """
    return Chunk.concat(read_chunks(path, format=format, **options)).to_frame(engine)


def _load(spec: ReaderSpec) -> Reader:
    """Return the spec's reader, loading it the first time."""
    with _lock:
        reader = _readers.get(spec.name)
    if reader is None:
        reader = spec.load()
        with _lock:
            _readers[spec.name] = reader
    return reader


def _discover() -> None:
    """Load the specs that installed packages declare, once."""
    global _discovered
    with _lock:
        if _discovered:
            return
        _discovered = True
        for ep in entry_points(group=ENTRY_POINT_GROUP):
            try:
                spec = ep.load()
                if not isinstance(spec, ReaderSpec):
                    raise TypeError(f"{ep.value} is a {type(spec).__name__}, not a ReaderSpec")
                if spec.name != ep.name:
                    raise ValueError(f"entry point {ep.name!r} holds a spec named {spec.name!r}")
            except Exception as e:  # one broken plugin must not take the others down
                warnings.warn(f"skipping nominal.io reader {ep.name!r}: {e}", RuntimeWarning, stacklevel=2)
                continue
            _specs.setdefault(spec.name, spec)
