"""Runtime helpers for authoring Nominal Hosted containerized extractors.

A containerized extractor is a Docker image Nominal runs during ingest: it mounts the
uploaded input file(s), runs your code, and ingests whatever your code writes to the
output directory. The contract is environment-driven:

- each input file is placed in the input mount (``/input``), and its path is also exposed
  in the environment variable declared for that input at registration time;
- output goes to the directory named by ``$OUTPUT_DIR``;
- single-file extractors must write exactly one file there;
- ``MANIFEST``-typed extractors instead write several files plus a ``manifest.json``
  describing each one.

This module wraps that contract so authors write only their transform. The ``@extractor``
decorator turns a ``def fn(ctx)`` into a container entrypoint: ``ctx`` resolves inputs and
parameters from the environment, collects the files you write via
:meth:`ExtractorContext.add_output`, and :meth:`Extractor.run` finalizes them -- writing
``manifest.json`` automatically for manifest extractors, enforcing the single-file rule
otherwise, and turning any failure into a non-zero exit so the ingest job fails cleanly.

The container is not told its registered output format -- Nominal injects that only into the
uploader sidecar -- so you declare it here with ``@extractor(manifest=True)``. This declaration
must match the output format the image is registered with (``MANIFEST`` vs a single-file
format); they live in two places and must agree.

It depends only on the standard library, so it stays lightweight inside a minimal extractor
image. Registering the built image with Nominal is a separate step (see the Nominal Hosted
extractor APIs); this module is only the in-container runtime.
"""

from __future__ import annotations

import enum
import json
import os
import sys
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Type, TypeVar, overload

# Mirrors the mount/env contract the Nominal ingest pipeline establishes for the customer
# container. The pipeline does NOT tell the container its registered output format, so the
# author declares manifest-vs-single-file via @extractor(manifest=...).
_DEFAULT_INPUT_DIR = "/input"
_OUTPUT_DIR_ENV = "OUTPUT_DIR"
_MANIFEST_FILENAME = "manifest.json"
# Lets tests (and non-default mounts) point input discovery somewhere other than /input.
_INPUT_DIR_ENV = "NOMINAL_EXTRACTOR_INPUT_DIR"

_T = TypeVar("_T")
_UNSET = object()


class ExtractorError(Exception):
    """Raised when the extractor contract is violated (missing input, wrong output count, ...)."""


class IngestType(str, enum.Enum):
    """How a manifest output file should be ingested. Mirrors ``ManifestIngestType``."""

    TABULAR = "TABULAR"
    AVRO_STREAM = "AVRO_STREAM"
    JSON_L = "JSON_L"


@dataclass(frozen=True)
class _Output:
    relative_path: str
    ingest_type: IngestType
    tag_columns: dict[str, str]
    channel_prefix: str | None


def _coerce(type_: Type[_T], raw: str, name: str) -> _T:
    if type_ is bool:
        lowered = raw.strip().lower()
        if lowered in ("true", "1", "yes", "y", "on"):
            return True  # type: ignore[return-value]
        if lowered in ("false", "0", "no", "n", "off"):
            return False  # type: ignore[return-value]
        raise ExtractorError(f"parameter {name!r} is not a valid boolean: {raw!r}")
    try:
        return type_(raw)  # type: ignore[call-arg]
    except (ValueError, TypeError) as ex:
        raise ExtractorError(f"parameter {name!r} could not be parsed as {type_.__name__}: {raw!r}") from ex


@dataclass
class ExtractorContext:
    """The execution context handed to an extractor function.

    Resolves inputs and parameters from the environment, and collects the output files the
    function writes. Authors do not construct this directly; :meth:`Extractor.run` builds it.
    """

    output_dir: Path
    manifest_mode: bool
    _env: Mapping[str, str] = field(repr=False)
    _input_dir: Path = field(repr=False)
    _outputs: list[_Output] = field(default_factory=list, repr=False)

    @property
    def inputs(self) -> list[Path]:
        """All input files Nominal mounted for this run, sorted by name."""
        if not self._input_dir.is_dir():
            return []
        return sorted(path for path in self._input_dir.iterdir() if path.is_file())

    def input(self, name: str | None = None) -> Path:
        """Resolve an input file.

        With ``name``, returns the path from that input's environment variable. Without it,
        returns the sole mounted input file, raising if there is not exactly one.
        """
        if name is not None:
            value = self._env.get(name)
            if not value:
                raise ExtractorError(f"input environment variable {name!r} is not set")
            return Path(value)
        files = self.inputs
        if len(files) != 1:
            raise ExtractorError(
                f"expected exactly one input file in {self._input_dir}, found {len(files)}; "
                "pass an environment-variable name to input() to select one"
            )
        return files[0]

    def param(
        self,
        name: str,
        type: Type[_T] = str,  # type: ignore[assignment]
        *,
        default: Any = _UNSET,
        required: bool = False,
    ) -> Any:
        """Read a parameter from the environment, coerced to ``type``.

        Returns ``default`` (or ``None``) when unset, unless ``required`` is set.
        """
        raw = self._env.get(name)
        if raw is None:
            if required:
                raise ExtractorError(f"required parameter {name!r} is not set")
            return None if default is _UNSET else default
        return _coerce(type, raw, name)

    def add_output(
        self,
        path: str | os.PathLike[str],
        *,
        ingest_type: IngestType = IngestType.TABULAR,
        tag_columns: Mapping[str, str] | None = None,
        channel_prefix: str | None = None,
    ) -> Path:
        """Declare a file you wrote to the output directory.

        Records the file (it must already exist under ``output_dir``); it does not write
        anything itself. For a manifest extractor these become the ``manifest.json`` entries;
        for a single-file extractor exactly one must be declared. The metadata args apply only
        to manifest extractors.
        """
        resolved = Path(path)
        if not resolved.is_file():
            raise ExtractorError(f"output file does not exist: {resolved}")
        try:
            relative = resolved.resolve().relative_to(self.output_dir.resolve())
        except ValueError:
            raise ExtractorError(f"output file {resolved} is not inside the output directory {self.output_dir}")
        self._outputs.append(
            _Output(
                relative_path=str(relative),
                ingest_type=IngestType(ingest_type),
                tag_columns=dict(tag_columns or {}),
                channel_prefix=channel_prefix,
            )
        )
        return resolved

    def build_manifest(self) -> dict[str, Any]:
        """Build the manifest document from the declared outputs (the ``ExtractorManifest`` shape)."""
        outputs: list[dict[str, Any]] = []
        for output in self._outputs:
            entry: dict[str, Any] = {
                "ingestType": output.ingest_type.value,
                "relativePath": output.relative_path,
                "tagColumns": output.tag_columns,
            }
            if output.channel_prefix is not None:
                entry["channelPrefix"] = output.channel_prefix
            outputs.append(entry)
        return {"outputs": outputs}


_ExtractorFn = Callable[[ExtractorContext], None]


@dataclass(frozen=True)
class Extractor:
    """A containerized-extractor entrypoint produced by :func:`extractor`.

    Call it directly with an :class:`ExtractorContext` (useful in tests), or call
    :meth:`run` as the container's entrypoint to drive it from the environment.
    """

    _fn: _ExtractorFn
    manifest: bool = False

    def __call__(self, ctx: ExtractorContext) -> None:
        self._fn(ctx)

    def run(self, *, env: Mapping[str, str] | None = None, exit: bool = True) -> ExtractorContext:
        """Run the extractor against the environment and finalize its outputs.

        Intended as the container entrypoint (``if __name__ == "__main__": my_extractor.run()``).
        On success returns the context; on failure prints a traceback and, when ``exit`` is
        True (the default), exits with a non-zero status so the ingest job fails. Pass
        ``exit=False`` to re-raise instead -- useful in tests.
        """
        environ = os.environ if env is None else env
        try:
            ctx = self._build_context(environ)
            self._fn(ctx)
            self._finalize(ctx)
            return ctx
        except BaseException:
            if exit:
                traceback.print_exc()
                sys.exit(1)
            raise

    def _build_context(self, env: Mapping[str, str]) -> ExtractorContext:
        output_dir = env.get(_OUTPUT_DIR_ENV)
        if not output_dir:
            raise ExtractorError(f"{_OUTPUT_DIR_ENV} is not set; this code must run inside a Nominal extractor")
        input_dir = env.get(_INPUT_DIR_ENV, _DEFAULT_INPUT_DIR)
        return ExtractorContext(
            output_dir=Path(output_dir),
            manifest_mode=self.manifest,
            _env=env,
            _input_dir=Path(input_dir),
        )

    def _finalize(self, ctx: ExtractorContext) -> None:
        if self.manifest:
            if not ctx._outputs:
                raise ExtractorError("manifest extractor produced no outputs; call ctx.add_output() for each file")
            (ctx.output_dir / _MANIFEST_FILENAME).write_text(json.dumps(ctx.build_manifest()))
        elif len(ctx._outputs) != 1:
            raise ExtractorError(
                f"single-file extractor must produce exactly one output, got {len(ctx._outputs)}; "
                "declare @extractor(manifest=True) and register the image with the MANIFEST output format "
                "to emit multiple files"
            )


@overload
def extractor(fn: _ExtractorFn) -> Extractor: ...


@overload
def extractor(*, manifest: bool = ...) -> Callable[[_ExtractorFn], Extractor]: ...


def extractor(
    fn: _ExtractorFn | None = None,
    *,
    manifest: bool = False,
) -> Extractor | Callable[[_ExtractorFn], Extractor]:
    """Turn ``def fn(ctx: ExtractorContext) -> None`` into a Nominal extractor entrypoint.

    Use ``@extractor`` for a single-file extractor (write one file to ``ctx.output_dir``), or
    ``@extractor(manifest=True)`` for a manifest extractor (write several files plus an
    auto-generated ``manifest.json``). The ``manifest`` choice must match the output format the
    image is registered with.

    Example::

        from nominal.experimental.extractor import extractor, ExtractorContext, IngestType

        @extractor(manifest=True)
        def split(ctx: ExtractorContext) -> None:
            table = read_parquet(ctx.input())
            for i, chunk in enumerate(chunks_of(table, ctx.param("PARTS", int, default=2))):
                out = ctx.output_dir / f"part_{i}.parquet"
                write_parquet(chunk, out)
                ctx.add_output(out, ingest_type=IngestType.TABULAR)

        if __name__ == "__main__":
            split.run()
    """

    def wrap(target: _ExtractorFn) -> Extractor:
        return Extractor(target, manifest=manifest)

    return wrap if fn is None else wrap(fn)
