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

Nominal describes the extractor's registered contract to the container through ``_NOMINAL_*``
environment variables -- the registered output format (``_NOMINAL_OUTPUT_FORMAT``), the mounted
inputs (``_NOMINAL_INPUTS``), and the declared parameters (``_NOMINAL_PARAMETERS``). The decorator
reads these so it neither has to inspect the filesystem nor be told the mode: manifest-vs-single-file
is taken from the registered format, and :meth:`ExtractorContext.input`/:meth:`ExtractorContext.param`
resolve by either the registered display name or the environment variable. You may still pass
``@extractor(manifest=...)`` to assert the mode you expect -- if it disagrees with the registered
format the decorator fails loudly rather than emitting output the uploader will reject. When the
variables are absent (an older backend, or a local run) the decorator falls back to the declared
flag, to listing the input mount, and to treating parameters as optional unless ``required=True`` is
passed explicitly.

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
# container.
_DEFAULT_INPUT_DIR = "/input"
_OUTPUT_DIR_ENV = "OUTPUT_DIR"
_MANIFEST_FILENAME = "manifest.json"
# Lets tests (and non-default mounts) point input discovery somewhere other than /input.
_INPUT_DIR_ENV = "NOMINAL_EXTRACTOR_INPUT_DIR"
# Contract metadata Nominal injects describing the registered extractor (scout
# ContainerizedExtractorV1ActivitiesImpl). All optional: absent on older backends and local runs.
_OUTPUT_FORMAT_ENV = "_NOMINAL_OUTPUT_FORMAT"  # registered FileOutputFormat name, e.g. "MANIFEST", "PARQUET"
_INPUTS_ENV = "_NOMINAL_INPUTS"  # JSON: [{"name","environmentVariable","path","required"}]
_PARAMETERS_ENV = "_NOMINAL_PARAMETERS"  # JSON: [{"name","environmentVariable","required"}]
_MANIFEST_FORMAT = "MANIFEST"  # the _NOMINAL_OUTPUT_FORMAT value that means manifest mode

_T = TypeVar("_T")
_UNSET = object()


class ExtractorError(Exception):
    """Raised when the extractor contract is violated (missing input, wrong output count, ...).

    Deliberately does not extend ``nominal.core.exceptions.NominalError``: importing anything under
    ``nominal.core`` executes ``nominal/core/__init__.py``, which imports ``NominalClient`` and pulls
    in ``nominal_api``/``conjure_python_client`` -- exactly the dependency graph this module avoids to
    stay light inside a minimal extractor image.
    """


class IngestType(str, enum.Enum):
    """How a manifest output file should be ingested. Mirrors ``ManifestIngestType``."""

    TABULAR = "TABULAR"
    AVRO_STREAM = "AVRO_STREAM"
    JSON_L = "JSON_L"


class EpochTimeUnit(str, enum.Enum):
    """Time unit of a numeric epoch timestamp. Mirrors ``ManifestEpochTimeUnit``."""

    SECONDS = "SECONDS"
    MILLISECONDS = "MILLISECONDS"
    MICROSECONDS = "MICROSECONDS"
    NANOSECONDS = "NANOSECONDS"


@dataclass(frozen=True)
class TimestampMetadata:
    """Per-output timestamp metadata for a manifest entry. Mirrors ``ManifestTimestampMetadata``.

    ``series_name`` is the column (TABULAR) or top-level JSON field (JSON_L) that holds the
    timestamp for this output; ``epoch_time_unit`` is the unit of that numeric value. When set on
    an output it overrides the job-level timestamp metadata for that file, letting outputs of
    different formats use different timestamp fields. Only numeric epoch timestamps are
    expressible here -- outputs needing ISO 8601 / custom-format timestamps should omit it and
    rely on the job-level metadata.

    Distinct from ``nominal.core.containerized_extractors.TimestampMetadata``, which describes a
    registration-time (job-level) timestamp field rather than a per-manifest-output override.
    """

    series_name: str
    epoch_time_unit: EpochTimeUnit


@dataclass(frozen=True)
class _InputSpec:
    """A mounted input file as described by ``_NOMINAL_INPUTS`` (registered name + resolved path)."""

    environment_variable: str
    name: str
    path: str
    required: bool


def _parse_input_specs(env: Mapping[str, str]) -> list[_InputSpec] | None:
    """Parse ``_NOMINAL_INPUTS`` into specs, or ``None`` when Nominal didn't inject it."""
    raw = env.get(_INPUTS_ENV)
    if not raw:
        return None
    try:
        entries = json.loads(raw)
    except json.JSONDecodeError as ex:
        raise ExtractorError(f"{_INPUTS_ENV} is not valid JSON: {raw!r}") from ex
    return [
        _InputSpec(
            environment_variable=entry["environmentVariable"],
            name=entry.get("name", entry["environmentVariable"]),
            path=entry["path"],
            required=bool(entry.get("required", False)),
        )
        for entry in entries
    ]


@dataclass(frozen=True)
class _ParameterSpec:
    """A declared parameter as described by ``_NOMINAL_PARAMETERS`` (registered name + required flag).

    The parameter's value is not carried here -- it is exposed separately under ``environment_variable``.
    """

    environment_variable: str
    name: str
    required: bool


def _parse_parameter_specs(env: Mapping[str, str]) -> list[_ParameterSpec] | None:
    """Parse ``_NOMINAL_PARAMETERS`` into specs, or ``None`` when Nominal didn't inject it."""
    raw = env.get(_PARAMETERS_ENV)
    if not raw:
        return None
    try:
        entries = json.loads(raw)
    except json.JSONDecodeError as ex:
        raise ExtractorError(f"{_PARAMETERS_ENV} is not valid JSON: {raw!r}") from ex
    return [
        _ParameterSpec(
            environment_variable=entry["environmentVariable"],
            name=entry.get("name", entry["environmentVariable"]),
            # Unset means required on the platform (see scout's ExtractionContractDefaults.isRequired),
            # but scout always resolves this before serializing here; True is just a defensive fallback.
            required=bool(entry.get("required", True)),
        )
        for entry in entries
    ]


@dataclass(frozen=True)
class _Output:
    relative_path: str
    ingest_type: IngestType
    tag_columns: dict[str, str]
    channel_prefix: str | None
    timestamp_metadata: TimestampMetadata | None


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
    _input_specs: list[_InputSpec] | None = field(default=None, repr=False)
    _param_specs: list[_ParameterSpec] | None = field(default=None, repr=False)
    _outputs: list[_Output] = field(default_factory=list, repr=False)

    @property
    def inputs(self) -> list[Path]:
        """All input files Nominal mounted for this run.

        Taken from the registered ``_NOMINAL_INPUTS`` metadata when present -- in the order Nominal
        serializes them (sorted by environment variable name, not necessarily registration order);
        otherwise discovered by listing the input mount, sorted by name.
        """
        if self._input_specs is not None:
            return [Path(spec.path) for spec in self._input_specs]
        if not self._input_dir.is_dir():
            return []
        return sorted(path for path in self._input_dir.iterdir() if path.is_file())

    def input(self, name: str | None = None) -> Path:
        """Resolve an input file.

        With ``name`` -- the input's registered display name or its environment variable -- returns
        that input's path. Without it, returns the sole mounted input file, raising if there is not
        exactly one.
        """
        if name is not None:
            if self._input_specs is not None:
                for spec in self._input_specs:
                    if name in (spec.environment_variable, spec.name):
                        return Path(spec.path)
            value = self._env.get(name)
            if value:
                return Path(value)
            raise ExtractorError(
                f"input {name!r} is not set; no matching _NOMINAL_INPUTS entry or environment variable"
            )
        files = self.inputs
        if len(files) != 1:
            raise ExtractorError(
                f"expected exactly one input file, found {len(files)}; pass an input name to input() to select one"
            )
        return files[0]

    def param(
        self,
        name: str,
        type: Type[_T] = str,  # type: ignore[assignment]
        *,
        default: Any = _UNSET,
        required: bool | None = None,
    ) -> Any:
        """Read a parameter from the environment, coerced to ``type``.

        ``name`` -- the parameter's registered display name or its environment variable -- is
        resolved against ``_NOMINAL_PARAMETERS`` when Nominal injected it; otherwise it is treated
        directly as the environment variable. Returns ``default`` (or ``None``) when unset, unless
        the parameter is required: pass ``required=True``/``False`` to say so explicitly, or omit it
        to take the registered contract's required flag (unset parameters are required by default,
        matching the platform); with no registered contract, an omitted ``required`` defaults to
        ``False``.
        """
        env_var, registered_required = self._resolve_param(name)
        raw = self._env.get(env_var)
        if raw is None:
            effective_required = registered_required if required is None else required
            if effective_required:
                raise ExtractorError(f"required parameter {name!r} is not set")
            return None if default is _UNSET else default
        return _coerce(type, raw, name)

    def _resolve_param(self, name: str) -> tuple[str, bool]:
        """Resolve a parameter name to its environment variable and registered required flag."""
        if self._param_specs is not None:
            for spec in self._param_specs:
                if name in (spec.environment_variable, spec.name):
                    return spec.environment_variable, spec.required
        return name, False

    def add_output(
        self,
        path: str | os.PathLike[str],
        *,
        ingest_type: IngestType = IngestType.TABULAR,
        tag_columns: Mapping[str, str] | None = None,
        channel_prefix: str | None = None,
        timestamp_metadata: TimestampMetadata | None = None,
    ) -> Path:
        """Declare a file you wrote to the output directory.

        Records the file (it must already exist under ``output_dir``); it does not write
        anything itself. For a manifest extractor these become the ``manifest.json`` entries;
        for a single-file extractor exactly one must be declared. The metadata args apply only
        to manifest extractors.

        ``timestamp_metadata`` overrides the job-level timestamp metadata for this output, so a
        manifest job can give each file its own timestamp field. For ``JSON_L`` outputs each line
        must still contain a ``MESSAGE`` field; that path is log ingest.
        """
        resolved = Path(path)
        if not resolved.is_file():
            raise ExtractorError(f"output file does not exist: {resolved}")
        try:
            relative = resolved.resolve().relative_to(self.output_dir.resolve())
        except ValueError as ex:
            raise ExtractorError(f"output file {resolved} is not inside the output directory {self.output_dir}") from ex
        self._outputs.append(
            _Output(
                relative_path=str(relative),
                ingest_type=IngestType(ingest_type),
                tag_columns=dict(tag_columns or {}),
                channel_prefix=channel_prefix,
                timestamp_metadata=timestamp_metadata,
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
            if output.timestamp_metadata is not None:
                entry["timestampMetadata"] = {
                    "seriesName": output.timestamp_metadata.series_name,
                    "epochTimeUnit": output.timestamp_metadata.epoch_time_unit.value,
                }
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
    manifest: bool | None = None

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
        except BaseException:  # deliberately broad: any failure (incl. SystemExit/KeyboardInterrupt
            # from user code) must fail the ingest job cleanly, not just Exception subclasses.
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
            manifest_mode=self._resolve_manifest_mode(env),
            _env=env,
            _input_dir=Path(input_dir),
            _input_specs=_parse_input_specs(env),
            _param_specs=_parse_parameter_specs(env),
        )

    def _resolve_manifest_mode(self, env: Mapping[str, str]) -> bool:
        """Decide manifest-vs-single-file from the registered output format and the declared flag.

        The registered format (``_NOMINAL_OUTPUT_FORMAT``) wins when present; the declared
        ``manifest=`` flag is an assertion that must agree with it. Falls back to the declared flag
        (default single-file) when Nominal didn't inject the format.
        """
        registered = env.get(_OUTPUT_FORMAT_ENV)
        registered_manifest = (registered == _MANIFEST_FORMAT) if registered else None
        if self.manifest is None:
            return False if registered_manifest is None else registered_manifest
        if registered_manifest is not None and registered_manifest != self.manifest:
            raise ExtractorError(
                f"@extractor(manifest={self.manifest}) disagrees with the image's registered output "
                f"format {registered!r} (_NOMINAL_OUTPUT_FORMAT). Re-register the image or fix the "
                "decorator so the declared mode and the registered format agree."
            )
        return self.manifest

    def _finalize(self, ctx: ExtractorContext) -> None:
        self._check_for_undeclared_output_files(ctx)
        if ctx.manifest_mode:
            if not ctx._outputs:
                raise ExtractorError("manifest extractor produced no outputs; call ctx.add_output() for each file")
            (ctx.output_dir / _MANIFEST_FILENAME).write_text(json.dumps(ctx.build_manifest()))
        elif len(ctx._outputs) != 1:
            raise ExtractorError(
                f"single-file extractor must produce exactly one output, got {len(ctx._outputs)}; "
                "register the image with the MANIFEST output format (or declare @extractor(manifest=True)) "
                "to emit multiple files"
            )

    @staticmethod
    def _check_for_undeclared_output_files(ctx: ExtractorContext) -> None:
        """Reject files sitting in ``output_dir`` that were never passed to ``add_output()``.

        Counting only declared outputs isn't enough to catch a stray file: an author who writes two
        files in single-file mode but only declares one would pass that count check while still
        leaving two files on disk, reproducing the downstream ``MultipleFilesFound`` failure this
        module exists to catch earlier.
        """
        declared = {output.relative_path for output in ctx._outputs}
        actual = {str(path.relative_to(ctx.output_dir)) for path in ctx.output_dir.rglob("*") if path.is_file()}
        undeclared = sorted(actual - declared)
        if undeclared:
            raise ExtractorError(
                f"output directory contains file(s) not passed to ctx.add_output(): {undeclared}; declare "
                "every file you want ingested, or remove stray files from the output directory"
            )


@overload
def extractor(fn: _ExtractorFn) -> Extractor: ...


@overload
def extractor(*, manifest: bool | None = ...) -> Callable[[_ExtractorFn], Extractor]: ...


def extractor(
    fn: _ExtractorFn | None = None,
    *,
    manifest: bool | None = None,
) -> Extractor | Callable[[_ExtractorFn], Extractor]:
    """Turn ``def fn(ctx: ExtractorContext) -> None`` into a Nominal extractor entrypoint.

    Whether the extractor writes a single file or several files plus a ``manifest.json`` is taken
    from the output format the image is registered with (Nominal injects it as
    ``_NOMINAL_OUTPUT_FORMAT``), so ``@extractor`` alone works for both. Pass ``manifest=True`` (or
    ``manifest=False``) only when you want to assert the mode -- the decorator then fails loudly if
    your assertion disagrees with the registered format. When the format isn't injected (older
    backend, or a local run) the declared flag is used, defaulting to single-file.

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
