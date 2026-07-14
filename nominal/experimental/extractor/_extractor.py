"""Runtime helpers for authoring Nominal Hosted containerized extractors.

A containerized extractor is a Docker image Nominal runs during ingest: it mounts the
uploaded input file(s), runs your code, and ingests whatever your code writes to the
output directory. The contract is environment-driven:

- each input file is placed in the input mount (``/input``), and its path is also exposed
  in the environment variable declared for that input at registration time;
- output goes to the directory named by ``$OUTPUT_DIR``;
- declared parameters arrive as environment variables (values are always strings).

The image's registered output format fixes which of two output contracts the ingest
pipeline applies, and this module provides one decorator per contract:

- :func:`single_file_extractor` -- the pipeline ingests exactly one output file, parsed
  according to the registered format (``PARQUET``, ``CSV``, ...). Your function declares
  that file with :meth:`SingleFileExtractorContext.set_output`.
- :func:`manifest_extractor` -- for images registered with the ``MANIFEST`` output format.
  The pipeline reads a ``manifest.json`` describing every output file; your function
  declares each file (with per-file ingest type, tag columns, channel prefix, and optional
  epoch timestamp metadata) with :meth:`ManifestExtractorContext.add_output`, and
  :meth:`Extractor.run` writes the manifest automatically.

Both decorators turn ``def fn(ctx) -> None`` into a container entrypoint: ``ctx`` resolves
inputs and parameters from the environment, and :meth:`Extractor.run` finalizes the outputs
and turns any failure into a non-zero exit so the ingest job fails cleanly. Nominal describes
the extractor's registered contract to the container through ``_NOMINAL_*`` environment
variables -- the registered output format (``_NOMINAL_OUTPUT_FORMAT``), the mounted inputs
(``_NOMINAL_INPUTS``), and the declared parameters (``_NOMINAL_PARAMETERS``). When the
registered output format is injected and disagrees with the decorator you used, the run fails
at startup with a clear error rather than emitting output the pipeline will reject; when it
is absent (a local run) the decorator's word is law.

Newer ingest pipelines additionally inject system metadata -- the ingest job and dataset RIDs,
the resolved job-level timestamp metadata, and the ingest request's tags -- exposed through
:attr:`ExtractorContext.ingest_job_rid`, :attr:`ExtractorContext.dataset_rid`,
:attr:`ExtractorContext.job_timestamp_metadata`, and :attr:`ExtractorContext.additional_tags`.
All are optional: None/empty when not injected (e.g. local runs).

The manifest document is emitted through the generated ``nominal_api.ingest_manifest`` types, so
its schema tracks the platform contract instead of being hand-mirrored here. Format I/O (pyarrow,
etc.) remains the author's own dependency. Registering the built image with Nominal is a separate
step (see the Nominal Hosted extractor APIs); this module is only the in-container runtime.
"""

from __future__ import annotations

import enum
import functools
import json
import logging
import os
import sys
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Generic, Mapping, TypeVar, overload

from conjure_python_client import ConjureDecoder, ConjureEncoder
from nominal_api import ingest_manifest, scout_catalog
from typing_extensions import assert_never

from nominal import ts
from nominal.core.container_image import FileOutputFormat, TimestampMetadata
from nominal.core.exceptions import NominalError

# Mirrors the mount/env contract the Nominal ingest pipeline establishes for the customer
# container.
_DEFAULT_INPUT_DIR = "/input"
_OUTPUT_DIR_ENV = "OUTPUT_DIR"
_MANIFEST_FILENAME = "manifest.json"  # the well-known name the ingest pipeline reads from the output directory
# Lets tests (and non-default mounts) point input discovery somewhere other than /input.
_INPUT_DIR_ENV = "NOMINAL_EXTRACTOR_INPUT_DIR"
# Contract metadata Nominal injects describing the registered extractor. All optional: absent on
# local runs.
_OUTPUT_FORMAT_ENV = "_NOMINAL_OUTPUT_FORMAT"  # registered FileOutputFormat name, e.g. "MANIFEST", "PARQUET"
_INPUTS_ENV = "_NOMINAL_INPUTS"  # JSON: [{"name","environmentVariable","path","required"}]
_PARAMETERS_ENV = "_NOMINAL_PARAMETERS"  # JSON: [{"name","environmentVariable","required"}]
# System metadata newer ingest pipelines additionally inject alongside the extractor's own
# arguments. All optional: absent when not injected (e.g. local runs).
_INGEST_JOB_RID_ENV = "_NOMINAL_INGEST_JOB_RID"
_DATASET_RID_ENV = "_NOMINAL_DATASET_RID"
_JOB_TIMESTAMP_METADATA_ENV = "_NOMINAL_TIMESTAMP_METADATA"  # JSON: the resolved job-level timestamp metadata
_ADDITIONAL_TAGS_ENV = "_NOMINAL_ADDITIONAL_TAGS"  # JSON: {"tag": "value"} applied to all ingested data

logger = logging.getLogger(__name__)


class ExtractorError(NominalError):
    """Raised when the extractor contract is violated (missing input, wrong output count, ...)."""


class IngestType(enum.Enum):
    """How a manifest output file should be ingested."""

    TABULAR = "TABULAR"
    AVRO_STREAM = "AVRO_STREAM"
    JSON_L = "JSON_L"

    def _to_conjure(self) -> ingest_manifest.ManifestIngestType:
        match self:
            case IngestType.TABULAR:
                result = ingest_manifest.ManifestIngestType.TABULAR
            case IngestType.AVRO_STREAM:
                result = ingest_manifest.ManifestIngestType.AVRO_STREAM
            case IngestType.JSON_L:
                result = ingest_manifest.ManifestIngestType.JSON_L
            case _:
                assert_never(self)
        return result


def _json_env(env: Mapping[str, str], var: str) -> Any:
    """Parse a JSON-valued environment variable, or None when it is absent/empty."""
    raw = env.get(var)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as ex:
        raise ExtractorError(f"{var} is not valid JSON: {raw!r}") from ex


_MANIFEST_EPOCH_UNITS: dict[str, ingest_manifest.ManifestEpochTimeUnit] = {
    "seconds": ingest_manifest.ManifestEpochTimeUnit.SECONDS,
    "milliseconds": ingest_manifest.ManifestEpochTimeUnit.MILLISECONDS,
    "microseconds": ingest_manifest.ManifestEpochTimeUnit.MICROSECONDS,
    "nanoseconds": ingest_manifest.ManifestEpochTimeUnit.NANOSECONDS,
}


def _manifest_epoch_time_unit(timestamp_type: ts._AnyTimestampType) -> ingest_manifest.ManifestEpochTimeUnit:
    """Validate a per-output timestamp type against the manifest contract and convert it.

    Manifest outputs can only express numeric epoch timestamps in seconds through nanoseconds;
    outputs needing richer types (ISO 8601, custom formats, relative offsets) must omit per-output
    metadata and rely on the job-level timestamp metadata, which supports the full range.
    """
    typed = ts._to_typed_timestamp_type(timestamp_type)
    if not isinstance(typed, ts.Epoch):
        raise ExtractorError(
            f"per-output timestamp metadata only supports numeric epoch timestamps, not {typed!r}; "
            "omit it and rely on the job-level timestamp metadata for richer timestamp types"
        )
    unit = _MANIFEST_EPOCH_UNITS.get(typed.unit)
    if unit is None:
        raise ExtractorError(
            f"per-output timestamp metadata does not support epoch unit {typed.unit!r}; "
            f"supported units are {', '.join(_MANIFEST_EPOCH_UNITS)}"
        )
    return unit


@dataclass(frozen=True)
class _InputSpec:
    """One entry of the registered input metadata (``_NOMINAL_INPUTS``).

    ``path`` is where the file is mounted; it is also exposed directly under
    ``environment_variable``.
    """

    environment_variable: str
    name: str
    path: str


@dataclass(frozen=True)
class _ParamSpec:
    """One entry of the registered parameter metadata (``_NOMINAL_PARAMETERS``).

    The value itself is exposed separately under ``environment_variable``.
    """

    environment_variable: str
    name: str
    required: bool


_SpecT = TypeVar("_SpecT", _InputSpec, _ParamSpec)


def _parse_input_specs(env: Mapping[str, str]) -> list[_InputSpec] | None:
    """Parse ``_NOMINAL_INPUTS`` into specs, or ``None`` when not injected."""
    entries = _json_env(env, _INPUTS_ENV)
    if entries is None:
        return None
    try:
        return [
            _InputSpec(
                environment_variable=entry["environmentVariable"],
                name=entry.get("name", entry["environmentVariable"]),
                path=entry["path"],
            )
            for entry in entries
        ]
    except (KeyError, TypeError, AttributeError) as ex:
        raise ExtractorError(f"{_INPUTS_ENV} is not valid extractor contract metadata: {entries!r}") from ex


def _parse_param_specs(env: Mapping[str, str]) -> list[_ParamSpec] | None:
    """Parse ``_NOMINAL_PARAMETERS`` into specs, or ``None`` when not injected."""
    entries = _json_env(env, _PARAMETERS_ENV)
    if entries is None:
        return None
    try:
        return [
            _ParamSpec(
                environment_variable=entry["environmentVariable"],
                name=entry.get("name", entry["environmentVariable"]),
                required=bool(entry["required"]),
            )
            for entry in entries
        ]
    except (KeyError, TypeError, AttributeError) as ex:
        raise ExtractorError(f"{_PARAMETERS_ENV} is not valid extractor contract metadata: {entries!r}") from ex


def _find_spec(specs: list[_SpecT] | None, name: str) -> _SpecT | None:
    """Find a spec by its registered display name or environment variable."""
    for spec in specs or []:
        if name in (spec.environment_variable, spec.name):
            return spec
    return None


def _spec_names(specs: list[_InputSpec] | list[_ParamSpec]) -> str:
    """Render specs as 'ENV_VAR' or 'ENV_VAR (display name)' for error messages."""
    return ", ".join(
        spec.environment_variable
        if spec.name == spec.environment_variable
        else f"{spec.environment_variable} ({spec.name!r})"
        for spec in specs
    )


@dataclass
class ExtractorContext:
    """The execution context handed to an extractor function.

    Resolves inputs and parameters from the environment, and collects the output files the
    function writes. Authors do not construct this directly; :meth:`Extractor.run` builds a
    :class:`SingleFileExtractorContext` or :class:`ManifestExtractorContext`.
    """

    output_dir: Path
    _env: Mapping[str, str] = field(repr=False)
    _input_dir: Path = field(repr=False)
    _input_specs: list[_InputSpec] | None = field(default=None, repr=False)
    _param_specs: list[_ParamSpec] | None = field(default=None, repr=False)

    @property
    def inputs(self) -> list[Path]:
        """All input files Nominal mounted for this run.

        Taken from the registered ``_NOMINAL_INPUTS`` metadata when present, in the order Nominal
        serializes them; otherwise discovered by listing the input mount, sorted by name.
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
            spec = _find_spec(self._input_specs, name)
            if spec is not None:
                return Path(spec.path)
            if self._input_specs is None:
                value = self._env.get(name)
                if value:
                    return Path(value)
                raise ExtractorError(f"input {name!r} is not set; no matching environment variable")
            raise ExtractorError(
                f"input {name!r} is not among this run's inputs: {_spec_names(self._input_specs) or '(none)'}; "
                "an optional input not provided by the ingest request is not listed"
            )
        files = self.inputs
        if len(files) != 1:
            raise ExtractorError(
                f"expected exactly one input file, found {len(files)}; pass an input name to input() to select one"
            )
        return files[0]

    def _param_env_var(self, name: str) -> str:
        """Resolve a parameter name to its environment variable.

        With registered contract metadata, the contract is authoritative: a name with no entry is
        an authoring error. Without it (a local run), ``name`` is treated as the environment
        variable directly.
        """
        spec = _find_spec(self._param_specs, name)
        if spec is not None:
            return spec.environment_variable
        if self._param_specs is None:
            return name
        raise ExtractorError(
            f"unknown parameter {name!r}; registered parameters are: {_spec_names(self._param_specs) or '(none)'}"
        )

    def param(self, name: str) -> str:
        """Read a required parameter from the environment.

        ``name`` -- the parameter's registered display name or its environment variable -- is
        resolved against ``_NOMINAL_PARAMETERS`` when Nominal injected it; otherwise it is treated
        directly as the environment variable. Raises :class:`ExtractorError` when the parameter is
        not set. Parameter values are strings; coerce them yourself: ``int(ctx.param("PARTS"))``.
        With registered contract metadata present, an unregistered ``name`` raises
        :class:`ExtractorError`.
        """
        raw = self._env.get(self._param_env_var(name))
        if raw is None:
            raise ExtractorError(f"required parameter {name!r} is not set")
        return raw

    @overload
    def get_param(self, name: str, default: None = None) -> str | None: ...

    @overload
    def get_param(self, name: str, default: str) -> str: ...

    def get_param(self, name: str, default: str | None = None) -> str | None:
        """Read an optional parameter from the environment, or ``default`` when unset.

        Name resolution matches :meth:`param`. Parameter values are strings; coerce them
        yourself: ``int(ctx.get_param("PARTS", "2"))``. With registered contract metadata present,
        an unregistered ``name`` raises :class:`ExtractorError`.
        """
        raw = self._env.get(self._param_env_var(name))
        return default if raw is None else raw

    @property
    def ingest_job_rid(self) -> str | None:
        """RID of the ingest job running this extractor, or None when Nominal didn't inject it."""
        return self._env.get(_INGEST_JOB_RID_ENV) or None

    @property
    def dataset_rid(self) -> str | None:
        """RID of the dataset this run ingests into, or None when Nominal didn't inject it."""
        return self._env.get(_DATASET_RID_ENV) or None

    @property
    def additional_tags(self) -> dict[str, str]:
        """Tags the ingest request applies to all data from this run; empty when Nominal didn't inject them."""
        tags = _json_env(self._env, _ADDITIONAL_TAGS_ENV)
        if tags is None:
            return {}
        if not isinstance(tags, dict):
            raise ExtractorError(f"{_ADDITIONAL_TAGS_ENV} is not a JSON object: {tags!r}")
        result: dict[str, str] = {}
        for key, value in tags.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise ExtractorError(f"{_ADDITIONAL_TAGS_ENV} must map string tag names to string values: {tags!r}")
            result[key] = value
        return result

    @property
    def job_timestamp_metadata(self) -> TimestampMetadata | None:
        """The job-level timestamp metadata this run's outputs default to.

        This is the metadata the pipeline resolved for the whole job (the ingest request's override
        when given, else the image's registered default) -- the value a manifest output falls back
        to when it declares no per-output timestamp metadata of its own. None when Nominal didn't
        inject it.
        """
        document = _json_env(self._env, _JOB_TIMESTAMP_METADATA_ENV)
        if document is None:
            return None
        try:
            decoded = ConjureDecoder().decode(document, scout_catalog.TimestampMetadata)
        except Exception as ex:
            raise ExtractorError(f"{_JOB_TIMESTAMP_METADATA_ENV} is not valid timestamp metadata: {document!r}") from ex
        return TimestampMetadata(
            series_name=decoded.series_name,
            timestamp_type=ts._catalog_timestamp_type_to_typed_timestamp_type(decoded.timestamp_type),
        )

    def _relative_output_path(self, path: str | os.PathLike[str]) -> tuple[Path, str]:
        """Validate an output file exists under ``output_dir``; return it with its relative path.

        The relative path always uses forward slashes (``as_posix()``), matching the platform's
        wire contract regardless of the host OS.
        """
        given = Path(path)
        if not given.is_file():
            raise ExtractorError(f"output file does not exist: {given}")
        try:
            relative = given.resolve().relative_to(self.output_dir.resolve())
        except ValueError as ex:
            raise ExtractorError(f"output file {given} is not inside the output directory {self.output_dir}") from ex
        return given, relative.as_posix()

    def _finalize(self) -> int:
        """Enforce the mode's output contract; returns the number of finalized outputs."""
        raise NotImplementedError


def _check_for_undeclared_output_files(output_dir: Path, declared: set[str], declare_method: str) -> None:
    """Reject files sitting in ``output_dir`` that were never declared as outputs.

    Counting only declared outputs isn't enough to catch a stray file: an author who writes two
    files but only declares one would pass a count check while still leaving two files on disk,
    reproducing the downstream multiple-files failure this module exists to catch earlier.
    """
    actual = {path.relative_to(output_dir).as_posix() for path in output_dir.rglob("*") if path.is_file()}
    undeclared = sorted(actual - declared)
    if undeclared:
        raise ExtractorError(
            f"output directory contains file(s) not passed to {declare_method}: {undeclared}; declare "
            "every file you want ingested, or remove stray files from the output directory"
        )


@dataclass
class SingleFileExtractorContext(ExtractorContext):
    """Context for :func:`single_file_extractor` functions: declare the one output via :meth:`set_output`."""

    _output_relative: str | None = field(default=None, repr=False)

    def set_output(self, path: str | os.PathLike[str]) -> Path:
        """Declare the single file you wrote to the output directory.

        Records the file (it must already exist under ``output_dir``); it does not write anything
        itself. A single-file extractor produces exactly one output, so a second call raises. Use
        :func:`manifest_extractor` (with the image registered under the ``MANIFEST`` output format)
        to emit multiple files.
        """
        if self._output_relative is not None:
            raise ExtractorError(
                f"set_output() was already called with {self._output_relative!r}; a single-file extractor "
                "produces exactly one output file. Register the image with the MANIFEST output format and "
                "use @manifest_extractor to emit multiple files"
            )
        resolved, relative = self._relative_output_path(path)
        logger.debug("declared output %s", relative)
        self._output_relative = relative
        return resolved

    def _finalize(self) -> int:
        if self._output_relative is None:
            raise ExtractorError(
                "single-file extractor produced no output; call ctx.set_output() with the file you wrote"
            )
        _check_for_undeclared_output_files(self.output_dir, {self._output_relative}, "ctx.set_output()")
        return 1


@dataclass
class ManifestExtractorContext(ExtractorContext):
    """Context for :func:`manifest_extractor` functions: declare each output via :meth:`add_output`."""

    _outputs: list[ingest_manifest.ManifestOutput] = field(default_factory=list, repr=False)

    def add_output(
        self,
        path: str | os.PathLike[str],
        *,
        ingest_type: IngestType = IngestType.TABULAR,
        tag_columns: Mapping[str, str] | None = None,
        channel_prefix: str | None = None,
        timestamp_column: str | None = None,
        timestamp_type: ts._AnyTimestampType | None = None,
    ) -> Path:
        """Declare a file you wrote to the output directory; it becomes one manifest entry.

        Records the file (it must already exist under ``output_dir``); it does not write anything
        itself. ``timestamp_column``/``timestamp_type`` (provided together) override the job-level
        timestamp metadata for this output, so each file can carry its own timestamp field. Only
        numeric epoch timestamp types (seconds through nanoseconds) are supported here; outputs
        needing richer types should omit the pair and rely on the job-level metadata. For
        ``JSON_L`` outputs each line must still contain a ``MESSAGE`` field; that path is log
        ingest.
        """
        if (timestamp_column is None) != (timestamp_type is None):
            raise ExtractorError("timestamp_column and timestamp_type must be provided together")
        resolved, relative = self._relative_output_path(path)
        if relative == _MANIFEST_FILENAME:
            raise ExtractorError(
                f"{_MANIFEST_FILENAME} is written automatically by the runtime; write your data to a "
                "different file name and declare that one with add_output() instead"
            )
        timestamp_metadata = None
        if timestamp_column is not None and timestamp_type is not None:
            timestamp_metadata = ingest_manifest.ManifestTimestampMetadata(
                series_name=timestamp_column,
                epoch_time_unit=_manifest_epoch_time_unit(timestamp_type),
            )
        logger.debug("declared output %s (%s)", relative, ingest_type.value)
        self._outputs.append(
            ingest_manifest.ManifestOutput(
                ingest_type=ingest_type._to_conjure(),
                relative_path=relative,
                tag_columns=dict(tag_columns or {}),
                channel_prefix=channel_prefix,
                timestamp_metadata=timestamp_metadata,
            )
        )
        return resolved

    def build_manifest(self) -> dict[str, Any]:
        """Build the manifest document from the declared outputs, exactly as it is written to disk."""
        document: dict[str, Any] = ConjureEncoder.do_encode(
            ingest_manifest.ExtractorManifest(outputs=list(self._outputs))
        )
        return document

    def _finalize(self) -> int:
        if not self._outputs:
            raise ExtractorError("manifest extractor produced no outputs; call ctx.add_output() for each file")
        _check_for_undeclared_output_files(
            self.output_dir, {output.relative_path for output in self._outputs}, "ctx.add_output()"
        )
        manifest_path = self.output_dir / _MANIFEST_FILENAME
        manifest_path.write_text(json.dumps(self.build_manifest()))
        logger.info("wrote %s describing %d output(s)", manifest_path, len(self._outputs))
        return len(self._outputs)


_CtxT = TypeVar("_CtxT", bound=ExtractorContext)


@dataclass
class Extractor(Generic[_CtxT]):
    """A containerized-extractor entrypoint produced by :func:`single_file_extractor` or :func:`manifest_extractor`.

    Call :meth:`run` as the container's entrypoint to drive it from the environment. In tests,
    drive it with :meth:`run` (``env=...``, ``exit=False``) rather than constructing a context by
    hand. Carries the wrapped function's metadata (``__name__``, ``__doc__``, ...) like any well-behaved
    decorator.
    """

    _fn: Callable[[_CtxT], None]
    _context_cls: type[_CtxT]

    def __post_init__(self) -> None:
        functools.update_wrapper(self, self._fn)

    def __call__(self, ctx: _CtxT) -> None:
        self._fn(ctx)

    @property
    def _is_manifest(self) -> bool:
        return issubclass(self._context_cls, ManifestExtractorContext)

    def run(self, *, env: Mapping[str, str] | None = None, exit: bool = True) -> _CtxT:
        """Run the extractor against the environment and finalize its outputs.

        Intended as the container entrypoint (``if __name__ == "__main__": my_extractor.run()``).
        On success returns the context; on failure prints a traceback and, when ``exit`` is
        True (the default), exits with a non-zero status so the ingest job fails. Pass
        ``exit=False`` to re-raise instead -- useful in tests.
        """
        environ = os.environ if env is None else env
        if env is None:
            # As the container entrypoint, make the runtime's log lines visible in the job's
            # captured output; a no-op when the author already configured logging.
            logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
        try:
            self._check_registered_format(environ)
            ctx = self._build_context(environ)
            logger.info(
                "running %s extractor %s with %d input(s)",
                "manifest" if self._is_manifest else "single-file",
                self._fn.__name__,
                len(ctx.inputs),
            )
            self._validate_registered_contract(ctx)
            self._fn(ctx)
            count = ctx._finalize()
            logger.info("extractor %s completed with %d output(s)", self._fn.__name__, count)
            return ctx
        except BaseException:  # deliberately broad: any failure (incl. SystemExit/KeyboardInterrupt
            # from user code) must fail the ingest job cleanly, not just Exception subclasses.
            if exit:
                traceback.print_exc()
                sys.exit(1)
            raise

    def _validate_registered_contract(self, ctx: _CtxT) -> None:
        """Check the registered contract against the environment once, at startup.

        Advisory only: a registered-required parameter left unset, or a registered input whose
        mounted path is missing, earns a warning at the top of the job log -- but the run
        proceeds, since only code that actually reads the affected name is impacted.
        """
        for param_spec in ctx._param_specs or []:
            if param_spec.required and ctx._env.get(param_spec.environment_variable) is None:
                logger.warning(
                    "required parameter %s (%r) has no value set; ctx.param will fail if it is read",
                    param_spec.environment_variable,
                    param_spec.name,
                )
        for input_spec in ctx._input_specs or []:
            if not Path(input_spec.path).is_file():
                logger.warning(
                    "input %s (%r) is not present at %s",
                    input_spec.environment_variable,
                    input_spec.name,
                    input_spec.path,
                )

    def _check_registered_format(self, env: Mapping[str, str]) -> None:
        """Assert the decorator's contract against the injected registered output format.

        When ``_NOMINAL_OUTPUT_FORMAT`` is absent (a local run) the decorator's word is law.
        """
        registered = env.get(_OUTPUT_FORMAT_ENV)
        if not registered:
            return
        registered_manifest = registered == FileOutputFormat.MANIFEST.value
        if registered_manifest == self._is_manifest:
            return
        declared, alternative = (
            ("@manifest_extractor", "@single_file_extractor")
            if self._is_manifest
            else ("@single_file_extractor", "@manifest_extractor")
        )
        raise ExtractorError(
            f"{declared} disagrees with the image's registered output format {registered!r} "
            f"(_NOMINAL_OUTPUT_FORMAT); re-register the image or switch to {alternative} so the "
            "code and the registration agree"
        )

    def _build_context(self, env: Mapping[str, str]) -> _CtxT:
        output_dir = env.get(_OUTPUT_DIR_ENV)
        if not output_dir:
            raise ExtractorError(f"{_OUTPUT_DIR_ENV} is not set; this code must run inside a Nominal extractor")
        input_dir = env.get(_INPUT_DIR_ENV, _DEFAULT_INPUT_DIR)
        return self._context_cls(
            output_dir=Path(output_dir),
            _env=env,
            _input_dir=Path(input_dir),
            _input_specs=_parse_input_specs(env),
            _param_specs=_parse_param_specs(env),
        )


def single_file_extractor(fn: Callable[[SingleFileExtractorContext], None]) -> Extractor[SingleFileExtractorContext]:
    """Turn ``def fn(ctx: SingleFileExtractorContext) -> None`` into a single-file extractor entrypoint.

    For images registered with a single-file output format (``PARQUET``, ``CSV``, ...): the ingest
    pipeline ingests exactly one output file, parsed per the registered format. Declare it with
    :meth:`SingleFileExtractorContext.set_output`. If the image's registered format turns out to be
    ``MANIFEST``, :meth:`Extractor.run` fails at startup with a clear error.

    Example::

        from nominal.experimental.extractor import SingleFileExtractorContext, single_file_extractor

        @single_file_extractor
        def convert(ctx: SingleFileExtractorContext) -> None:
            table = read_input(ctx.input())
            out = ctx.output_dir / "converted.parquet"
            write_parquet(table, out)
            ctx.set_output(out)

        if __name__ == "__main__":
            convert.run()
    """
    return Extractor(fn, SingleFileExtractorContext)


def manifest_extractor(fn: Callable[[ManifestExtractorContext], None]) -> Extractor[ManifestExtractorContext]:
    """Turn ``def fn(ctx: ManifestExtractorContext) -> None`` into a manifest extractor entrypoint.

    For images registered with the ``MANIFEST`` output format: declare each output file (and its
    per-file ingest type, tag columns, channel prefix, and optional epoch timestamp metadata) with
    :meth:`ManifestExtractorContext.add_output`; ``manifest.json`` is written automatically when
    the function returns. If the image's registered format is not ``MANIFEST``, :meth:`Extractor.run`
    fails at startup with a clear error.

    Example::

        from nominal.experimental.extractor import IngestType, ManifestExtractorContext, manifest_extractor

        @manifest_extractor
        def split(ctx: ManifestExtractorContext) -> None:
            table = read_parquet(ctx.input())
            for i, chunk in enumerate(chunks_of(table, int(ctx.get_param("PARTS", "2")))):
                out = ctx.output_dir / f"part_{i}.parquet"
                write_parquet(chunk, out)
                ctx.add_output(out, ingest_type=IngestType.TABULAR)

        if __name__ == "__main__":
            split.run()
    """
    return Extractor(fn, ManifestExtractorContext)
