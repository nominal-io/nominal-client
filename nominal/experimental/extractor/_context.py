"""The execution contexts an extractor function receives."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Mapping, Sequence, overload

from conjure_python_client import ConjureDecoder, ConjureEncoder
from nominal_api import ingest_manifest, scout_catalog, scout_video_api

from nominal import ts
from nominal.core._video_types import _scale_parameter
from nominal.core.container_image import TimestampMetadata
from nominal.core.exceptions import ExtractorError, NominalVideoTimestampModeError
from nominal.core.filetype import FileType
from nominal.experimental.extractor._env import (
    _ADDITIONAL_TAGS_ENV,
    _DATASET_RID_ENV,
    _INGEST_JOB_RID_ENV,
    _JOB_TIMESTAMP_METADATA_ENV,
    _find_spec,
    _InputSpec,
    _json_env,
    _ParamSpec,
    _spec_names,
)
from nominal.experimental.extractor._manifest import IngestType, _manifest_timestamp_metadata

logger = logging.getLogger(__name__)

# The well-known name the ingest pipeline reads from the output directory; the runtime writes it.
_MANIFEST_FILENAME = "manifest.json"


@dataclass
class ExtractorContext:
    """The execution context handed to an extractor function.

    Resolves inputs and parameters from the environment, and collects the output files the
    function writes. Authors do not construct this directly; :meth:`Extractor.run` builds a
    :class:`SingleFileExtractorContext` or :class:`ManifestExtractorContext`.
    """

    # The declaration method authors call in this mode, named in the stray-file warning. Set per subclass.
    _declare_method: ClassVar[str]

    output_dir: Path
    _env: Mapping[str, str] = field(repr=False)
    _input_dir: Path = field(repr=False)
    _input_specs: list[_InputSpec] | None = field(default=None, repr=False)
    _param_specs: list[_ParamSpec] | None = field(default=None, repr=False)
    # Every declared output's POSIX path relative to output_dir, recorded by _declare once a
    # declaration succeeds, so the stray-file warning never re-derives it from mode-specific state.
    _declared: set[str] = field(default_factory=set, repr=False)

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
        """RID of the ingest job running this extractor.

        None when Nominal injected no value, empty included -- an empty environment variable means the
        same thing as an absent one here, as it does everywhere else in this contract.
        """
        return self._env.get(_INGEST_JOB_RID_ENV) or None

    @property
    def dataset_rid(self) -> str | None:
        """RID of the dataset this run ingests into.

        None when Nominal injected no value, empty included; see :attr:`ingest_job_rid`.
        """
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
        wire contract regardless of the host OS. Resolving a path does not declare it -- declaration
        methods call :meth:`_declare` once every one of their own checks has passed.
        """
        given = Path(path)
        if not given.is_file():
            raise ExtractorError(f"output file does not exist: {given}")
        try:
            relative = given.resolve().relative_to(self.output_dir.resolve())
        except ValueError as ex:
            raise ExtractorError(f"output file {given} is not inside the output directory {self.output_dir}") from ex
        return given, relative.as_posix()

    def _declare(self, relative: str) -> None:
        """Record ``relative`` as an accounted-for output file.

        Called only once a declaration has fully succeeded: a rejected file must not count as
        declared, or an author who catches the error and carries on would leave it on disk, absent
        from the manifest, with the stray-file warning below staying silent about it.

        Declaring one file more than once is allowed and deliberate -- the same table ingested under
        two timestamp columns, or as two ingest types, is a real thing to want. A set makes the
        repeat a no-op here, and each declaration still becomes its own manifest entry.
        """
        self._declared.add(relative)

    def _warn_about_stray_files(self) -> None:
        """Warn about files sitting in ``output_dir`` that were never declared as outputs.

        The ingest pipeline reads only what the manifest names, so an undeclared file is written for
        nothing -- most often a file the author meant to declare and didn't. Advisory rather than
        fatal: an extractor with a legitimate reason to leave a scratch file behind should still be
        able to finish, and the author is better placed than the runtime to judge which it is.
        """
        actual = {path.relative_to(self.output_dir).as_posix() for path in self.output_dir.rglob("*") if path.is_file()}
        undeclared = sorted(actual - self._declared)
        if undeclared:
            logger.warning(
                "output directory contains file(s) not passed to %s: %s; these will not be ingested",
                self._declare_method,
                undeclared,
            )

    def _finalize(self) -> int:
        """Enforce the mode's output contract; returns the number of finalized outputs."""
        raise NotImplementedError


@dataclass
class SingleFileExtractorContext(ExtractorContext):
    """Context for :func:`single_file_extractor` functions: declare the one output via :meth:`set_output`."""

    _declare_method: ClassVar[str] = "ctx.set_output()"

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
        self._declare(relative)
        return resolved

    def _finalize(self) -> int:
        if self._output_relative is None:
            raise ExtractorError(
                "single-file extractor produced no output; call ctx.set_output() with the file you wrote"
            )
        self._warn_about_stray_files()
        return 1


@dataclass
class ManifestExtractorContext(ExtractorContext):
    """Context for :func:`manifest_extractor` functions.

    One declaration method per output format: :meth:`add_tabular`, :meth:`add_avro_stream`,
    :meth:`add_journal_json`, :meth:`add_video`. Each exposes only the options its format actually
    uses.
    """

    _declare_method: ClassVar[str] = "an add_* method"

    _outputs: list[ingest_manifest.ManifestOutput] = field(default_factory=list, repr=False)
    _video_outputs: list[ingest_manifest.ManifestVideoOutput] = field(default_factory=list, repr=False)

    def _relative_declarable_path(self, path: str | os.PathLike[str]) -> tuple[Path, str]:
        """Resolve a path a declaration method may claim; return it with its relative path.

        ``manifest.json`` belongs to the runtime, which writes it from the declared entries -- a
        property of manifest mode rather than of any one declaration method, so the rule lives here
        and every method that resolves a path gets it.
        """
        resolved, relative = self._relative_output_path(path)
        if relative == _MANIFEST_FILENAME:
            raise ExtractorError(
                f"{_MANIFEST_FILENAME} is written by the runtime, not declared; write your output to a "
                "different file name and declare that one instead"
            )
        return resolved, relative

    def add_tabular(
        self,
        path: str | os.PathLike[str],
        *,
        tag_columns: Mapping[str, str] | None = None,
        channel_prefix: str | None = None,
        timestamp_column: str | None = None,
        timestamp_type: ts._AnyTimestampType | None = None,
    ) -> Path:
        """Declare a CSV or Parquet file you wrote; its columns become channels.

        ``tag_columns`` maps tag names to the columns carrying their values. ``channel_prefix`` is
        prepended to every channel from this file. ``timestamp_column``/``timestamp_type`` (provided
        together) override the job-level timestamp metadata for this output, so each file can carry
        its own timestamp column; only numeric types work here -- absolute epochs
        (:class:`ts.Epoch`) or offsets from a starting time (:class:`ts.Relative`). Outputs needing
        ISO 8601 or custom formats omit the pair and inherit the job-level metadata, which supports
        the full range.
        """
        resolved, relative = self._relative_declarable_path(path)
        FileType.from_path_dataset(resolved)
        return self._record_output(
            resolved,
            relative,
            IngestType.TABULAR,
            tag_columns=tag_columns,
            channel_prefix=channel_prefix,
            timestamp_column=timestamp_column,
            timestamp_type=timestamp_type,
        )

    def add_avro_stream(self, path: str | os.PathLike[str], *, channel_prefix: str | None = None) -> Path:
        """Declare an avro-stream file you wrote (``.avro`` or ``.avro.gz``).

        Avro records carry their own channel, timestamp, value, and tags, so this takes no tag
        columns and no timestamp metadata -- the pipeline reads all of that from the records
        themselves. ``channel_prefix`` is still prepended to every channel from this file.
        """
        resolved, relative = self._relative_declarable_path(path)
        FileType.from_avro_stream(resolved)
        return self._record_output(resolved, relative, IngestType.AVRO_STREAM, channel_prefix=channel_prefix)

    def add_journal_json(
        self,
        path: str | os.PathLike[str],
        *,
        timestamp_column: str | None = None,
        timestamp_type: ts._AnyTimestampType | None = None,
    ) -> Path:
        """Declare a journal JSONL file you wrote (``.jsonl`` or ``.jsonl.gz``); it is ingested as logs.

        Each line must carry a ``MESSAGE`` field. ``timestamp_column``/``timestamp_type`` (provided
        together) name the top-level JSON field holding each line's timestamp, overriding the
        job-level metadata; the same numeric-only restriction as :meth:`add_tabular` applies.

        Log samples carry no tags and every log point lands on one channel, so this takes neither
        tag columns nor a channel prefix -- the ingest pipeline ignores both for log outputs.
        """
        resolved, relative = self._relative_declarable_path(path)
        FileType.from_path_journal_json(resolved)
        return self._record_output(
            resolved,
            relative,
            IngestType.JSON_L,
            timestamp_column=timestamp_column,
            timestamp_type=timestamp_type,
        )

    def _record_output(
        self,
        resolved: Path,
        relative: str,
        ingest_type: IngestType,
        *,
        tag_columns: Mapping[str, str] | None = None,
        channel_prefix: str | None = None,
        timestamp_column: str | None = None,
        timestamp_type: ts._AnyTimestampType | None = None,
    ) -> Path:
        """Record one manifest entry, shared by the per-format declaration methods.

        Each of those exposes only the fields its ingest type actually reads, so this takes the
        union and trusts its callers not to pass one that would be silently dropped.
        """
        if (timestamp_column is None) != (timestamp_type is None):
            raise ExtractorError("timestamp_column and timestamp_type must be provided together")
        timestamp_metadata = None
        if timestamp_column is not None and timestamp_type is not None:
            timestamp_metadata = _manifest_timestamp_metadata(timestamp_column, timestamp_type)
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
        self._declare(relative)
        return resolved

    @overload
    def add_video(
        self,
        path: str | os.PathLike[str],
        *,
        channel: str,
        start: ts._InferrableTimestampType,
        ending_timestamp: ts._InferrableTimestampType | None = ...,
        true_frame_rate: float | None = ...,
        scale_factor: float | None = ...,
    ) -> Path: ...

    @overload
    def add_video(
        self,
        path: str | os.PathLike[str],
        *,
        channel: str,
        frame_timestamps: Sequence[ts.IntegralNanosecondsUTC],
    ) -> Path: ...

    def add_video(
        self,
        path: str | os.PathLike[str],
        *,
        channel: str,
        start: ts._InferrableTimestampType | None = None,
        frame_timestamps: Sequence[ts.IntegralNanosecondsUTC] | None = None,
        ending_timestamp: ts._InferrableTimestampType | None = None,
        true_frame_rate: float | None = None,
        scale_factor: float | None = None,
    ) -> Path:
        """Declare a video you wrote to the output directory; it becomes one video manifest entry.

        The video is ingested as ``channel`` on the dataset this run writes to, alongside any
        telemetry outputs. Exactly one of ``start`` or ``frame_timestamps`` is required, and they fix
        the video's absolute time two different ways:

        - ``start`` -- the video's absolute starting timestamp; each frame's time comes from the
          video's own encoded presentation timestamps, offset from that start. Pass one of
          ``ending_timestamp``, ``true_frame_rate``, or ``scale_factor`` (at most one) when the media
          plays at a different rate than the camera recorded at.
        - ``frame_timestamps`` -- one absolute nanosecond timestamp per frame, when precise per-frame
          metadata is available. Unlike every other declaration method, this one *writes*: the runtime
          serializes the timestamps to a sidecar beside the video (``cam.mp4`` gets
          ``cam.mp4.timestamps.json``) and declares it for you, so authors never have to reproduce
          that file's format.

        The video file itself must already exist under ``output_dir`` and carry a supported video
        extension.

        Rejections come back as :class:`ExtractorError` when they are about the extractor's own
        contract -- a reserved file name, a file outside the output directory -- and as the argument
        errors the rest of the client raises (all :class:`ValueError` subclasses) when the arguments
        themselves are malformed, which is what :meth:`Dataset.add_video` does too.

        NOTE: video outputs require a recent version of the Nominal platform. An older ingest
        pipeline ignores them, and rejects a manifest whose only outputs are videos.
        """
        if not channel:
            raise ExtractorError("channel must be a non-empty channel name for the video")
        if (start is None) == (frame_timestamps is None):
            raise NominalVideoTimestampModeError()

        # Resolving first means declaring manifest.json reports the collision rather than
        # complaining that .json is not a video container.
        resolved, relative = self._relative_declarable_path(path)
        FileType.from_video(resolved)

        if frame_timestamps is not None:
            if not frame_timestamps:
                raise ExtractorError("frame_timestamps must contain at least one timestamp")
            if any(value is not None for value in (ending_timestamp, true_frame_rate, scale_factor)):
                raise ExtractorError(
                    "'ending_timestamp', 'true_frame_rate', and 'scale_factor' apply only to 'start'; per-frame "
                    "timestamps already fix every frame's absolute time"
                )
            timestamp_manifest = ingest_manifest.VideoTimestampManifest(
                frame_timestamps_relative_path=self._write_frame_timestamps(resolved, relative, frame_timestamps)
            )
        elif start is not None:
            scale_parameter = _scale_parameter(
                ending_timestamp=ending_timestamp, true_frame_rate=true_frame_rate, scale_factor=scale_factor
            )
            timestamp_manifest = ingest_manifest.VideoTimestampManifest(
                no_manifest=scout_video_api.NoTimestampManifest(
                    starting_timestamp=ts._SecondsNanos.from_flexible(start).to_api(),
                    scale_parameter=scale_parameter,
                )
            )
        else:  # unreachable: the check above admits exactly one of the two arms
            raise NominalVideoTimestampModeError()

        logger.debug("declared video %s on channel %s", relative, channel)
        self._video_outputs.append(
            ingest_manifest.ManifestVideoOutput(
                relative_path=relative, channel=channel, timestamp_manifest=timestamp_manifest
            )
        )
        self._declare(relative)
        return resolved

    def _write_frame_timestamps(
        self, video: Path, video_relative: str, frame_timestamps: Sequence[ts.IntegralNanosecondsUTC]
    ) -> str:
        """Serialize per-frame timestamps beside ``video``; returns the sidecar's relative path.

        Named after the video so a listing says which one a sidecar belongs to, keeping the full name
        with its extension so ``cam.mp4`` and ``cam.mkv`` do not collide. Declaring one video
        repeatedly is allowed and only the repeats need telling apart, so the first sidecar takes the
        plain name and the nth gains a ``.n`` before the extension. The count comes from the entries
        already declared for that path, so it is deterministic without consulting the filesystem.
        """
        already_declared = sum(1 for output in self._video_outputs if output.relative_path == video_relative)
        suffix = ".timestamps.json" if not already_declared else f".timestamps.{already_declared}.json"
        sidecar = video.with_name(f"{video.name}{suffix}")
        if sidecar.exists():
            # Only declared outputs belong in the output directory, so whatever is sitting here is
            # already a contract violation -- but clobbering it silently would lose it.
            raise ExtractorError(
                f"cannot write the frame timestamp sidecar for {video_relative}: {sidecar.name} already exists "
                "in the output directory; the runtime writes that file, so remove it or rename the video"
            )
        sidecar.write_text(json.dumps(list(frame_timestamps)))
        _, relative = self._relative_output_path(sidecar)
        self._declare(relative)
        return relative

    def build_manifest(self) -> dict[str, Any]:
        """Build the manifest document from the declared outputs, exactly as it is written to disk."""
        document: dict[str, Any] = ConjureEncoder.do_encode(
            ingest_manifest.ExtractorManifest(outputs=list(self._outputs), video_outputs=list(self._video_outputs))
        )
        return document

    def _finalize(self) -> int:
        if not self._outputs and not self._video_outputs:
            raise ExtractorError(
                "manifest extractor produced no outputs; declare each file you wrote with the add_* method "
                "for its format"
            )
        self._warn_about_stray_files()
        manifest_path = self.output_dir / _MANIFEST_FILENAME
        manifest_path.write_text(json.dumps(self.build_manifest()))
        logger.info(
            "wrote %s describing %d output(s) and %d video(s)",
            manifest_path,
            len(self._outputs),
            len(self._video_outputs),
        )
        return len(self._outputs) + len(self._video_outputs)
