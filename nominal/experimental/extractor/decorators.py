"""Declare an extractor's inputs, settings, failures, and output contract.

Place ``@manifest_extractor`` (or ``@single_file_extractor``) outermost. Add
``@input``, ``@parameter``, and ``@error`` beneath it; the runner uses those declarations
both to invoke your callback and to describe the image for registration.
"""

from __future__ import annotations

import builtins
from typing import Callable, Sequence, overload

from nominal import ts
from nominal.core.container_image import FileExtractionInput, FileExtractionParameter, FileOutputFormat
from nominal.experimental.extractor._arguments import _Input, _Parameter
from nominal.experimental.extractor._definition import _MISSING, _Missing, _names
from nominal.experimental.extractor._errors import _ErrorMapping
from nominal.experimental.extractor.context import ManifestExtractorContext, SingleFileExtractorContext
from nominal.experimental.extractor.runner import Extractor
from nominal.experimental.extractor.types import _parameter_converter

__all__ = ["input", "parameter", "error", "manifest_extractor", "single_file_extractor"]


def input(
    argument: str,
    *,
    envvar: str | None = None,
    name: str | None = None,
    description: str | None = None,
    file_suffixes: Sequence[str] = (),
    default: None | _Missing = _MISSING,
) -> _Input:
    """Declare a file your callback needs and receive its mounted path.

    For example, ``@input("recording", envvar="SOURCE", file_suffixes=["csv"])``
    supplies the callback's ``recording: Path`` argument and describes that input for
    registration. Use ``default=None`` and ``Path | None`` for an optional file.

    Place below the outer extractor decorator. The named callback argument must accept a
    keyword value and have no signature default. Annotations describe the received value;
    they do not control binding. Registered metadata is authoritative when present; otherwise
    the path comes from the declared environment variable. Lookup matches environment
    variables exactly, not display names.

    Args:
        argument: Name of the callback argument to populate.
        envvar: Input environment variable; defaults to the uppercase argument name.
        name: Registration display name; defaults to the argument name.
        description: Optional registration description.
        file_suffixes: Registration filters such as ["csv", "mcap"]; empty accepts any suffix.
            These do not validate the local file's extension.
        default: Omit to require the input, or use None to allow an absent file.
            Other defaults are unsupported.

    Raises:
        ValueError: A name is invalid or reserved, or the default is unsupported.
        ExtractorError: At binding time, a required input is absent or a supplied path
            does not exist as a file. All inputs resolve before the callback runs.
    """
    if default is not _MISSING and default is not None:
        raise ValueError("input default must be None")
    variable, display = _names(argument, envvar, name)
    return _Input(
        argument,
        FileExtractionInput(
            name=display,
            environment_variable=variable,
            description=description,
            file_suffixes=tuple(file_suffixes),
            required=default is _MISSING,
        ),
    )


def parameter(
    argument: str,
    *,
    envvar: str | None = None,
    name: str | None = None,
    description: str | None = None,
    type: Callable[[str], object] | _Missing = _MISSING,
    default: object = _MISSING,
) -> _Parameter:
    """Declare a setting so your callback receives a usable Python value.

    For example, ``@parameter("parts", type=IntRange(min=1), default=2)`` supplies
    the callback's ``parts: int`` argument. Omission gives 2; a supplied value is converted
    and checked before extraction starts. This also declares the setting for registration.

    Place below the outer extractor decorator, with no default on the callback argument.
    Binding happens before extraction and uses the runner's normal error mappings. Registered
    parameter metadata is authoritative: an unknown parameter fails even if it has a default.
    Registration stores names, descriptions, environment variables, and requiredness;
    converters and default values remain runtime settings.

    Args:
        argument: Name of the explicit callback argument to populate.
        envvar: Parameter environment variable; defaults to the uppercase argument name.
        name: Registration display name; defaults to the argument name.
        description: Optional registration description.
        type: Converter for supplied strings. When omitted, infer str/int/float/bool from
            a concrete default, otherwise use str. Explicit converters take precedence;
            annotations do not select conversion. Boolean conversion accepts true/false,
            yes/no, on/off, and 1/0 case-insensitively. Float values must be finite.
        default: Already-converted Python value used when absent, including None; omit to
            require the parameter. Built-in types and constraints validate concrete defaults.
            Custom converters are never invoked on defaults. An empty supplied string
            is a value, not a request for the default.

    Raises:
        ValueError: A name is invalid or reserved, or a constrained default is invalid.
        TypeError: The converter is not callable or a default has an incompatible type.
        ExtractorError: At binding time, a parameter is missing or unregistered, or conversion
            raises ValueError or TypeError. Those converter messages and values are suppressed,
            except BadParameter, whose author-controlled diagnostic is included.

    Other converter exceptions propagate unchanged to the runner, including their messages
    in tracebacks and mapped termination reports. Wrap third-party failures in ValueError
    or TypeError to sanitize them, or BadParameter with a message safe to display.
    """
    variable, display = _names(argument, envvar, name)
    if type is _MISSING:
        inferred = builtins.type(default)
        type = inferred if inferred in (str, int, float, bool) else str
    converter = _parameter_converter(type, None if default is _MISSING else default)
    return _Parameter(
        argument,
        FileExtractionParameter(
            name=display,
            environment_variable=variable,
            description=description,
            required=default is _MISSING,
        ),
        converter,
        default,
    )


def error(
    exception_type: type[Exception],
    *,
    code: str,
    exit_code: int,
    retryable: bool = False,
    message: str | None = None,
) -> _ErrorMapping:
    """Declare a structured failure handled by the extractor runner.

    Place below ``@manifest_extractor`` or ``@single_file_extractor``, alongside ``@input``
    and ``@parameter``. Stack declarations for different Exception subclasses; the closest
    class in the raised exception's MRO wins, regardless of decorator order. Declaring the
    same class twice is an error. This decorator preserves the callback's signature.

    ``code`` is a nonempty catalog error code; ``exit_code`` is an integer from 1 through 255.
    ``retryable`` describes whether another attempt may succeed without changing the input.
    Codes must fit the 4,096-byte termination JSON envelope. Long messages are shortened;
    mapped failures retain their full traceback on stderr.

    Mappings cover startup, argument binding, extraction, and output finalization. They
    configure runtime reporting. ``message`` supplies the static catalog fallback text
    required by :meth:`Extractor.catalog_manifest`; runtime messages come from the exception.
    Direct image registration does not export error policies.
    ``run(exit=False)`` and direct callback invocation propagate errors without reporting.
    """
    return _ErrorMapping(exception_type, code, exit_code, retryable, message)


@overload
def single_file_extractor(
    fn: Callable[..., None],
    *,
    default_timestamp_column: str | None = None,
    default_timestamp_type: ts._AnyTimestampType | None = None,
    output_format: FileOutputFormat | None = None,
) -> Extractor[SingleFileExtractorContext]: ...


@overload
def single_file_extractor(
    *,
    default_timestamp_column: str | None = None,
    default_timestamp_type: ts._AnyTimestampType | None = None,
    output_format: FileOutputFormat | None = None,
) -> Callable[[Callable[..., None]], Extractor[SingleFileExtractorContext]]: ...


def single_file_extractor(
    fn: Callable[..., None] | None = None,
    *,
    default_timestamp_column: str | None = None,
    default_timestamp_type: ts._AnyTimestampType | None = None,
    output_format: FileOutputFormat | None = None,
) -> Extractor[SingleFileExtractorContext] | Callable[[Callable[..., None]], Extractor[SingleFileExtractorContext]]:
    """Declare a single-file extractor with ``ctx`` followed by injected arguments.

    Use ``@error(ExceptionType, code="...", exit_code=64)`` below this decorator to declare
    structured failures handled automatically by :meth:`Extractor.run`.
    Declare inputs and parameters below this decorator with ``@input`` and ``@parameter``;
    they are passed as keyword arguments after ``ctx``. Declare ``default_timestamp_column``
    and ``default_timestamp_type`` to generate :meth:`Extractor.registration_kwargs`.

    For images registered with a single-file output format (``PARQUET``, ``CSV``, ...): the ingest
    pipeline ingests exactly one output file, parsed per the registered format. Declare it with
    :meth:`SingleFileExtractorContext.set_output`. If the image's registered format turns out to be
    ``MANIFEST``, :meth:`Extractor.run` fails at startup with a clear error.

    Set ``output_format`` explicitly to generate registration metadata; it must then match
    the registered format exactly. Omitting it preserves legacy runtime behavior.

    This is the original output contract, kept for images already registered against it. New
    extractors should use :func:`manifest_extractor`, a strict superset: this mode has no per-output
    timestamp, tag-column, or channel-prefix control, and changing an image's output format later
    requires registering a new image.

    Example::

        from pathlib import Path

        from nominal.core.container_image import FileOutputFormat
        from nominal.experimental.extractor import SingleFileExtractorContext, input, single_file_extractor

        @single_file_extractor(
            output_format=FileOutputFormat.PARQUET,
            default_timestamp_column="time_us",
            default_timestamp_type="epoch_microseconds",
        )
        @input("source")
        def convert(ctx: SingleFileExtractorContext, *, source: Path) -> None:
            table = read_input(source)
            out = ctx.output_dir / "converted.parquet"
            write_parquet(table, out)
            ctx.set_output(out)

        if __name__ == "__main__":
            convert.run()
    """

    def decorate(function: Callable[..., None]) -> Extractor[SingleFileExtractorContext]:
        return Extractor(
            function,
            SingleFileExtractorContext,
            _output_format=output_format,
            _timestamp_column=default_timestamp_column,
            _timestamp_type=default_timestamp_type,
        )

    return decorate if fn is None else decorate(fn)


@overload
def manifest_extractor(
    fn: Callable[..., None],
    *,
    default_timestamp_column: str | None = None,
    default_timestamp_type: ts._AnyTimestampType | None = None,
) -> Extractor[ManifestExtractorContext]: ...


@overload
def manifest_extractor(
    *,
    default_timestamp_column: str | None = None,
    default_timestamp_type: ts._AnyTimestampType | None = None,
) -> Callable[[Callable[..., None]], Extractor[ManifestExtractorContext]]: ...


def manifest_extractor(
    fn: Callable[..., None] | None = None,
    *,
    default_timestamp_column: str | None = None,
    default_timestamp_type: ts._AnyTimestampType | None = None,
) -> Extractor[ManifestExtractorContext] | Callable[[Callable[..., None]], Extractor[ManifestExtractorContext]]:
    """Turn a parsing function into a manifest extractor, recommended for new images.

    Declare the callback's files and settings with ``@input`` and ``@parameter`` below
    this decorator. The framework resolves them before invoking the callback; ``ctx``
    supplies output paths and job metadata. Add ``@error`` for expected failure mappings.

    Write files under ``ctx.output_dir`` and declare each with
    :meth:`ManifestExtractorContext.add_tabular`,
    :meth:`~ManifestExtractorContext.add_avro_stream`,
    :meth:`~ManifestExtractorContext.add_journal_json`, or
    :meth:`~ManifestExtractorContext.add_video`. One run can produce several files with
    different timestamps, tags, and channel prefixes. :meth:`Extractor.run` writes
    ``manifest.json`` after the callback returns.

    Supply both timestamp defaults here to reuse the declarations through
    :meth:`Extractor.registration_kwargs` or :meth:`Extractor.catalog_manifest`.
    They describe image defaults; per-output timestamp settings belong on the context's
    output methods. Register the image with ``MANIFEST``; the runner rejects a different
    injected output format at startup.

    Example::

        from pathlib import Path

        from nominal.experimental.extractor import ManifestExtractorContext, input, manifest_extractor, parameter

        @manifest_extractor(
            default_timestamp_column="time_us",
            default_timestamp_type="epoch_microseconds",
        )
        @input("source")
        @parameter("parts", type=int, default=2)
        def split(ctx: ManifestExtractorContext, *, source: Path, parts: int) -> None:
            table = read_parquet(source)
            for i, chunk in enumerate(chunks_of(table, parts)):
                out = ctx.output_dir / f"part_{i}.parquet"
                write_parquet(chunk, out)
                ctx.add_tabular(out, timestamp_column="time_us", timestamp_type="epoch_microseconds")

            footage = ctx.output_dir / "camera.mp4"
            write_video(footage)
            ctx.add_video(footage, channel="camera/front", start=recording_started_at)

        if __name__ == "__main__":
            split.run()
    """

    def decorate(function: Callable[..., None]) -> Extractor[ManifestExtractorContext]:
        return Extractor(
            function,
            ManifestExtractorContext,
            _output_format=FileOutputFormat.MANIFEST,
            _timestamp_column=default_timestamp_column,
            _timestamp_type=default_timestamp_type,
        )

    return decorate if fn is None else decorate(fn)
