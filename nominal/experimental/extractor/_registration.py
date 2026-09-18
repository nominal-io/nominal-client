"""Project extractor definitions into SDK and catalog registration contracts."""

from __future__ import annotations

import re
from typing import Any, Mapping, TypedDict

from nominal import ts
from nominal.core.container_image import FileExtractionInput, FileExtractionParameter, FileOutputFormat
from nominal.experimental.extractor._arguments import _Input, _Parameter
from nominal.experimental.extractor._definition import _Definition
from nominal.experimental.extractor._errors import _ErrorMapping


class _RegistrationKwargs(TypedDict):
    """Describe the exact keyword arguments exported to register_image.

    This dictionary contract lets type checkers validate **kwargs at the existing SDK boundary
    without introducing another registration object or losing field types to dict[str, Any].
    """

    inputs: list[FileExtractionInput]
    parameters: list[FileExtractionParameter]
    output_format: FileOutputFormat
    default_timestamp_column: str
    default_timestamp_type: ts._AnyTimestampType


def _registration_kwargs(definition: _Definition) -> _RegistrationKwargs:
    """Project a definition into the keyword contract accepted by register_image."""
    if definition.timestamp_column is None or definition.timestamp_type is None:
        raise ValueError("declare default_timestamp_column and default_timestamp_type to generate registration")
    if definition.output_format is None:
        raise ValueError("declare output_format on @single_file_extractor to generate registration")
    return {
        "inputs": [argument.spec for argument in definition.arguments if isinstance(argument, _Input)],
        "parameters": [argument.spec for argument in definition.arguments if isinstance(argument, _Parameter)],
        "output_format": definition.output_format,
        "default_timestamp_column": definition.timestamp_column,
        "default_timestamp_type": definition.timestamp_type,
    }


_RESERVED_CODES = frozenset(
    {
        "IMAGE_PULL_FAILED",
        "EXTRACTOR_TIMEOUT",
        "EXTRACTOR_OOM_KILLED",
        "OUTPUT_UPLOAD_FAILED",
        "INVALID_OUTPUT",
        "UNKNOWN",
    }
)


def _text(value: str | None, field: str, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"catalog {field} must be nonempty and at most {maximum} characters")
    return value


def _environment(variable: str) -> str:
    if re.fullmatch(r"[A-Z][A-Z0-9_]*", variable) is None or variable.startswith("_NOMINAL_"):
        raise ValueError(f"invalid catalog environment variable {variable!r}")
    return variable


def _catalog_manifest(
    definition: _Definition,
    *,
    id: str,
    version: str,
    display_name: str,
    description: str,
) -> dict[str, Any]:
    """Project declared runtime metadata into the catalog schema, rejecting lossy exports."""
    _text(id, "id", 64)
    if re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", id) is None:
        raise ValueError("catalog id must contain lowercase letters, digits, and separating hyphens")
    if re.fullmatch(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", version) is None:
        raise ValueError("catalog version must be major.minor.patch without leading zeros")
    _text(display_name, "display_name", 128)
    _text(description, "description", 1024)
    registration = _registration_kwargs(definition)
    column = _text(registration["default_timestamp_column"], "timestamp column", 128)
    timestamp = ts._to_typed_timestamp_type(registration["default_timestamp_type"])
    if isinstance(timestamp, ts.Epoch):
        timestamp_type = {
            "absolute": {"epoch_of_time_unit": {"time_unit": ts._time_unit_to_conjure(timestamp.unit).value}}
        }
    elif isinstance(timestamp, ts.Iso8601):
        timestamp_type = {"absolute": {"iso8601": {}}}
    else:
        # Relative carries a fixed origin and Custom carries parsing rules: the catalog
        # schema cannot represent either completely. Never silently drop those fields.
        raise ValueError(
            "catalog timestamp defaults must be epoch or ISO 8601; use per-ingest overrides for other types"
        )

    if not registration["inputs"]:
        raise ValueError("catalog requires at least one declared input")
    inputs = []
    for spec in registration["inputs"]:
        if not spec.file_suffixes or any(
            re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.]*", suffix) is None for suffix in spec.file_suffixes
        ):
            raise ValueError(f"catalog input {spec.name!r} requires valid suffix filters without a leading dot")
        inputs.append(
            {
                "environment_variable": _environment(spec.environment_variable),
                "file_filters": [{"suffix": suffix} for suffix in spec.file_suffixes],
                "required": spec.required,
            }
        )
    parameters = []
    for parameter_spec in registration["parameters"]:
        parameter: dict[str, Any] = {
            "environment_variable": _environment(parameter_spec.environment_variable),
            "name": parameter_spec.name,
            "required": parameter_spec.required,
        }
        if parameter_spec.description is not None:
            if len(parameter_spec.description) > 512:
                raise ValueError(f"catalog parameter {parameter_spec.name!r} description exceeds 512 characters")
            parameter["description"] = parameter_spec.description
        parameters.append(parameter)

    return {
        "id": id,
        "version": version,
        "display_name": display_name,
        "description": description,
        "inputs": inputs,
        "parameters": parameters,
        "output_file_format": registration["output_format"].value,
        "default_timestamp_metadata": {"series_name": column, "timestamp_type": timestamp_type},
        "exit_code_mappings": _error_fallbacks(definition.errors),
    }


def _error_fallbacks(errors: Mapping[type[Exception], _ErrorMapping]) -> list[dict[str, Any]]:
    """A process exit code must identify exactly one catalog fallback."""
    fallbacks: dict[int, dict[str, Any]] = {}
    for mapping in errors.values():
        if mapping.code in _RESERVED_CODES:
            raise ValueError(f"catalog error code {mapping.code!r} is platform-reserved")
        if re.fullmatch(r"[A-Z][A-Z0-9_]*", mapping.code) is None:
            raise ValueError(f"invalid catalog error code {mapping.code!r}")
        fallback = {
            "exit_code": mapping.exit_code,
            "code": mapping.code,
            "message": _text(mapping.message, f"error {mapping.code!r} message", 512),
            "retryable": mapping.retryable,
        }
        if mapping.exit_code in fallbacks and fallbacks[mapping.exit_code] != fallback:
            raise ValueError(f"conflicting catalog fallbacks for exit code {mapping.exit_code}")
        fallbacks[mapping.exit_code] = fallback
    return [fallbacks[code] for code in sorted(fallbacks)]
