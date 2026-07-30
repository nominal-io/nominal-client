"""The environment contract Nominal establishes for an extractor container."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping, TypeVar

from nominal.core.exceptions import ExtractorError

# Mirrors the mount/env contract the Nominal ingest pipeline establishes for the customer
# container.
_DEFAULT_INPUT_DIR = "/input"
_OUTPUT_DIR_ENV = "OUTPUT_DIR"

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


def _json_env(env: Mapping[str, str], var: str) -> Any:
    """Parse a JSON-valued environment variable, or None when it is absent/empty."""
    raw = env.get(var)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError as ex:
        raise ExtractorError(f"{var} is not valid JSON: {raw!r}") from ex


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
