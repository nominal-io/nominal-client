from __future__ import annotations

from typing import Any

import click
import pytest

from nominal.cli.container import _parse_config
from nominal.core.container_image import FileExtractionInput, FileExtractionParameter, FileOutputFormat


def test_valid_config_parses_into_sdk_types() -> None:
    """A full config parses into SDK dataclasses and enums, ready to pass to register_image."""
    parsed = _parse_config(
        {
            "tag": "abc123",
            "default_timestamp_column": "timestamp",
            "default_timestamp_type": "iso_8601",
            "output_format": "parquet",
            "inputs": [{"name": "Input", "environment_variable": "INPUT_FILE", "required": True}],
            "parameters": [{"name": "Rate", "environment_variable": "SAMPLE_RATE"}],
        }
    )

    assert parsed.inputs == [FileExtractionInput(name="Input", environment_variable="INPUT_FILE", required=True)]
    assert parsed.parameters == [FileExtractionParameter(name="Rate", environment_variable="SAMPLE_RATE")]
    assert parsed.tag == "abc123"
    assert parsed.timestamp_column == "timestamp"
    assert parsed.timestamp_type == "iso_8601"
    assert parsed.output_format is FileOutputFormat.PARQUET


def test_empty_config_is_valid_with_everything_deferred_to_flags() -> None:
    """An empty config is legal: every value may instead be supplied via register-image flags."""
    parsed = _parse_config({})

    assert parsed.inputs == []
    assert parsed.parameters == []
    assert parsed.tag is None
    assert parsed.timestamp_column is None
    assert parsed.timestamp_type is None
    assert parsed.output_format is None


def test_unknown_top_level_keys_are_rejected_not_ignored() -> None:
    """A typo'd config key fails loudly instead of being silently dropped at register time."""
    with pytest.raises(click.BadParameter, match="default_timestamp_colum"):
        _parse_config({"default_timestamp_colum": "timestamp"})


def test_unknown_entry_fields_are_rejected() -> None:
    """A typo'd field inside an input entry fails loudly instead of being silently dropped."""
    with pytest.raises(click.BadParameter, match="inputs"):
        _parse_config({"inputs": [{"name": "A", "environment_variable": "X", "requird": True}]})


@pytest.mark.parametrize(
    "entry",
    [
        {"name": "", "environment_variable": "X"},
        {"name": "A", "environment_variable": ""},
    ],
)
def test_entries_require_nonempty_name_and_environment_variable(entry: dict[str, Any]) -> None:
    """Inputs and parameters must carry a non-empty name and environment variable."""
    with pytest.raises(click.BadParameter, match="non-empty"):
        _parse_config({"inputs": [entry]})


def test_duplicate_environment_variables_across_inputs_and_parameters_are_rejected() -> None:
    """An environment variable can only carry one value, so reuse across inputs and parameters fails."""
    with pytest.raises(click.BadParameter, match="duplicate environment variables"):
        _parse_config(
            {
                "inputs": [{"name": "A", "environment_variable": "X"}],
                "parameters": [{"name": "B", "environment_variable": "X"}],
            }
        )


@pytest.mark.parametrize(
    "config",
    [
        {"tag": 123},
        {"tag": ""},
        {"default_timestamp_column": ["timestamp"]},
        {"output_format": 123},
    ],
)
def test_scalar_values_must_be_nonempty_strings(config: dict[str, Any]) -> None:
    """Wrong-typed or empty scalar values fail validation instead of failing after the tarball upload."""
    with pytest.raises(click.BadParameter, match="non-empty string"):
        _parse_config(config)


def test_timestamp_type_is_case_insensitive_like_the_flag() -> None:
    """Config-supplied timestamp types accept any casing, matching the --timestamp-type flag."""
    assert _parse_config({"default_timestamp_type": "ISO_8601"}).timestamp_type == "iso_8601"


def test_unknown_timestamp_type_is_rejected() -> None:
    """A timestamp type the SDK doesn't recognize fails rather than passing through to the backend."""
    with pytest.raises(click.BadParameter, match="weeks"):
        _parse_config({"default_timestamp_type": "weeks"})


def test_non_registerable_output_format_is_rejected() -> None:
    """Formats the backend cannot ingest via containerized extraction are rejected up-front."""
    with pytest.raises(click.BadParameter, match="not registerable"):
        _parse_config({"output_format": "json_l"})
