"""Catalog exports share the runtime declarations and respect the catalog wire contract."""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from nominal import ts
from nominal.experimental import extractor as ex

IDENTITY = dict(id="recording-extractor", version="1.2.0", display_name="Recording", description="Decode recordings")


def make_extractor(**options):
    @ex.manifest_extractor(
        default_timestamp_column="time", default_timestamp_type=options.pop("timestamp", "epoch_seconds")
    )
    @ex.input("recording", envvar=options.pop("envvar", "SOURCE"), file_suffixes=options.pop("suffixes", ["csv"]))
    @ex.parameter("stride", type=int, default=1, description="Sample stride")
    @ex.error(
        ValueError,
        code=options.pop("code", "MALFORMED_INPUT"),
        exit_code=64,
        message=options.pop("message", "Recording is malformed"),
    )
    def convert(ctx, recording: Path, stride: int):
        raise AssertionError("export must not invoke the callback")

    assert not options
    return convert


def test_catalog_export_is_pure_serializable_and_independent():
    convert = make_extractor()
    expected = {
        **IDENTITY,
        "inputs": [{"environment_variable": "SOURCE", "file_filters": [{"suffix": "csv"}], "required": True}],
        "parameters": [
            {"environment_variable": "STRIDE", "name": "stride", "description": "Sample stride", "required": False}
        ],
        "output_file_format": "MANIFEST",
        "default_timestamp_metadata": {
            "series_name": "time",
            "timestamp_type": {"absolute": {"epoch_of_time_unit": {"time_unit": "SECONDS"}}},
        },
        "exit_code_mappings": [
            {"exit_code": 64, "code": "MALFORMED_INPUT", "message": "Recording is malformed", "retryable": False}
        ],
    }
    actual = convert.catalog_manifest(**IDENTITY)
    assert json.loads(json.dumps(actual)) == expected
    actual["inputs"].clear()
    assert convert.catalog_manifest(**IDENTITY) == expected
    assert convert.registration_kwargs()["inputs"][0].environment_variable == "SOURCE"


@pytest.mark.parametrize(
    "change",
    [
        {"id": "Bad_ID"},
        {"version": "01.2.0"},
        {"display_name": ""},
        {"description": "x" * 1025},
    ],
)
def test_invalid_catalog_identity(change):
    with pytest.raises(ValueError):
        make_extractor().catalog_manifest(**(IDENTITY | change))


@pytest.mark.parametrize(
    "options,match",
    [
        ({"envvar": "source"}, "environment"),
        ({"suffixes": []}, "suffix"),
        ({"suffixes": [".csv"]}, "suffix"),
        ({"message": None}, "message"),
        ({"message": "x" * 513}, "message"),
        ({"code": "UNKNOWN"}, "reserved"),
        ({"code": "bad-code"}, "code"),
        ({"timestamp": ts.Relative("seconds", start=datetime(2026, 1, 1, tzinfo=timezone.utc))}, "timestamp"),
        ({"timestamp": ts.Custom("yyyy-MM-dd")}, "timestamp"),
    ],
)
def test_catalog_restrictions_do_not_change_runtime_registration(options, match):
    convert = make_extractor(**options)
    convert.registration_kwargs()
    with pytest.raises(ValueError, match=match):
        convert.catalog_manifest(**IDENTITY)


@pytest.mark.parametrize("same_fallback", [False, True])
def test_fallbacks_group_by_exit_code_or_reject_ambiguity(same_fallback):
    @ex.input("source", file_suffixes=["csv"])
    @ex.error(ValueError, code="BAD_INPUT", exit_code=64, message="Bad recording")
    def callback(ctx, source):
        pass

    first = ex.manifest_extractor(callback, default_timestamp_column="time", default_timestamp_type="iso_8601")
    callback = ex.error(OSError, code="BAD_INPUT" if same_fallback else "IO", exit_code=64, message="Bad recording")(
        callback
    )
    second = ex.manifest_extractor(callback, default_timestamp_column="time", default_timestamp_type="iso_8601")
    assert len(first.catalog_manifest(**IDENTITY)["exit_code_mappings"]) == 1
    if same_fallback:
        assert second.catalog_manifest(**IDENTITY) == first.catalog_manifest(**IDENTITY)
    else:
        with pytest.raises(ValueError, match="exit.*64"):
            second.catalog_manifest(**IDENTITY)
