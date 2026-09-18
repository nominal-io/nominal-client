"""Registration metadata comes only from static declarations."""

from pathlib import Path
from unittest.mock import patch

import pytest

from nominal.core.container_image import FileExtractionInput, FileExtractionParameter, FileOutputFormat
from nominal.experimental import extractor as ex


def test_registration_is_pure_and_ordered():
    def converter(raw):
        raise AssertionError("must not convert")

    @ex.manifest_extractor(default_timestamp_column="time", default_timestamp_type="epoch_microseconds")
    @ex.input("recording", name="Flight recording", description="Data", file_suffixes=["mcap"])
    @ex.input("calibration", default=None)
    @ex.parameter("parts", envvar="PART_COUNT", type=converter, default=2, description="Partitions")
    def extract(ctx, recording: Path, calibration: Path | None, parts: int):
        raise AssertionError("must not run")

    with patch("os.environ") as environ:
        environ.get.side_effect = AssertionError("must not read environment")
        actual = extract.registration_kwargs()
    assert actual == {
        "inputs": [
            FileExtractionInput("Flight recording", "RECORDING", "Data", ("mcap",), True),
            FileExtractionInput("calibration", "CALIBRATION", required=False),
        ],
        "parameters": [FileExtractionParameter("parts", "PART_COUNT", "Partitions", False)],
        "output_format": FileOutputFormat.MANIFEST,
        "default_timestamp_column": "time",
        "default_timestamp_type": "epoch_microseconds",
    }
    actual["inputs"].clear()
    assert len(extract.registration_kwargs()["inputs"]) == 2


@pytest.mark.parametrize("declared", [False, True])
def test_legacy_lookups_never_appear_in_registration(declared):
    def extract(ctx, **kwargs):
        ctx.input("HIDDEN")
        ctx.param("SECRET")

    # Explicit callback argument needed only when using a declaration.
    if declared:

        def extract(ctx, value):
            ctx.param("SECRET")

        extract = ex.parameter("value")(extract)
    entrypoint = ex.manifest_extractor(extract, default_timestamp_column="time", default_timestamp_type="epoch_seconds")
    metadata = entrypoint.registration_kwargs()
    assert metadata["inputs"] == []
    assert [p.environment_variable for p in metadata["parameters"]] == (["VALUE"] if declared else [])


@pytest.mark.parametrize("outer", [ex.manifest_extractor, ex.single_file_extractor])
def test_registration_requires_explicit_timestamp_pair(outer):
    with pytest.raises(ValueError, match="timestamp"):
        outer(lambda ctx: None, default_timestamp_column="time")
    with pytest.raises(ValueError, match="timestamp"):
        outer(lambda ctx: None).registration_kwargs()


def test_single_file_requires_format_only_for_registration():
    extract = ex.single_file_extractor(
        lambda ctx: None, default_timestamp_column="time", default_timestamp_type="epoch_seconds"
    )
    with pytest.raises(ValueError, match="output_format"):
        extract.registration_kwargs()


@pytest.mark.parametrize(
    "format", [FileOutputFormat.MANIFEST, FileOutputFormat.UNSPECIFIED, FileOutputFormat.PARQUET_TAR]
)
def test_single_file_rejects_invalid_format(format):
    with pytest.raises(ValueError, match="output_format"):
        ex.single_file_extractor(lambda ctx: None, output_format=format)


def test_explicit_single_file_format_and_error_mapping(tmp_path):
    @ex.single_file_extractor(
        output_format=FileOutputFormat.CSV, default_timestamp_column="time", default_timestamp_type="epoch_seconds"
    )
    @ex.error(ex.ExtractorError, code="FORMAT", exit_code=65)
    def extract(ctx):
        raise AssertionError("format mismatch must fail before callback")

    assert extract.registration_kwargs()["output_format"] is FileOutputFormat.CSV
    with pytest.raises(SystemExit) as error:
        extract.run(
            env={
                "OUTPUT_DIR": str(tmp_path),
                "_NOMINAL_OUTPUT_FORMAT": "PARQUET",
            },
            termination_log_path=tmp_path / "termination",
        )
    assert error.value.code == 65
