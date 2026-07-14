from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from nominal import ts
from nominal.experimental.extractor import (
    ExtractorError,
    IngestType,
    ManifestExtractorContext,
    SingleFileExtractorContext,
    TimestampMetadata,
    manifest_extractor,
    single_file_extractor,
)


def _env(input_dir: Path, output_dir: Path, **extra: str) -> dict[str, str]:
    return {
        "OUTPUT_DIR": str(output_dir),
        "NOMINAL_EXTRACTOR_INPUT_DIR": str(input_dir),
        **extra,
    }


@pytest.fixture
def dirs(tmp_path: Path) -> tuple[Path, Path]:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    output_dir.mkdir()
    return input_dir, output_dir


def test_single_input_and_output_passthrough(dirs: tuple[Path, Path]) -> None:
    """A plain @extractor resolves the sole input and writes its single output without a manifest."""
    input_dir, output_dir = dirs
    (input_dir / "data.parquet").write_text("payload")

    @single_file_extractor
    def passthrough(ctx: SingleFileExtractorContext) -> None:
        out = ctx.output_dir / "out.parquet"
        out.write_text(ctx.input().read_text())
        ctx.set_output(out)

    passthrough.run(env=_env(input_dir, output_dir), exit=False)

    assert (output_dir / "out.parquet").read_text() == "payload"
    assert not (output_dir / "manifest.json").exists()  # single-file mode writes no manifest


def test_manifest_mode_writes_manifest_from_outputs(dirs: tuple[Path, Path]) -> None:
    """Manifest mode writes manifest.json describing every add_output call."""
    input_dir, output_dir = dirs
    (input_dir / "data.parquet").write_text("rows")

    @manifest_extractor
    def split(ctx: ManifestExtractorContext) -> None:
        for i in range(2):
            part = ctx.output_dir / f"part_{i}.parquet"
            part.write_text(f"part-{i}")
            ctx.add_output(part, ingest_type=IngestType.TABULAR, tag_columns={"vehicle": "veh_id"})

    ctx = split.run(env=_env(input_dir, output_dir), exit=False)

    manifest = json.loads((output_dir / "manifest.json").read_text())
    assert ctx.build_manifest() == manifest  # the public accessor is the same single serialization path
    entry = {
        "ingestType": "TABULAR",
        "tagColumns": {"vehicle": "veh_id"},
        "channelPrefix": None,
        "timestampMetadata": None,
    }
    assert manifest == {
        "outputs": [
            {**entry, "relativePath": "part_0.parquet"},
            {**entry, "relativePath": "part_1.parquet"},
        ]
    }


def test_manifest_includes_channel_prefix_when_set(dirs: tuple[Path, Path]) -> None:
    """A manifest entry carries channelPrefix when the output declares one."""
    input_dir, output_dir = dirs

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        out = ctx.output_dir / "telemetry.jsonl"
        out.write_text("{}")
        ctx.add_output(out, ingest_type=IngestType.JSON_L, channel_prefix="telemetry/")

    emit.run(env=_env(input_dir, output_dir), exit=False)

    [entry] = json.loads((output_dir / "manifest.json").read_text())["outputs"]
    assert entry["channelPrefix"] == "telemetry/"


def test_manifest_includes_timestamp_metadata_when_set(dirs: tuple[Path, Path]) -> None:
    """A manifest entry carries per-output timestampMetadata when the output declares it."""
    input_dir, output_dir = dirs

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        out = ctx.output_dir / "telemetry.jsonl"
        out.write_text("{}")
        ctx.add_output(
            out,
            ingest_type=IngestType.JSON_L,
            timestamp_column="ts",
            timestamp_type="epoch_microseconds",
        )

    emit.run(env=_env(input_dir, output_dir), exit=False)

    [entry] = json.loads((output_dir / "manifest.json").read_text())["outputs"]
    assert entry["timestampMetadata"] == {"seriesName": "ts", "epochTimeUnit": "MICROSECONDS"}


def test_manifest_leaves_timestamp_metadata_null_when_unset(dirs: tuple[Path, Path]) -> None:
    """Manifest entries carry a null timestampMetadata when unset, deferring to the job-level metadata."""
    input_dir, output_dir = dirs

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        out = ctx.output_dir / "data.parquet"
        out.write_text("rows")
        ctx.add_output(out)

    emit.run(env=_env(input_dir, output_dir), exit=False)

    [entry] = json.loads((output_dir / "manifest.json").read_text())["outputs"]
    assert entry["timestampMetadata"] is None


def test_manifest_mode_rejects_zero_outputs(dirs: tuple[Path, Path]) -> None:
    """Manifest mode fails when the extractor declares no outputs."""
    input_dir, output_dir = dirs

    @manifest_extractor
    def noop(ctx: ManifestExtractorContext) -> None:
        return None

    with pytest.raises(ExtractorError, match="no outputs"):
        noop.run(env=_env(input_dir, output_dir), exit=False)


def test_param_and_get_param_read_strings(dirs: tuple[Path, Path]) -> None:
    """param() returns the raw string; get_param() falls back to its default when unset."""
    input_dir, output_dir = dirs
    captured: dict[str, object] = {}

    @single_file_extractor
    def read_params(ctx: SingleFileExtractorContext) -> None:
        captured["parts"] = int(ctx.param("PARTS"))
        captured["missing"] = ctx.get_param("ABSENT")
        captured["fallback"] = ctx.get_param("ABSENT", "7")
        out = ctx.output_dir / "out.bin"
        out.write_text("x")
        ctx.set_output(out)

    read_params.run(env=_env(input_dir, output_dir, PARTS="3"), exit=False)

    assert captured == {"parts": 3, "missing": None, "fallback": "7"}


def test_param_missing_raises(dirs: tuple[Path, Path]) -> None:
    """param() raises when the parameter is not set."""
    input_dir, output_dir = dirs

    @single_file_extractor
    def needs_param(ctx: SingleFileExtractorContext) -> None:
        ctx.param("MODE")

    with pytest.raises(ExtractorError, match="required parameter 'MODE'"):
        needs_param.run(env=_env(input_dir, output_dir), exit=False)


def test_input_by_env_var_name(dirs: tuple[Path, Path]) -> None:
    """input(name) resolves a mounted input by its environment variable."""
    input_dir, output_dir = dirs
    target = input_dir / "data.parquet"
    target.write_text("x")

    @single_file_extractor
    def by_name(ctx: SingleFileExtractorContext) -> None:
        assert ctx.input("INPUT_FILE") == target
        out = ctx.output_dir / "out.bin"
        out.write_text("x")
        ctx.set_output(out)

    by_name.run(env=_env(input_dir, output_dir, INPUT_FILE=str(target)), exit=False)


def test_add_output_rejects_file_outside_output_dir(dirs: tuple[Path, Path]) -> None:
    """add_output rejects files that are not inside the output directory."""
    input_dir, output_dir = dirs
    stray = input_dir / "stray.parquet"
    stray.write_text("x")

    @manifest_extractor
    def misplaced(ctx: ManifestExtractorContext) -> None:
        ctx.add_output(stray)

    with pytest.raises(ExtractorError, match="not inside the output directory"):
        misplaced.run(env=_env(input_dir, output_dir), exit=False)


def test_inputs_enumerated_from_nominal_inputs_metadata(dirs: tuple[Path, Path]) -> None:
    """Inputs and input() resolve from _NOMINAL_INPUTS metadata instead of listing the mount."""
    input_dir, output_dir = dirs
    nominal_inputs = json.dumps(
        [
            {
                "name": "Telemetry",
                "environmentVariable": "TELEMETRY",
                "path": "/input/telemetry.parquet",
                "required": True,
            },
            {"name": "Events", "environmentVariable": "EVENTS", "path": "/input/events.parquet", "required": False},
        ]
    )

    @single_file_extractor
    def reads_inputs(ctx: SingleFileExtractorContext) -> None:
        # Enumerated from metadata (in the order Nominal serializes it), without listing the filesystem.
        assert ctx.inputs == [Path("/input/telemetry.parquet"), Path("/input/events.parquet")]
        # Resolvable by environment variable or by registered display name.
        assert ctx.input("EVENTS") == Path("/input/events.parquet")
        assert ctx.input("Telemetry") == Path("/input/telemetry.parquet")
        out = ctx.output_dir / "out.parquet"
        out.write_text("x")
        ctx.set_output(out)

    reads_inputs.run(
        env=_env(input_dir, output_dir, _NOMINAL_OUTPUT_FORMAT="PARQUET", _NOMINAL_INPUTS=nominal_inputs),
        exit=False,
    )


def test_input_unknown_name_with_contract_raises(dirs: tuple[Path, Path]) -> None:
    """input(name) on a name absent from _NOMINAL_INPUTS raises ExtractorError, not 'unknown'."""
    input_dir, output_dir = dirs
    nominal_inputs = json.dumps(
        [{"name": "Telemetry", "environmentVariable": "TELEMETRY", "path": "/input/telemetry.parquet"}]
    )

    @single_file_extractor
    def reads(ctx: SingleFileExtractorContext) -> None:
        ctx.input("OTHER")

    with pytest.raises(ExtractorError, match="not among this run's inputs"):
        reads.run(env=_env(input_dir, output_dir, _NOMINAL_INPUTS=nominal_inputs), exit=False)


def test_param_resolved_by_registered_display_name(dirs: tuple[Path, Path]) -> None:
    """param() resolves a parameter by its registered display name via _NOMINAL_PARAMETERS."""
    input_dir, output_dir = dirs
    nominal_parameters = json.dumps([{"name": "Chunk Size", "environmentVariable": "PARTS", "required": False}])

    @single_file_extractor
    def read_by_display_name(ctx: SingleFileExtractorContext) -> None:
        assert ctx.param("Chunk Size") == "3"
        assert ctx.get_param("PARTS") == "3"
        out = ctx.output_dir / "out.bin"
        out.write_text("x")
        ctx.set_output(out)

    read_by_display_name.run(
        env=_env(input_dir, output_dir, PARTS="3", _NOMINAL_PARAMETERS=nominal_parameters), exit=False
    )


def test_param_unknown_name_with_contract_raises(dirs: tuple[Path, Path]) -> None:
    """param() on a name absent from the registered contract raises ExtractorError."""
    input_dir, output_dir = dirs
    nominal_parameters = json.dumps([{"name": "Parts", "environmentVariable": "PARTS", "required": True}])

    @single_file_extractor
    def reads(ctx: SingleFileExtractorContext) -> None:
        ctx.param("NOPE")

    with pytest.raises(ExtractorError, match="registered parameters are"):
        reads.run(env=_env(input_dir, output_dir, PARTS="3", _NOMINAL_PARAMETERS=nominal_parameters), exit=False)


def test_get_param_returns_default_for_registered_unset_param(dirs: tuple[Path, Path]) -> None:
    """get_param() returns its default for a registered-but-unprovided optional parameter."""
    input_dir, output_dir = dirs
    nominal_parameters = json.dumps([{"name": "Mode", "environmentVariable": "MODE", "required": False}])
    captured: dict[str, object] = {}

    @single_file_extractor
    def reads(ctx: SingleFileExtractorContext) -> None:
        captured["mode"] = ctx.get_param("MODE", "fallback")
        out = ctx.output_dir / "out.bin"
        out.write_text("x")
        ctx.set_output(out)

    reads.run(env=_env(input_dir, output_dir, _NOMINAL_PARAMETERS=nominal_parameters), exit=False)

    assert captured == {"mode": "fallback"}


def test_finalize_rejects_undeclared_output_files(dirs: tuple[Path, Path]) -> None:
    """Files in the output directory never declared as outputs fail the run."""
    input_dir, output_dir = dirs

    @single_file_extractor
    def forgets_to_declare(ctx: SingleFileExtractorContext) -> None:
        declared = ctx.output_dir / "declared.bin"
        declared.write_text("x")
        (ctx.output_dir / "stray.bin").write_text("x")
        ctx.set_output(declared)

    with pytest.raises(ExtractorError, match="not passed to ctx.set_output"):
        forgets_to_declare.run(env=_env(input_dir, output_dir), exit=False)


def test_run_exits_nonzero_on_failure(dirs: tuple[Path, Path]) -> None:
    """run() prints a traceback and exits non-zero when the extractor raises."""
    input_dir, output_dir = dirs

    @single_file_extractor
    def boom(ctx: SingleFileExtractorContext) -> None:
        raise ValueError("kaboom")

    with pytest.raises(SystemExit) as exc:
        boom.run(env=_env(input_dir, output_dir), exit=True)
    assert exc.value.code == 1


def test_system_metadata_exposed_when_injected(dirs: tuple[Path, Path]) -> None:
    """The pipeline-injected job/dataset RIDs, tags, and job-level timestamp metadata are exposed on the context."""
    input_dir, output_dir = dirs
    (input_dir / "data.parquet").write_text("payload")
    job_timestamp = {
        "seriesName": "ts",
        "timestampType": {
            "type": "absolute",
            "absolute": {"type": "epochOfTimeUnit", "epochOfTimeUnit": {"timeUnit": "MICROSECONDS"}},
        },
    }

    @single_file_extractor
    def passthrough(ctx: SingleFileExtractorContext) -> None:
        out = ctx.output_dir / "out.parquet"
        out.write_text("done")
        ctx.set_output(out)

    ctx = passthrough.run(
        env=_env(
            input_dir,
            output_dir,
            _NOMINAL_INGEST_JOB_RID="ri.ingest-job.x",
            _NOMINAL_DATASET_RID="ri.dataset.y",
            _NOMINAL_ADDITIONAL_TAGS=json.dumps({"vehicle": "veh-1"}),
            _NOMINAL_TIMESTAMP_METADATA=json.dumps(job_timestamp),
        ),
        exit=False,
    )

    assert ctx.ingest_job_rid == "ri.ingest-job.x"
    assert ctx.dataset_rid == "ri.dataset.y"
    assert ctx.additional_tags == {"vehicle": "veh-1"}
    assert ctx.job_timestamp_metadata == TimestampMetadata(series_name="ts", timestamp_type=ts.Epoch("microseconds"))


def test_system_metadata_defaults_when_not_injected(dirs: tuple[Path, Path]) -> None:
    """System metadata degrades to None/empty on pipelines and local runs that don't inject it."""
    input_dir, output_dir = dirs
    (input_dir / "data.parquet").write_text("payload")

    @single_file_extractor
    def passthrough(ctx: SingleFileExtractorContext) -> None:
        out = ctx.output_dir / "out.parquet"
        out.write_text("done")
        ctx.set_output(out)

    ctx = passthrough.run(env=_env(input_dir, output_dir), exit=False)

    assert ctx.ingest_job_rid is None
    assert ctx.dataset_rid is None
    assert ctx.additional_tags == {}
    assert ctx.job_timestamp_metadata is None


def test_system_metadata_bad_json_raises(dirs: tuple[Path, Path]) -> None:
    """Malformed injected tag JSON raises ExtractorError on access instead of passing through."""
    input_dir, output_dir = dirs
    (input_dir / "data.parquet").write_text("payload")

    @single_file_extractor
    def reads_tags(ctx: SingleFileExtractorContext) -> None:
        ctx.additional_tags

    with pytest.raises(ExtractorError, match="_NOMINAL_ADDITIONAL_TAGS"):
        reads_tags.run(env=_env(input_dir, output_dir, _NOMINAL_ADDITIONAL_TAGS="{not json"), exit=False)


def test_per_output_timestamp_rejects_non_epoch_types(dirs: tuple[Path, Path]) -> None:
    """Per-output timestamp metadata rejects timestamp types the manifest contract cannot express."""
    input_dir, output_dir = dirs

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        out = ctx.output_dir / "data.csv"
        out.write_text("ts,x")
        ctx.add_output(out, timestamp_column="ts", timestamp_type="iso_8601")

    with pytest.raises(ExtractorError, match="numeric epoch"):
        emit.run(env=_env(input_dir, output_dir), exit=False)


def test_per_output_timestamp_requires_both_column_and_type(dirs: tuple[Path, Path]) -> None:
    """timestamp_column and timestamp_type must be provided together."""
    input_dir, output_dir = dirs

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        out = ctx.output_dir / "data.csv"
        out.write_text("ts,x")
        ctx.add_output(out, timestamp_column="ts")

    with pytest.raises(ExtractorError, match="together"):
        emit.run(env=_env(input_dir, output_dir), exit=False)


def test_decorator_preserves_function_metadata() -> None:
    """@extractor carries the wrapped function's name and docstring like a well-behaved decorator."""

    @single_file_extractor
    def my_extractor(ctx: SingleFileExtractorContext) -> None:
        """Does things."""

    assert my_extractor.__name__ == "my_extractor"
    assert my_extractor.__doc__ == "Does things."


def test_malformed_nominal_inputs_entry_raises_extractor_error(dirs: tuple[Path, Path]) -> None:
    """A _NOMINAL_INPUTS entry missing a required key raises ExtractorError naming the variable."""
    input_dir, output_dir = dirs
    bad = json.dumps([{"name": "Telemetry"}])  # missing environmentVariable and path

    @single_file_extractor
    def noop(ctx: SingleFileExtractorContext) -> None:  # pragma: no cover - must fail before the body runs
        raise AssertionError("body should not run")

    with pytest.raises(ExtractorError, match="_NOMINAL_INPUTS"):
        noop.run(env=_env(input_dir, output_dir, _NOMINAL_INPUTS=bad), exit=False)


def test_non_list_nominal_parameters_raises_extractor_error(dirs: tuple[Path, Path]) -> None:
    """_NOMINAL_PARAMETERS that decodes to a non-list raises ExtractorError instead of TypeError."""
    input_dir, output_dir = dirs

    @single_file_extractor
    def noop(ctx: SingleFileExtractorContext) -> None:  # pragma: no cover - must fail before the body runs
        raise AssertionError("body should not run")

    with pytest.raises(ExtractorError, match="_NOMINAL_PARAMETERS"):
        noop.run(env=_env(input_dir, output_dir, _NOMINAL_PARAMETERS=json.dumps({"a": 1})), exit=False)


def test_additional_tags_rejects_non_string_values(dirs: tuple[Path, Path]) -> None:
    """Injected tags with non-string values raise instead of being stringified."""
    input_dir, output_dir = dirs

    @single_file_extractor
    def reads_tags(ctx: SingleFileExtractorContext) -> None:
        ctx.additional_tags

    with pytest.raises(ExtractorError, match="string"):
        reads_tags.run(env=_env(input_dir, output_dir, _NOMINAL_ADDITIONAL_TAGS=json.dumps({"vehicle": 1})), exit=False)


def test_single_file_second_set_output_raises(dirs: tuple[Path, Path]) -> None:
    """set_output() enforces the single-file contract: a second call raises."""
    input_dir, output_dir = dirs

    @single_file_extractor
    def two_outputs(ctx: SingleFileExtractorContext) -> None:
        for i in range(2):
            part = ctx.output_dir / f"part_{i}.parquet"
            part.write_text("x")
            ctx.set_output(part)

    with pytest.raises(ExtractorError, match="already called"):
        two_outputs.run(env=_env(input_dir, output_dir), exit=False)


def test_single_file_no_output_raises(dirs: tuple[Path, Path]) -> None:
    """A single-file extractor that never calls set_output() fails the run."""
    input_dir, output_dir = dirs

    @single_file_extractor
    def noop(ctx: SingleFileExtractorContext) -> None:
        return None

    with pytest.raises(ExtractorError, match="no output"):
        noop.run(env=_env(input_dir, output_dir), exit=False)


def test_single_file_decorator_rejects_manifest_registration(dirs: tuple[Path, Path]) -> None:
    """@single_file_extractor fails at startup when the image is registered MANIFEST."""
    input_dir, output_dir = dirs

    @single_file_extractor
    def convert(ctx: SingleFileExtractorContext) -> None:  # pragma: no cover - must fail before the body runs
        raise AssertionError("body should not run")

    with pytest.raises(ExtractorError, match="disagrees with the image's registered output format"):
        convert.run(env=_env(input_dir, output_dir, _NOMINAL_OUTPUT_FORMAT="MANIFEST"), exit=False)


def test_manifest_decorator_rejects_single_file_registration(dirs: tuple[Path, Path]) -> None:
    """@manifest_extractor fails at startup when the image is registered with a single-file format."""
    input_dir, output_dir = dirs

    @manifest_extractor
    def split(ctx: ManifestExtractorContext) -> None:  # pragma: no cover - must fail before the body runs
        raise AssertionError("body should not run")

    with pytest.raises(ExtractorError, match="disagrees with the image's registered output format"):
        split.run(env=_env(input_dir, output_dir, _NOMINAL_OUTPUT_FORMAT="PARQUET"), exit=False)


def test_decorators_accept_agreeing_registration(dirs: tuple[Path, Path]) -> None:
    """Each decorator runs normally when the registered output format agrees."""
    input_dir, output_dir = dirs

    @single_file_extractor
    def convert(ctx: SingleFileExtractorContext) -> None:
        out = ctx.output_dir / "out.parquet"
        out.write_text("x")
        ctx.set_output(out)

    convert.run(env=_env(input_dir, output_dir, _NOMINAL_OUTPUT_FORMAT="PARQUET"), exit=False)
    (output_dir / "out.parquet").unlink()

    @manifest_extractor
    def split(ctx: ManifestExtractorContext) -> None:
        out = ctx.output_dir / "part_0.parquet"
        out.write_text("x")
        ctx.add_output(out)

    split.run(env=_env(input_dir, output_dir, _NOMINAL_OUTPUT_FORMAT="MANIFEST"), exit=False)
    assert (output_dir / "manifest.json").exists()


def test_add_output_rejects_manifest_filename_collision(dirs: tuple[Path, Path]) -> None:
    """add_output rejects a file named manifest.json: the runtime writes that file itself."""
    input_dir, output_dir = dirs

    @manifest_extractor
    def clobbers_manifest(ctx: ManifestExtractorContext) -> None:
        out = ctx.output_dir / "manifest.json"
        out.write_text("{}")
        ctx.add_output(out)

    with pytest.raises(ExtractorError, match="written automatically"):
        clobbers_manifest.run(env=_env(input_dir, output_dir), exit=False)


def test_manifest_relative_path_uses_forward_slashes(dirs: tuple[Path, Path]) -> None:
    """A nested output's relativePath uses forward slashes in the manifest, even on Windows."""
    input_dir, output_dir = dirs

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        sub = ctx.output_dir / "sub"
        sub.mkdir()
        out = sub / "part.parquet"
        out.write_text("rows")
        ctx.add_output(out)

    emit.run(env=_env(input_dir, output_dir), exit=False)

    [entry] = json.loads((output_dir / "manifest.json").read_text())["outputs"]
    assert entry["relativePath"] == "sub/part.parquet"


def test_run_logs_completion_with_output_count(dirs: tuple[Path, Path], caplog: pytest.LogCaptureFixture) -> None:
    """run() logs completion with the finalized output count."""
    input_dir, output_dir = dirs

    @single_file_extractor
    def convert(ctx: SingleFileExtractorContext) -> None:
        out = ctx.output_dir / "out.parquet"
        out.write_text("x")
        ctx.set_output(out)

    with caplog.at_level(logging.INFO, logger="nominal.experimental.extractor._extractor"):
        convert.run(env=_env(input_dir, output_dir), exit=False)

    assert "completed with 1 output(s)" in caplog.text


def test_startup_warns_when_registered_required_param_unset(
    dirs: tuple[Path, Path], caplog: pytest.LogCaptureFixture
) -> None:
    """A registered-required parameter with no value set warns once at startup."""
    input_dir, output_dir = dirs
    nominal_parameters = json.dumps([{"name": "Mode", "environmentVariable": "MODE", "required": True}])

    @single_file_extractor
    def ignores_mode(ctx: SingleFileExtractorContext) -> None:
        out = ctx.output_dir / "out.bin"
        out.write_text("x")
        ctx.set_output(out)

    with caplog.at_level(logging.WARNING, logger="nominal.experimental.extractor._extractor"):
        ignores_mode.run(env=_env(input_dir, output_dir, _NOMINAL_PARAMETERS=nominal_parameters), exit=False)

    assert len([r for r in caplog.records if "has no value set" in r.getMessage()]) == 1


def test_startup_warns_when_registered_input_path_missing(
    dirs: tuple[Path, Path], caplog: pytest.LogCaptureFixture
) -> None:
    """A registered input whose mounted path does not exist warns at startup; the run proceeds."""
    input_dir, output_dir = dirs
    nominal_inputs = json.dumps(
        [{"name": "Telemetry", "environmentVariable": "TELEMETRY", "path": "/no/such/file.parquet", "required": True}]
    )

    @single_file_extractor
    def ignores_input(ctx: SingleFileExtractorContext) -> None:
        out = ctx.output_dir / "out.bin"
        out.write_text("x")
        ctx.set_output(out)

    with caplog.at_level(logging.WARNING, logger="nominal.experimental.extractor._extractor"):
        ignores_input.run(env=_env(input_dir, output_dir, _NOMINAL_INPUTS=nominal_inputs), exit=False)

    assert "is not present at" in caplog.text
