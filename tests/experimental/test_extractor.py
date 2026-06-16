from __future__ import annotations

import json
from pathlib import Path

import pytest

from nominal.experimental.extractor import (
    ExtractorContext,
    ExtractorError,
    IngestType,
    extractor,
)


def _env(input_dir: Path, output_dir: Path, *, manifest: bool = False, **extra: str) -> dict[str, str]:
    env = {
        "OUTPUT_DIR": str(output_dir),
        "NOMINAL_EXTRACTOR_INPUT_DIR": str(input_dir),
        **extra,
    }
    if manifest:
        env["MANIFEST_FILE_NAME"] = "manifest.json"
    return env


@pytest.fixture
def dirs(tmp_path: Path) -> tuple[Path, Path]:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    output_dir.mkdir()
    return input_dir, output_dir


def test_single_input_and_output_passthrough(dirs: tuple[Path, Path]) -> None:
    input_dir, output_dir = dirs
    (input_dir / "data.parquet").write_text("payload")

    @extractor
    def passthrough(ctx: ExtractorContext) -> None:
        out = ctx.output_dir / "out.parquet"
        out.write_text(ctx.input().read_text())
        ctx.add_output(out)

    passthrough.run(env=_env(input_dir, output_dir), exit=False)

    assert (output_dir / "out.parquet").read_text() == "payload"
    assert not (output_dir / "manifest.json").exists()  # single-file mode writes no manifest


def test_manifest_mode_writes_manifest_from_outputs(dirs: tuple[Path, Path]) -> None:
    input_dir, output_dir = dirs
    (input_dir / "data.parquet").write_text("rows")

    @extractor
    def split(ctx: ExtractorContext) -> None:
        for i in range(2):
            part = ctx.output_dir / f"part_{i}.parquet"
            part.write_text(f"part-{i}")
            ctx.add_output(part, ingest_type=IngestType.TABULAR, tag_columns={"vehicle": "veh_id"})

    split.run(env=_env(input_dir, output_dir, manifest=True), exit=False)

    manifest = json.loads((output_dir / "manifest.json").read_text())
    assert manifest == {
        "outputs": [
            {"ingestType": "TABULAR", "relativePath": "part_0.parquet", "tagColumns": {"vehicle": "veh_id"}},
            {"ingestType": "TABULAR", "relativePath": "part_1.parquet", "tagColumns": {"vehicle": "veh_id"}},
        ]
    }


def test_manifest_includes_channel_prefix_when_set(dirs: tuple[Path, Path]) -> None:
    input_dir, output_dir = dirs

    @extractor
    def emit(ctx: ExtractorContext) -> None:
        out = ctx.output_dir / "telemetry.jsonl"
        out.write_text("{}")
        ctx.add_output(out, ingest_type=IngestType.JSON_L, channel_prefix="telemetry/")

    emit.run(env=_env(input_dir, output_dir, manifest=True), exit=False)

    [entry] = json.loads((output_dir / "manifest.json").read_text())["outputs"]
    assert entry["channelPrefix"] == "telemetry/"


def test_single_file_mode_rejects_multiple_outputs(dirs: tuple[Path, Path]) -> None:
    input_dir, output_dir = dirs

    @extractor
    def two_outputs(ctx: ExtractorContext) -> None:
        for i in range(2):
            part = ctx.output_dir / f"part_{i}.parquet"
            part.write_text("x")
            ctx.add_output(part)

    with pytest.raises(ExtractorError, match="exactly one output"):
        two_outputs.run(env=_env(input_dir, output_dir), exit=False)


def test_manifest_mode_rejects_zero_outputs(dirs: tuple[Path, Path]) -> None:
    input_dir, output_dir = dirs

    @extractor
    def noop(ctx: ExtractorContext) -> None:
        return None

    with pytest.raises(ExtractorError, match="no outputs"):
        noop.run(env=_env(input_dir, output_dir, manifest=True), exit=False)


def test_params_are_coerced_with_defaults(dirs: tuple[Path, Path]) -> None:
    input_dir, output_dir = dirs
    captured: dict[str, object] = {}

    @extractor
    def read_params(ctx: ExtractorContext) -> None:
        captured["parts"] = ctx.param("PARTS", int, default=2)
        captured["verbose"] = ctx.param("VERBOSE", bool, default=False)
        captured["missing"] = ctx.param("ABSENT")
        out = ctx.output_dir / "out.bin"
        out.write_text("x")
        ctx.add_output(out)

    read_params.run(env=_env(input_dir, output_dir, PARTS="3", VERBOSE="true"), exit=False)

    assert captured == {"parts": 3, "verbose": True, "missing": None}


def test_required_param_missing_raises(dirs: tuple[Path, Path]) -> None:
    input_dir, output_dir = dirs

    @extractor
    def needs_param(ctx: ExtractorContext) -> None:
        ctx.param("MODE", required=True)

    with pytest.raises(ExtractorError, match="required parameter 'MODE'"):
        needs_param.run(env=_env(input_dir, output_dir), exit=False)


def test_input_by_env_var_name(dirs: tuple[Path, Path]) -> None:
    input_dir, output_dir = dirs
    target = input_dir / "data.parquet"
    target.write_text("x")

    @extractor
    def by_name(ctx: ExtractorContext) -> None:
        assert ctx.input("INPUT_FILE") == target
        out = ctx.output_dir / "out.bin"
        out.write_text("x")
        ctx.add_output(out)

    by_name.run(env=_env(input_dir, output_dir, INPUT_FILE=str(target)), exit=False)


def test_add_output_rejects_file_outside_output_dir(dirs: tuple[Path, Path]) -> None:
    input_dir, output_dir = dirs
    stray = input_dir / "stray.parquet"
    stray.write_text("x")

    @extractor
    def misplaced(ctx: ExtractorContext) -> None:
        ctx.add_output(stray)

    with pytest.raises(ExtractorError, match="not inside the output directory"):
        misplaced.run(env=_env(input_dir, output_dir, manifest=True), exit=False)


def test_run_exits_nonzero_on_failure(dirs: tuple[Path, Path]) -> None:
    input_dir, output_dir = dirs

    @extractor
    def boom(ctx: ExtractorContext) -> None:
        raise ValueError("kaboom")

    with pytest.raises(SystemExit) as exc:
        boom.run(env=_env(input_dir, output_dir), exit=True)
    assert exc.value.code == 1
