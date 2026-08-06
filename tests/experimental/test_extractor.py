from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Protocol, TypeVar

import pytest

from nominal import ts
from nominal.core.exceptions import NominalVideoScaleModeError, NominalVideoTimestampModeError
from nominal.experimental.extractor import (
    Extractor,
    ExtractorContext,
    ExtractorError,
    ManifestExtractorContext,
    SingleFileExtractorContext,
    TimestampMetadata,
    manifest_extractor,
    single_file_extractor,
)

_CtxT = TypeVar("_CtxT", bound=ExtractorContext)


@pytest.fixture
def input_dir(tmp_path: Path) -> Path:
    """The mount Nominal places an extractor's input files in."""
    path = tmp_path / "input"
    path.mkdir()
    return path


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    """The directory an extractor writes its outputs to, named by $OUTPUT_DIR."""
    path = tmp_path / "output"
    path.mkdir()
    return path


@pytest.fixture
def extractor_env(input_dir: Path, output_dir: Path) -> Callable[..., dict[str, str]]:
    """The environment Nominal hands the container, plus any extra variables a test injects.

    Extra keyword arguments are how a test supplies the ``_NOMINAL_*`` contract metadata or a
    parameter value.
    """

    def build(**extra: str) -> dict[str, str]:
        return {
            "OUTPUT_DIR": str(output_dir),
            "NOMINAL_EXTRACTOR_INPUT_DIR": str(input_dir),
            **extra,
        }

    return build


class RunExtractor(Protocol):
    """Runs an extractor and hands back *its own* context type, so subclass members stay reachable."""

    def __call__(self, extractor: Extractor[_CtxT], **extra_env: str) -> _CtxT: ...


class ReadManifest(Protocol):
    """Reads the manifest document the runtime wrote."""

    def __call__(self) -> dict[str, Any]: ...


@pytest.fixture
def run_extractor(extractor_env: Callable[..., dict[str, str]]) -> RunExtractor:
    """Run an extractor against that environment, re-raising instead of exiting."""

    def run(extractor: Extractor[_CtxT], **extra_env: str) -> _CtxT:
        return extractor.run(env=extractor_env(**extra_env), exit=False)

    return run


@pytest.fixture
def manifest_document(output_dir: Path) -> ReadManifest:
    """Read back the manifest the runtime wrote, as the ingest pipeline would see it."""

    def read() -> dict[str, Any]:
        document: dict[str, Any] = json.loads((output_dir / "manifest.json").read_text())
        return document

    return read


def test_single_input_and_output_passthrough(input_dir: Path, output_dir: Path, run_extractor: RunExtractor) -> None:
    """A plain @extractor resolves the sole input and writes its single output without a manifest."""
    (input_dir / "data.parquet").write_text("payload")

    @single_file_extractor
    def passthrough(ctx: SingleFileExtractorContext) -> None:
        out = ctx.output_dir / "out.parquet"
        out.write_text(ctx.input().read_text())
        ctx.set_output(out)

    run_extractor(passthrough)

    assert (output_dir / "out.parquet").read_text() == "payload"
    assert not (output_dir / "manifest.json").exists()  # single-file mode writes no manifest


def test_manifest_mode_writes_manifest_from_outputs(
    input_dir: Path, output_dir: Path, run_extractor: RunExtractor
) -> None:
    """Manifest mode writes manifest.json describing every add_output call."""
    (input_dir / "data.parquet").write_text("rows")

    @manifest_extractor
    def split(ctx: ManifestExtractorContext) -> None:
        for i in range(2):
            part = ctx.output_dir / f"part_{i}.parquet"
            part.write_text(f"part-{i}")
            ctx.add_tabular(part, tag_columns={"vehicle": "veh_id"})

    ctx = run_extractor(split)

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
        ],
        "videoOutputs": [],
    }


@pytest.mark.parametrize(
    ("filename", "add_output_kwargs", "expected_entry_fields"),
    [
        pytest.param(
            "telemetry.csv",
            {"channel_prefix": "telemetry/"},
            {"channelPrefix": "telemetry/"},
            id="channel-prefix",
        ),
        pytest.param(
            "telemetry.csv",
            {"timestamp_column": "ts", "timestamp_type": "epoch_microseconds"},
            {"timestampMetadata": {"seriesName": "ts", "epochTimeUnit": "MICROSECONDS", "relativeOffset": None}},
            id="epoch-timestamp-metadata",
        ),
        pytest.param(
            "run.csv",
            {
                "timestamp_column": "elapsed",
                "timestamp_type": ts.Relative("milliseconds", start=datetime(2026, 1, 1, tzinfo=timezone.utc)),
            },
            {
                "timestampMetadata": {
                    "seriesName": "elapsed",
                    "epochTimeUnit": "MILLISECONDS",
                    "relativeOffset": "2026-01-01T00:00:00.000000000Z",
                }
            },
            id="relative-timestamp-metadata",
        ),
        pytest.param(
            "data.parquet",
            {},
            {"channelPrefix": None, "timestampMetadata": None},
            id="nothing-declared-defers-to-job-level",
        ),
    ],
)
def test_manifest_entry_carries_what_the_output_declared(
    manifest_document: ReadManifest,
    run_extractor: RunExtractor,
    filename: str,
    add_output_kwargs: dict[str, Any],
    expected_entry_fields: dict[str, Any],
) -> None:
    """Each add_output argument reaches its own field on the manifest entry, and is null when unset."""

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        out = ctx.output_dir / filename
        out.write_text("rows")
        ctx.add_tabular(out, **add_output_kwargs)

    run_extractor(emit)

    [entry] = manifest_document()["outputs"]
    assert {field: entry[field] for field in expected_entry_fields} == expected_entry_fields


@pytest.mark.parametrize(
    ("method", "filename", "expected_match"),
    [
        pytest.param("add_tabular", "data.jsonl", "must end in one of", id="tabular-rejects-jsonl"),
        pytest.param("add_tabular", "data.parquet.tar", "must end in one of", id="tabular-rejects-parquet-archive"),
        pytest.param("add_avro_stream", "data.parquet", "must end in one of", id="avro-rejects-parquet"),
        pytest.param("add_journal_json", "data.csv", "must end in one of", id="journal-json-rejects-csv"),
        pytest.param("add_video", "data.csv", "must end in one of", id="video-rejects-csv"),
    ],
)
def test_each_format_method_rejects_the_wrong_extension(
    run_extractor: RunExtractor, method: str, filename: str, expected_match: str
) -> None:
    """Each format has extensions the pipeline can actually read, so the wrong one fails at the call.

    Parquet archives are the subtle one: they are tabular to a reader, but the manifest ingest path
    only handles plain CSV and Parquet.
    """
    kwargs: dict[str, Any] = {"channel": "c", "start": 1_753_000_000_000_000_000} if method == "add_video" else {}

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        out = ctx.output_dir / filename
        out.write_text("rows")
        getattr(ctx, method)(out, **kwargs)

    with pytest.raises(ValueError, match=expected_match):
        run_extractor(emit)


def test_avro_stream_accepts_gzipped(manifest_document: ReadManifest, run_extractor: RunExtractor) -> None:
    """The pipeline reads .avro.gz as well as .avro."""

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        out = ctx.output_dir / "records.avro.gz"
        out.write_bytes(b"avro")
        ctx.add_avro_stream(out, channel_prefix="sensors/")

    run_extractor(emit)

    [entry] = manifest_document()["outputs"]
    assert entry["ingestType"] == "AVRO_STREAM"
    assert entry["channelPrefix"] == "sensors/"


def test_manifest_mode_rejects_zero_outputs(run_extractor: RunExtractor) -> None:
    """Manifest mode fails when the extractor declares no outputs."""

    @manifest_extractor
    def noop(ctx: ManifestExtractorContext) -> None:
        return None

    with pytest.raises(ExtractorError, match="no outputs"):
        run_extractor(noop)


def test_param_and_get_param_read_strings(output_dir: Path, run_extractor: RunExtractor) -> None:
    """param() returns the raw string; get_param() falls back to its default when unset."""
    captured: dict[str, object] = {}

    @single_file_extractor
    def read_params(ctx: SingleFileExtractorContext) -> None:
        captured["parts"] = int(ctx.param("PARTS"))
        captured["missing"] = ctx.get_param("ABSENT")
        captured["fallback"] = ctx.get_param("ABSENT", "7")
        out = ctx.output_dir / "out.bin"
        out.write_text("x")
        ctx.set_output(out)

    run_extractor(read_params, PARTS="3")

    assert captured == {"parts": 3, "missing": None, "fallback": "7"}


def test_param_missing_raises(run_extractor: RunExtractor) -> None:
    """param() raises when the parameter is not set."""

    @single_file_extractor
    def needs_param(ctx: SingleFileExtractorContext) -> None:
        ctx.param("MODE")

    with pytest.raises(ExtractorError, match="required parameter 'MODE'"):
        run_extractor(needs_param)


def test_input_by_env_var_name(input_dir: Path, output_dir: Path, run_extractor: RunExtractor) -> None:
    """input(name) resolves a mounted input by its environment variable."""
    target = input_dir / "data.parquet"
    target.write_text("x")

    @single_file_extractor
    def by_name(ctx: SingleFileExtractorContext) -> None:
        assert ctx.input("INPUT_FILE") == target
        out = ctx.output_dir / "out.bin"
        out.write_text("x")
        ctx.set_output(out)

    run_extractor(by_name, INPUT_FILE=str(target))


def test_add_output_rejects_file_outside_output_dir(input_dir: Path, run_extractor: RunExtractor) -> None:
    """add_output rejects files that are not inside the output directory."""
    stray = input_dir / "stray.parquet"
    stray.write_text("x")

    @manifest_extractor
    def misplaced(ctx: ManifestExtractorContext) -> None:
        ctx.add_tabular(stray)

    with pytest.raises(ExtractorError, match="not inside the output directory"):
        run_extractor(misplaced)


def test_inputs_enumerated_from_nominal_inputs_metadata(output_dir: Path, run_extractor: RunExtractor) -> None:
    """Inputs and input() resolve from _NOMINAL_INPUTS metadata instead of listing the mount."""
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

    run_extractor(reads_inputs, _NOMINAL_OUTPUT_FORMAT="PARQUET", _NOMINAL_INPUTS=nominal_inputs)


def test_input_unknown_name_with_contract_raises(run_extractor: RunExtractor) -> None:
    """input(name) on a name absent from _NOMINAL_INPUTS raises ExtractorError, not 'unknown'."""
    nominal_inputs = json.dumps(
        [{"name": "Telemetry", "environmentVariable": "TELEMETRY", "path": "/input/telemetry.parquet"}]
    )

    @single_file_extractor
    def reads(ctx: SingleFileExtractorContext) -> None:
        ctx.input("OTHER")

    with pytest.raises(ExtractorError, match="not among this run's inputs"):
        run_extractor(reads, _NOMINAL_INPUTS=nominal_inputs)


def test_param_resolved_by_registered_display_name(output_dir: Path, run_extractor: RunExtractor) -> None:
    """param() resolves a parameter by its registered display name via _NOMINAL_PARAMETERS."""
    nominal_parameters = json.dumps([{"name": "Chunk Size", "environmentVariable": "PARTS", "required": False}])

    @single_file_extractor
    def read_by_display_name(ctx: SingleFileExtractorContext) -> None:
        assert ctx.param("Chunk Size") == "3"
        assert ctx.get_param("PARTS") == "3"
        out = ctx.output_dir / "out.bin"
        out.write_text("x")
        ctx.set_output(out)

    run_extractor(read_by_display_name, PARTS="3", _NOMINAL_PARAMETERS=nominal_parameters)


def test_param_unknown_name_with_contract_raises(run_extractor: RunExtractor) -> None:
    """param() on a name absent from the registered contract raises ExtractorError."""
    nominal_parameters = json.dumps([{"name": "Parts", "environmentVariable": "PARTS", "required": True}])

    @single_file_extractor
    def reads(ctx: SingleFileExtractorContext) -> None:
        ctx.param("NOPE")

    with pytest.raises(ExtractorError, match="registered parameters are"):
        run_extractor(reads, PARTS="3", _NOMINAL_PARAMETERS=nominal_parameters)


def test_get_param_returns_default_for_registered_unset_param(output_dir: Path, run_extractor: RunExtractor) -> None:
    """get_param() returns its default for a registered-but-unprovided optional parameter."""
    nominal_parameters = json.dumps([{"name": "Mode", "environmentVariable": "MODE", "required": False}])
    captured: dict[str, object] = {}

    @single_file_extractor
    def reads(ctx: SingleFileExtractorContext) -> None:
        captured["mode"] = ctx.get_param("MODE", "fallback")
        out = ctx.output_dir / "out.bin"
        out.write_text("x")
        ctx.set_output(out)

    run_extractor(reads, _NOMINAL_PARAMETERS=nominal_parameters)

    assert captured == {"mode": "fallback"}


def test_finalize_warns_about_undeclared_output_files(
    run_extractor: RunExtractor, caplog: pytest.LogCaptureFixture
) -> None:
    """An undeclared file is warned about, not fatal: it just will not be ingested."""

    @single_file_extractor
    def forgets_to_declare(ctx: SingleFileExtractorContext) -> None:
        declared = ctx.output_dir / "declared.bin"
        declared.write_text("x")
        (ctx.output_dir / "stray.bin").write_text("x")
        ctx.set_output(declared)

    with caplog.at_level(logging.WARNING, logger="nominal.experimental.extractor"):
        run_extractor(forgets_to_declare)

    assert "not passed to ctx.set_output()" in caplog.text
    assert "stray.bin" in caplog.text


def test_run_exits_nonzero_on_failure(extractor_env: Callable[..., dict[str, str]]) -> None:
    """run() prints a traceback and exits non-zero when the extractor raises."""

    @single_file_extractor
    def boom(ctx: SingleFileExtractorContext) -> None:
        raise ValueError("kaboom")

    with pytest.raises(SystemExit) as exc:
        boom.run(env=extractor_env(), exit=True)
    assert exc.value.code == 1


def test_system_metadata_exposed_when_injected(input_dir: Path, output_dir: Path, run_extractor: RunExtractor) -> None:
    """The pipeline-injected job/dataset RIDs, tags, and job-level timestamp metadata are exposed on the context."""
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

    ctx = run_extractor(
        passthrough,
        _NOMINAL_INGEST_JOB_RID="ri.ingest-job.x",
        _NOMINAL_DATASET_RID="ri.dataset.y",
        _NOMINAL_ADDITIONAL_TAGS=json.dumps({"vehicle": "veh-1"}),
        _NOMINAL_TIMESTAMP_METADATA=json.dumps(job_timestamp),
    )

    assert ctx.ingest_job_rid == "ri.ingest-job.x"
    assert ctx.dataset_rid == "ri.dataset.y"
    assert ctx.additional_tags == {"vehicle": "veh-1"}
    assert ctx.job_timestamp_metadata == TimestampMetadata(series_name="ts", timestamp_type=ts.Epoch("microseconds"))


def test_system_metadata_treats_an_empty_value_as_absent(run_extractor: RunExtractor) -> None:
    """An environment variable set to the empty string means the same as one that was never set.

    Without this, callers would get a falsy string that is not a RID and would have to check for it
    separately, which no other part of this contract asks of them.
    """
    captured: dict[str, object] = {}

    @single_file_extractor
    def reads(ctx: SingleFileExtractorContext) -> None:
        captured["job"] = ctx.ingest_job_rid
        captured["dataset"] = ctx.dataset_rid
        out = ctx.output_dir / "out.bin"
        out.write_text("x")
        ctx.set_output(out)

    run_extractor(reads, _NOMINAL_INGEST_JOB_RID="", _NOMINAL_DATASET_RID="")

    assert captured == {"job": None, "dataset": None}


def test_system_metadata_defaults_when_not_injected(
    input_dir: Path, output_dir: Path, run_extractor: RunExtractor
) -> None:
    """System metadata degrades to None/empty on pipelines and local runs that don't inject it."""
    (input_dir / "data.parquet").write_text("payload")

    @single_file_extractor
    def passthrough(ctx: SingleFileExtractorContext) -> None:
        out = ctx.output_dir / "out.parquet"
        out.write_text("done")
        ctx.set_output(out)

    ctx = run_extractor(passthrough)

    assert ctx.ingest_job_rid is None
    assert ctx.dataset_rid is None
    assert ctx.additional_tags == {}
    assert ctx.job_timestamp_metadata is None


@pytest.mark.parametrize(
    ("add_output_kwargs", "expected_match"),
    [
        pytest.param({"timestamp_column": "ts", "timestamp_type": "iso_8601"}, "numeric epoch", id="non-epoch-type"),
        pytest.param(
            {
                "timestamp_column": "elapsed",
                "timestamp_type": ts.Relative("hours", start=datetime(2026, 1, 1, tzinfo=timezone.utc)),
            },
            "does not support time unit 'hours'",
            id="unit-outside-contract",
        ),
    ],
)
def test_add_output_rejects_unexpressible_timestamp_metadata(
    run_extractor: RunExtractor,
    add_output_kwargs: dict[str, Any],
    expected_match: str,
) -> None:
    """A timestamp type the manifest contract cannot express is an ExtractorError."""

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        out = ctx.output_dir / "data.csv"
        out.write_text("ts,x")
        ctx.add_tabular(out, **add_output_kwargs)

    with pytest.raises(ExtractorError, match=expected_match):
        run_extractor(emit)


@pytest.mark.parametrize(
    "add_output_kwargs",
    [
        pytest.param({"timestamp_column": "ts"}, id="column-without-type"),
        pytest.param({"timestamp_type": "epoch_seconds"}, id="type-without-column"),
    ],
)
def test_add_output_rejects_a_half_specified_timestamp_pair_as_a_value_error(
    run_extractor: RunExtractor, add_output_kwargs: dict[str, Any]
) -> None:
    """A malformed argument is a ValueError, as everywhere else in the client.

    ExtractorError is reserved for violations of the extractor's own contract -- a reserved file
    name, a file outside the output directory -- not for a caller passing half of a pair.
    """

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        out = ctx.output_dir / "data.csv"
        out.write_text("ts,x")
        ctx.add_tabular(out, **add_output_kwargs)

    with pytest.raises(ValueError, match="pass both"):
        run_extractor(emit)


def test_decorator_preserves_function_metadata() -> None:
    """@extractor carries the wrapped function's name and docstring like a well-behaved decorator."""

    @single_file_extractor
    def my_extractor(ctx: SingleFileExtractorContext) -> None:
        """Does things."""

    assert my_extractor.__name__ == "my_extractor"
    assert my_extractor.__doc__ == "Does things."


@pytest.mark.parametrize(
    ("env_var", "value", "expected_match"),
    [
        pytest.param(
            "_NOMINAL_INPUTS",
            json.dumps([{"name": "Telemetry"}]),  # missing environmentVariable and path
            "_NOMINAL_INPUTS",
            id="inputs-entry-missing-keys",
        ),
        pytest.param("_NOMINAL_PARAMETERS", json.dumps({"a": 1}), "_NOMINAL_PARAMETERS", id="parameters-not-a-list"),
        pytest.param("_NOMINAL_INPUTS", "{not json", "_NOMINAL_INPUTS", id="inputs-not-valid-json"),
        pytest.param("_NOMINAL_ADDITIONAL_TAGS", "{not json", "_NOMINAL_ADDITIONAL_TAGS", id="tags-not-valid-json"),
        pytest.param("_NOMINAL_ADDITIONAL_TAGS", json.dumps({"vehicle": 1}), "string", id="tag-value-not-a-string"),
        pytest.param("_NOMINAL_ADDITIONAL_TAGS", json.dumps(["not", "a", "map"]), "JSON object", id="tags-not-a-map"),
    ],
)
def test_malformed_injected_contract_metadata_raises(
    run_extractor: RunExtractor, env_var: str, value: str, expected_match: str
) -> None:
    """Contract metadata the platform injected but that does not parse raises ExtractorError, naming it.

    The tag cases read ctx.additional_tags, which is resolved lazily; the input and parameter cases
    are parsed while the context is built, before the body runs at all.
    """

    @single_file_extractor
    def reads_tags(ctx: SingleFileExtractorContext) -> None:
        ctx.additional_tags
        out = ctx.output_dir / "out.bin"
        out.write_text("x")
        ctx.set_output(out)

    with pytest.raises(ExtractorError, match=expected_match):
        run_extractor(reads_tags, **{env_var: value})


def test_single_file_second_set_output_raises(output_dir: Path, run_extractor: RunExtractor) -> None:
    """set_output() enforces the single-file contract: a second call raises."""

    @single_file_extractor
    def two_outputs(ctx: SingleFileExtractorContext) -> None:
        for i in range(2):
            part = ctx.output_dir / f"part_{i}.parquet"
            part.write_text("x")
            ctx.set_output(part)

    with pytest.raises(ExtractorError, match="already called"):
        run_extractor(two_outputs)


def test_single_file_no_output_raises(run_extractor: RunExtractor) -> None:
    """A single-file extractor that never calls set_output() fails the run."""

    @single_file_extractor
    def noop(ctx: SingleFileExtractorContext) -> None:
        return None

    with pytest.raises(ExtractorError, match="no output"):
        run_extractor(noop)


@pytest.mark.parametrize(
    ("decorator", "registered_format"),
    [
        pytest.param(single_file_extractor, "MANIFEST", id="single-file-code-manifest-image"),
        pytest.param(manifest_extractor, "PARQUET", id="manifest-code-single-file-image"),
        pytest.param(manifest_extractor, "CSV", id="manifest-code-csv-image"),
    ],
)
def test_decorator_rejects_disagreeing_registration(
    run_extractor: RunExtractor,
    decorator: Callable[..., Extractor[Any]],
    registered_format: str,
) -> None:
    """A decorator that contradicts the image's registered output format fails at startup."""

    @decorator
    def extract(ctx: ExtractorContext) -> None:  # pragma: no cover - must fail before the body runs
        raise AssertionError("body should not run")

    with pytest.raises(ExtractorError, match="disagrees with the image's registered output format"):
        run_extractor(extract, _NOMINAL_OUTPUT_FORMAT=registered_format)


def test_decorators_accept_agreeing_registration(output_dir: Path, run_extractor: RunExtractor) -> None:
    """Each decorator runs normally when the registered output format agrees."""

    @single_file_extractor
    def convert(ctx: SingleFileExtractorContext) -> None:
        out = ctx.output_dir / "out.parquet"
        out.write_text("x")
        ctx.set_output(out)

    run_extractor(convert, _NOMINAL_OUTPUT_FORMAT="PARQUET")
    (output_dir / "out.parquet").unlink()

    @manifest_extractor
    def split(ctx: ManifestExtractorContext) -> None:
        out = ctx.output_dir / "part_0.parquet"
        out.write_text("x")
        ctx.add_tabular(out)

    run_extractor(split, _NOMINAL_OUTPUT_FORMAT="MANIFEST")
    assert (output_dir / "manifest.json").exists()


def test_add_output_rejects_manifest_filename_collision(output_dir: Path, run_extractor: RunExtractor) -> None:
    """add_output rejects a file named manifest.json: the runtime writes that file itself."""

    @manifest_extractor
    def clobbers_manifest(ctx: ManifestExtractorContext) -> None:
        out = ctx.output_dir / "manifest.json"
        out.write_text("{}")
        ctx.add_tabular(out)

    with pytest.raises(ExtractorError, match="is written by the runtime"):
        run_extractor(clobbers_manifest)


def test_manifest_relative_path_uses_forward_slashes(output_dir: Path, run_extractor: RunExtractor) -> None:
    """A nested output's relativePath uses forward slashes in the manifest, even on Windows."""

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        sub = ctx.output_dir / "sub"
        sub.mkdir()
        out = sub / "part.parquet"
        out.write_text("rows")
        ctx.add_tabular(out)

    run_extractor(emit)

    [entry] = json.loads((output_dir / "manifest.json").read_text())["outputs"]
    assert entry["relativePath"] == "sub/part.parquet"


def test_run_logs_completion_with_output_count(
    output_dir: Path, run_extractor: RunExtractor, caplog: pytest.LogCaptureFixture
) -> None:
    """run() logs completion with the finalized output count."""

    @single_file_extractor
    def convert(ctx: SingleFileExtractorContext) -> None:
        out = ctx.output_dir / "out.parquet"
        out.write_text("x")
        ctx.set_output(out)

    with caplog.at_level(logging.INFO, logger="nominal.experimental.extractor"):
        run_extractor(convert)

    assert "completed with 1 output(s)" in caplog.text


@pytest.mark.parametrize(
    ("contract_env", "expected_warning"),
    [
        pytest.param(
            {"_NOMINAL_PARAMETERS": json.dumps([{"name": "Mode", "environmentVariable": "MODE", "required": True}])},
            "has no value set",
            id="required-parameter-unset",
        ),
        pytest.param(
            {
                "_NOMINAL_INPUTS": json.dumps(
                    [
                        {
                            "name": "Telemetry",
                            "environmentVariable": "TELEMETRY",
                            "path": "/no/such/file.parquet",
                            "required": True,
                        }
                    ]
                )
            },
            "is not present at",
            id="registered-input-path-missing",
        ),
    ],
)
def test_startup_warns_about_the_registered_contract_but_proceeds(
    run_extractor: RunExtractor,
    caplog: pytest.LogCaptureFixture,
    contract_env: dict[str, str],
    expected_warning: str,
) -> None:
    """A registered contract the environment does not satisfy warns once at startup; the run proceeds.

    Advisory rather than fatal: only code that actually reads the affected name is impacted.
    """

    @single_file_extractor
    def ignores_the_contract(ctx: SingleFileExtractorContext) -> None:
        out = ctx.output_dir / "out.bin"
        out.write_text("x")
        ctx.set_output(out)

    with caplog.at_level(logging.WARNING, logger="nominal.experimental.extractor"):
        run_extractor(ignores_the_contract, **contract_env)

    assert len([r for r in caplog.records if expected_warning in r.getMessage()]) == 1


def test_manifest_video_from_start_carries_channel_and_starting_timestamp(
    output_dir: Path, run_extractor: RunExtractor
) -> None:
    """A start-anchored video becomes a videoOutputs entry with a noManifest timestamp strategy."""
    start = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        video = ctx.output_dir / "front.mp4"
        video.write_bytes(b"h264")
        ctx.add_video(video, channel="camera/front", start=start)

    run_extractor(emit)

    manifest = json.loads((output_dir / "manifest.json").read_text())
    [entry] = manifest["videoOutputs"]
    assert entry["relativePath"] == "front.mp4"
    assert entry["channel"] == "camera/front"
    assert entry["timestampManifest"]["noManifest"]["startingTimestamp"]["seconds"] == int(start.timestamp())


def test_manifest_video_from_start_writes_no_sidecar(output_dir: Path, run_extractor: RunExtractor) -> None:
    """A start-anchored video needs no per-frame sidecar, so none is written."""

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        video = ctx.output_dir / "front.mp4"
        video.write_bytes(b"h264")
        ctx.add_video(video, channel="camera/front", start=1_753_000_000_000_000_000)

    run_extractor(emit)

    assert sorted(p.name for p in output_dir.iterdir()) == ["front.mp4", "manifest.json"]


def test_manifest_video_frame_timestamps_writes_sidecar(output_dir: Path, run_extractor: RunExtractor) -> None:
    """Per-frame timestamps are serialized to a sidecar the entry points at by relative path."""
    timestamps = [1_753_000_000_000_000_000, 1_753_000_000_033_000_000]

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        video = ctx.output_dir / "rear.mp4"
        video.write_bytes(b"h264")
        ctx.add_video(video, channel="camera/rear", frame_timestamps=timestamps)

    run_extractor(emit)

    [entry] = json.loads((output_dir / "manifest.json").read_text())["videoOutputs"]
    assert entry["timestampManifest"]["frameTimestampsRelativePath"] == "rear.mp4.timestamps.json"
    assert json.loads((output_dir / "rear.mp4.timestamps.json").read_text()) == timestamps


def test_manifest_videos_only_finalizes(output_dir: Path, run_extractor: RunExtractor) -> None:
    """An extractor that produces only videos is valid; outputs may be empty."""

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        video = ctx.output_dir / "only.mp4"
        video.write_bytes(b"h264")
        ctx.add_video(video, channel="camera/only", start=1_753_000_000_000_000_000)

    run_extractor(emit)

    manifest = json.loads((output_dir / "manifest.json").read_text())
    assert manifest["outputs"] == []
    assert len(manifest["videoOutputs"]) == 1


def test_manifest_carries_telemetry_and_video_together(output_dir: Path, run_extractor: RunExtractor) -> None:
    """A mixed manifest describes telemetry outputs and videos side by side."""

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        table = ctx.output_dir / "telemetry.parquet"
        table.write_text("rows")
        ctx.add_tabular(table)
        video = ctx.output_dir / "front.mp4"
        video.write_bytes(b"h264")
        ctx.add_video(video, channel="camera/front", start=1_753_000_000_000_000_000)

    run_extractor(emit)

    manifest = json.loads((output_dir / "manifest.json").read_text())
    assert [output["relativePath"] for output in manifest["outputs"]] == ["telemetry.parquet"]
    assert [video["relativePath"] for video in manifest["videoOutputs"]] == ["front.mp4"]


def test_manifest_video_nested_paths_use_forward_slashes(output_dir: Path, run_extractor: RunExtractor) -> None:
    """A nested video and its sidecar are both reported with POSIX separators."""

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        nested = ctx.output_dir / "cams"
        nested.mkdir()
        video = nested / "rear.mp4"
        video.write_bytes(b"h264")
        ctx.add_video(video, channel="camera/rear", frame_timestamps=[1_753_000_000_000_000_000])

    run_extractor(emit)

    [entry] = json.loads((output_dir / "manifest.json").read_text())["videoOutputs"]
    assert entry["relativePath"] == "cams/rear.mp4"
    assert entry["timestampManifest"]["frameTimestampsRelativePath"] == "cams/rear.mp4.timestamps.json"


def test_manifest_video_scale_parameter_reaches_the_entry(output_dir: Path, run_extractor: RunExtractor) -> None:
    """A camera frame rate distinct from the media frame rate is carried on the video entry."""

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        video = ctx.output_dir / "slow.mp4"
        video.write_bytes(b"h264")
        ctx.add_video(video, channel="camera/slow", start=1_753_000_000_000_000_000, true_frame_rate=59.94)

    run_extractor(emit)

    [entry] = json.loads((output_dir / "manifest.json").read_text())["videoOutputs"]
    assert entry["timestampManifest"]["noManifest"]["scaleParameter"]["trueFrameRate"] == 59.94


@pytest.mark.parametrize(
    ("filename", "kwargs", "expected_exception", "expected_match"),
    [
        pytest.param(
            "front.mp4",
            {},
            NominalVideoTimestampModeError,
            "exactly one of 'start' or 'frame_timestamps'",
            id="no-timestamp-mode",
        ),
        pytest.param(
            "front.mp4",
            {"start": 1_753_000_000_000_000_000, "frame_timestamps": [1_753_000_000_000_000_000]},
            NominalVideoTimestampModeError,
            "exactly one of 'start' or 'frame_timestamps'",
            id="both-timestamp-modes",
        ),
        pytest.param(
            "front.mp4", {"frame_timestamps": []}, ExtractorError, "at least one timestamp", id="empty-frame-timestamps"
        ),
        pytest.param(
            "front.mp4",
            {"start": 1_753_000_000_000_000_000, "true_frame_rate": 59.94, "scale_factor": 2.0},
            NominalVideoScaleModeError,
            "at most one of",
            id="two-scale-arguments",
        ),
        pytest.param(
            "front.mp4",
            {"frame_timestamps": [1_753_000_000_000_000_000], "scale_factor": 2.0},
            ExtractorError,
            "apply only to 'start'",
            id="scale-argument-with-frame-timestamps",
        ),
        pytest.param(
            "front.parquet",
            {"start": 1_753_000_000_000_000_000},
            ValueError,
            "must end in one of",
            id="unsupported-extension",
        ),
        pytest.param(
            "manifest.json",
            {"start": 1_753_000_000_000_000_000},
            ExtractorError,
            "is written by the runtime",
            id="reserved-manifest-filename",
        ),
        pytest.param(
            "front.mp4",
            {"channel": "", "start": 1_753_000_000_000_000_000},
            ExtractorError,
            "non-empty channel name",
            id="blank-channel",
        ),
    ],
)
def test_add_video_rejects_invalid_declarations(
    run_extractor: RunExtractor,
    filename: str,
    kwargs: dict[str, object],
    expected_exception: type[Exception],
    expected_match: str,
) -> None:
    """add_video declares nothing it cannot describe: each invalid combination fails at the call.

    The type says which kind of mistake it was -- ExtractorError for the extractor's own contract,
    and the argument errors the rest of the client raises for malformed arguments.
    """

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        video = ctx.output_dir / filename
        video.write_bytes(b"h264")
        ctx.add_video(video, **{"channel": "camera/front", **kwargs})  # type: ignore[call-overload]

    with pytest.raises(expected_exception, match=expected_match):
        run_extractor(emit)


def test_add_video_rejects_file_outside_output_dir(input_dir: Path, run_extractor: RunExtractor) -> None:
    """Only files under the output directory can be ingested."""
    outside = input_dir / "elsewhere.mp4"
    outside.write_bytes(b"h264")

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        ctx.add_video(outside, channel="camera/front", start=1_753_000_000_000_000_000)

    with pytest.raises(ExtractorError, match="not inside the output directory"):
        run_extractor(emit)


def test_add_video_refuses_to_overwrite_an_existing_sidecar(run_extractor: RunExtractor) -> None:
    """The runtime never clobbers a file already sitting where its sidecar goes.

    Only declared outputs belong in the output directory, so such a file is already a contract
    violation -- but silently overwriting it would destroy whatever it was.
    """

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        video = ctx.output_dir / "front.mp4"
        video.write_bytes(b"h264")
        (ctx.output_dir / "front.mp4.timestamps.json").write_text("mine")
        ctx.add_video(video, channel="camera/front", frame_timestamps=[1_753_000_000_000_000_000])

    with pytest.raises(ExtractorError, match="already exists"):
        run_extractor(emit)


def test_one_video_may_be_declared_twice_with_different_frame_timestamps(
    manifest_document: ReadManifest, run_extractor: RunExtractor
) -> None:
    """Sidecars are named for the manifest position, not the video, so one video can carry several.

    This is what the runtime-owned directory buys: two declarations of the same file no longer fight
    over a sidecar path derived from its name.
    """

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        video = ctx.output_dir / "cam.mp4"
        video.write_bytes(b"h264")
        ctx.add_video(video, channel="camera/a", frame_timestamps=[1_753_000_000_000_000_000])
        ctx.add_video(video, channel="camera/b", frame_timestamps=[1_753_000_000_000_000_001])

    run_extractor(emit)

    videos = manifest_document()["videoOutputs"]
    assert [v["relativePath"] for v in videos] == ["cam.mp4", "cam.mp4"]
    assert [v["timestampManifest"]["frameTimestampsRelativePath"] for v in videos] == [
        "cam.mp4.timestamps.json",
        "cam.mp4.timestamps.1.json",
    ]


def test_stray_file_warning_still_fires_alongside_a_video(
    run_extractor: RunExtractor, caplog: pytest.LogCaptureFixture
) -> None:
    """Declaring a video does not excuse undeclared files elsewhere in the output directory."""

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        video = ctx.output_dir / "front.mp4"
        video.write_bytes(b"h264")
        ctx.add_video(video, channel="camera/front", frame_timestamps=[1_753_000_000_000_000_000])
        (ctx.output_dir / "stray.bin").write_text("x")

    with caplog.at_level(logging.WARNING, logger="nominal.experimental.extractor"):
        run_extractor(emit)

    assert "not passed to an add_* method" in caplog.text
    assert "stray.bin" in caplog.text


@pytest.mark.parametrize(
    ("filename", "declare_twice"),
    [
        pytest.param(
            "data.csv",
            lambda ctx, path: (
                ctx.add_tabular(path, timestamp_column="ts1", timestamp_type="epoch_seconds"),
                ctx.add_tabular(path, timestamp_column="ts2", timestamp_type="epoch_microseconds"),
            ),
            id="tabular-twice-different-timestamp-columns",
        ),
        pytest.param(
            "logs.jsonl",
            lambda ctx, path: (
                ctx.add_journal_json(path, timestamp_column="ts", timestamp_type="epoch_seconds"),
                ctx.add_journal_json(path),
            ),
            id="journal-json-twice",
        ),
        pytest.param(
            "cam.mp4",
            lambda ctx, path: (
                ctx.add_video(path, channel="camera/a", start=1_753_000_000_000_000_000),
                ctx.add_video(path, channel="camera/b", start=1_753_000_000_000_000_000),
            ),
            id="video-twice-different-channels",
        ),
    ],
)
def test_one_file_may_be_declared_more_than_once(
    manifest_document: ReadManifest, run_extractor: RunExtractor, filename: str, declare_twice: Any
) -> None:
    """Declaring one file twice is allowed: each declaration is its own manifest entry.

    Ingesting the same table under two timestamp columns is a real thing to want, so the runtime does
    not second-guess it. Declaring one file as two *different* formats is not expressible any more --
    each add_* method checks the extension its format requires.
    """

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        target = ctx.output_dir / filename
        target.write_text("rows")
        declare_twice(ctx, target)

    run_extractor(emit)

    document = manifest_document()
    declared = [entry["relativePath"] for entry in document["outputs"]]
    declared += [video["relativePath"] for video in document["videoOutputs"]]
    assert declared == [filename, filename]


def test_same_filename_in_different_folders_is_not_a_conflict(
    manifest_document: ReadManifest, run_extractor: RunExtractor
) -> None:
    """Outputs are keyed by relative path, so one basename can appear in several folders."""

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        for folder in ("cams", "rear"):
            directory = ctx.output_dir / folder
            directory.mkdir()
            video = directory / "front.mp4"
            video.write_bytes(b"h264")
            ctx.add_video(video, channel=f"camera/{folder}", start=1_753_000_000_000_000_000)

    run_extractor(emit)

    assert [v["relativePath"] for v in manifest_document()["videoOutputs"]] == ["cams/front.mp4", "rear/front.mp4"]


def test_rejected_declaration_does_not_count_as_declared(
    run_extractor: RunExtractor, caplog: pytest.LogCaptureFixture
) -> None:
    """A file whose declaration was rejected is still an undeclared file.

    Resolving a path must not mark it accounted for: an author who catches the error and carries on
    would otherwise leave the file on disk and absent from the manifest, with nothing said about it.
    """

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        rejected = ctx.output_dir / "notavideo.txt"
        rejected.write_text("x")
        try:
            ctx.add_video(rejected, channel="camera/front", start=1_753_000_000_000_000_000)
        except ValueError:  # not a video container
            pass
        declared = ctx.output_dir / "telemetry.parquet"
        declared.write_text("rows")
        ctx.add_tabular(declared)

    with caplog.at_level(logging.WARNING, logger="nominal.experimental.extractor"):
        run_extractor(emit)

    assert "notavideo.txt" in caplog.text


@pytest.mark.parametrize(
    ("timestamp_type", "expected_timestamp_metadata"),
    [
        pytest.param(
            "epoch_microseconds",
            {"seriesName": "timestamps", "epochTimeUnit": "MICROSECONDS", "relativeOffset": None},
            id="epoch",
        ),
        pytest.param(
            ts.Relative("milliseconds", start=datetime(2026, 1, 1, tzinfo=timezone.utc)),
            {
                "seriesName": "timestamps",
                "epochTimeUnit": "MILLISECONDS",
                "relativeOffset": "2026-01-01T00:00:00.000000000Z",
            },
            id="relative",
        ),
        pytest.param(None, None, id="omitted-defers-to-job-level"),
    ],
)
def test_avro_output_declares_how_to_read_its_timestamps(
    manifest_document: ReadManifest,
    run_extractor: RunExtractor,
    timestamp_type: Any,
    expected_timestamp_metadata: dict[str, Any] | None,
) -> None:
    """An avro output declares a timestamp type and no column: the schema fixes which field holds them."""

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        out = ctx.output_dir / "records.avro"
        out.write_bytes(b"avro")
        ctx.add_avro_stream(out, timestamp_type=timestamp_type)

    run_extractor(emit)

    [entry] = manifest_document()["outputs"]
    assert entry["ingestType"] == "AVRO_STREAM"
    assert entry["timestampMetadata"] == expected_timestamp_metadata


@pytest.mark.parametrize(
    ("timestamp_type", "expected_match"),
    [
        pytest.param("iso_8601", "numeric epoch", id="non-numeric-type"),
        pytest.param(ts.Custom("yyyy-DDD HH:mm:ss"), "numeric epoch", id="custom-format"),
        pytest.param(ts.Relative("hours", start=0), "does not support time unit 'hours'", id="unit-outside-contract"),
    ],
)
def test_avro_output_rejects_unexpressible_timestamp_type(
    run_extractor: RunExtractor, timestamp_type: Any, expected_match: str
) -> None:
    """A type the manifest cannot express is refused at the call, not silently narrowed.

    The first two are also type errors under the numeric-only annotation; the runtime guard is what
    protects callers without a type checker, so the test exercises it directly.
    """

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        out = ctx.output_dir / "records.avro"
        out.write_bytes(b"avro")
        ctx.add_avro_stream(out, timestamp_type=timestamp_type)

    with pytest.raises(ExtractorError, match=expected_match):
        run_extractor(emit)


def test_avro_output_composes_channel_prefix_and_timestamp_type(
    manifest_document: ReadManifest, run_extractor: RunExtractor
) -> None:
    """The two avro options are independent and both reach the entry."""

    @manifest_extractor
    def emit(ctx: ManifestExtractorContext) -> None:
        out = ctx.output_dir / "records.avro.gz"
        out.write_bytes(b"avro")
        ctx.add_avro_stream(out, channel_prefix="sensors/", timestamp_type=ts.Epoch("seconds"))

    run_extractor(emit)

    [entry] = manifest_document()["outputs"]
    assert entry["channelPrefix"] == "sensors/"
    assert entry["timestampMetadata"]["epochTimeUnit"] == "SECONDS"
