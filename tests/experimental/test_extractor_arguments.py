"""Declarative runtime arguments and legacy migration behavior."""

import json
import logging
from functools import wraps
from pathlib import Path

import pytest

from nominal.experimental import extractor as ex


def write_output(ctx, text):
    output = ctx.output_dir / "data.csv"
    output.write_text(text)
    if isinstance(ctx, ex.ManifestExtractorContext):
        ctx.add_tabular(output)
    else:
        ctx.set_output(output)


@pytest.mark.parametrize("outer", [ex.manifest_extractor, ex.single_file_extractor])
def test_arguments_resolve_on_each_run(outer, tmp_path, caplog):
    source = tmp_path / "recording.txt"
    source.write_text("recording")

    calls = []

    def track_calls(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            calls.append(kwargs)
            fn(*args, **kwargs)

        return wrapped

    @outer
    @ex.error(ValueError, code="INPUT", exit_code=64)
    @track_calls
    @ex.input("recording")
    @ex.parameter("parts", type=int, default=2)
    def extract(ctx, *, recording: Path, parts: int):
        write_output(ctx, f"{recording.read_text()}:{parts}")

    log = tmp_path / "termination"
    for value in ("3", "4", None):
        env = {"OUTPUT_DIR": str(tmp_path), "RECORDING": str(source)}
        if value is not None:
            env["PARTS"] = value
        extract.run(env=env, exit=False, termination_log_path=log)
        assert (tmp_path / "data.csv").read_text() == f"recording:{value or 2}"
    assert [call["parts"] for call in calls] == [3, 4, 2]
    assert not log.exists()
    assert "legacy" not in caplog.text


@pytest.mark.parametrize("raw,expected", [("false", False), ("0", False), ("OFF", False), ("yes", True), ("1", True)])
def test_boolean_conversion(tmp_path, raw, expected):
    @ex.single_file_extractor
    @ex.parameter("enabled", type=bool)
    def extract(ctx, enabled):
        assert enabled is expected
        write_output(ctx, "done")

    extract.run(env={"OUTPUT_DIR": str(tmp_path), "ENABLED": raw}, exit=False)


@pytest.mark.parametrize("converter,raw", [(int, None), (int, ""), (int, "secret-invalid-value"), (bool, "neither")])
def test_binding_failure_precedes_callback_and_can_be_mapped(tmp_path, converter, raw, capsys):
    called = []

    @ex.manifest_extractor
    @ex.error(ex.ExtractorError, code="ARGUMENT", exit_code=64)
    @ex.parameter("parts", type=converter)
    def extract(ctx, parts):
        called.append(parts)

    log = tmp_path / "termination"
    env = {"OUTPUT_DIR": str(tmp_path)}
    if raw is not None:
        env["PARTS"] = raw
    with pytest.raises(ex.ExtractorError, match="parts|PARTS"):
        extract.run(env=env, exit=False, termination_log_path=log)
    assert not log.exists()
    with pytest.raises(SystemExit) as error:
        extract.run(env=env, termination_log_path=log)
    assert error.value.code == 64
    assert json.loads(log.read_text())["code"] == "ARGUMENT"
    stderr = capsys.readouterr().err
    if raw:
        assert raw not in stderr
    assert not called
    assert not (tmp_path / "manifest.json").exists()


def test_optional_inputs_and_none_parameter(tmp_path):
    @ex.single_file_extractor
    @ex.input("calibration", default=None)
    @ex.parameter("offset", type=int, default=None)
    def extract(ctx, calibration, offset):
        assert calibration is None
        assert offset is None
        write_output(ctx, "done")

    extract.run(env={"OUTPUT_DIR": str(tmp_path), "_NOMINAL_INPUTS": "[]"}, exit=False)
    with pytest.raises(ex.ExtractorError, match="calibration|CALIBRATION"):
        extract.run(env={"OUTPUT_DIR": str(tmp_path), "CALIBRATION": str(tmp_path / "missing")}, exit=False)


def test_metadata_is_authoritative(tmp_path):
    @ex.single_file_extractor
    @ex.parameter("parts", default="2")
    def extract(ctx, parts):
        write_output(ctx, parts)

    with pytest.raises(ex.ExtractorError, match="unknown parameter"):
        extract.run(env={"OUTPUT_DIR": str(tmp_path), "_NOMINAL_PARAMETERS": "[]", "PARTS": "4"}, exit=False)


def test_registered_input_path_and_envvar_override(tmp_path):
    source = tmp_path / "source"
    source.write_text("data")

    @ex.single_file_extractor
    @ex.input("recording", envvar="SOURCE")
    @ex.parameter("label", envvar="LABEL", default="fallback")
    def extract(ctx, recording, label):
        assert recording == source
        assert label == ""  # An empty supplied string is not absent.
        write_output(ctx, recording.read_text())

    extract.run(
        env={
            "OUTPUT_DIR": str(tmp_path),
            "LABEL": "",
            "SOURCE": "/wrong",
            "_NOMINAL_INPUTS": json.dumps(
                [{"name": "Recording", "environmentVariable": "SOURCE", "path": str(source)}]
            ),
        },
        exit=False,
    )


def test_legacy_warning_once_per_run(tmp_path, caplog):
    for method in (
        ex.ExtractorContext.inputs.fget,
        ex.ExtractorContext.input,
        ex.ExtractorContext.param,
        ex.ExtractorContext.get_param,
    ):
        assert "declare" in vars(method)["__deprecated__"]

    @ex.single_file_extractor
    def extract(ctx):
        ctx.inputs
        ctx.get_param("PARTS", "2")
        ctx.param("PARTS")
        write_output(ctx, "done")

    with caplog.at_level(logging.WARNING):
        for _ in range(2):
            ctx = extract.run(env={"OUTPUT_DIR": str(tmp_path), "PARTS": "3"}, exit=False)
            ctx.get_param("PARTS")
    warnings = [record for record in caplog.records if "legacy" in record.message]
    assert len(warnings) == 2
    assert "@input" in warnings[0].message


def test_mixed_arguments_warn_only_for_legacy_access(tmp_path, caplog):
    @ex.single_file_extractor
    @ex.parameter("parts", default="2")
    def extract(ctx, parts):
        write_output(ctx, parts + ctx.get_param("LABEL", "legacy"))

    extract.run(env={"OUTPUT_DIR": str(tmp_path)}, exit=False)
    assert sum("legacy" in record.message for record in caplog.records) == 1


@pytest.mark.parametrize(
    "decorate",
    [
        lambda fn: ex.parameter("missing")(fn),
        lambda fn: ex.parameter("ctx")(fn),
        lambda fn: ex.parameter("parts")(ex.parameter("parts")(fn)),
    ],
)
def test_invalid_bindings(decorate):
    def extract(ctx, parts, recording=None):
        pass

    with pytest.raises((TypeError, ValueError)):
        ex.manifest_extractor(decorate(extract))


@pytest.mark.parametrize("envvar", ["OUTPUT_DIR", "NOMINAL_EXTRACTOR_INPUT_DIR", "_NOMINAL_TEST", "bad-name", ""])
def test_invalid_environment_names(envvar):
    with pytest.raises(ValueError, match="env"):
        ex.parameter("parts", envvar=envvar)


def test_signature_defaults_and_undeclared_required_arguments():
    with pytest.raises(TypeError, match="default"):
        ex.manifest_extractor(ex.parameter("parts")(lambda ctx, parts=2: None))
    with pytest.raises(TypeError, match="binding|argument"):
        ex.manifest_extractor(ex.parameter("parts")(lambda ctx, parts, other: None))


@pytest.mark.parametrize("wrapped", [False, True])
def test_wrong_decorator_order(wrapped):
    entrypoint = ex.manifest_extractor(lambda ctx: None)
    if wrapped:

        @wraps(entrypoint)
        def callback(*args, **kwargs):
            return entrypoint(*args, **kwargs)
    else:
        callback = entrypoint
    with pytest.raises(TypeError, match="outermost"):
        ex.parameter("parts")(callback)


@pytest.mark.parametrize("kind", ["parameter", "input"])
def test_display_names_cannot_shadow_declarative_envvars(kind, tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.write_text("first")
    second.write_text("second")

    @ex.single_file_extractor
    @getattr(ex, kind)("first", name="SECOND")
    @getattr(ex, kind)("second")
    def extract(ctx, first, second):
        assert first != second
        write_output(ctx, "done")

    specs = [
        {"name": "SECOND", "environmentVariable": "FIRST", "path": str(first), "required": True},
        {"name": "second", "environmentVariable": "SECOND", "path": str(second), "required": True},
    ]
    metadata_key = "_NOMINAL_INPUTS" if kind == "input" else "_NOMINAL_PARAMETERS"
    extract.run(
        env={"OUTPUT_DIR": str(tmp_path), "FIRST": str(first), "SECOND": str(second), metadata_key: json.dumps(specs)},
        exit=False,
    )


def test_direct_call_binds_without_finalizing(tmp_path, caplog):
    @ex.manifest_extractor
    @ex.parameter("value", type=int, default=5)
    def extract(ctx, value):
        write_output(ctx, str(value))

    ctx = extract._build_context({"OUTPUT_DIR": str(tmp_path)})
    extract(ctx)
    assert (tmp_path / "data.csv").read_text() == "5"
    assert not (tmp_path / "manifest.json").exists()
    assert "legacy" not in caplog.text


def test_registered_missing_input_does_not_fall_back_to_environment(tmp_path):
    @ex.manifest_extractor
    @ex.input("source")
    def extract(ctx, source):
        raise AssertionError("must not call")

    source = tmp_path / "source"
    source.write_text("data")
    with pytest.raises(ex.ExtractorError, match="not among"):
        extract.run(env={"OUTPUT_DIR": str(tmp_path), "SOURCE": str(source), "_NOMINAL_INPUTS": "[]"}, exit=False)


@pytest.mark.parametrize("callback", [lambda ctx, value, /: None, lambda **kwargs: None])
def test_positional_only_and_catchall_targets_are_rejected(callback):
    with pytest.raises(TypeError):
        ex.manifest_extractor(ex.parameter("value")(callback))


def test_duplicate_display_names_are_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        ex.manifest_extractor(
            ex.input("first", name="File")(ex.input("second", name="File")(lambda ctx, first, second: None))
        )


def test_duplicate_environment_variables_are_rejected():
    def extract(ctx, parts, recording):
        pass

    decorated = ex.parameter("parts", envvar="SHARED")(ex.input("recording", envvar="SHARED")(extract))
    with pytest.raises(ValueError, match="duplicate.*envvar"):
        ex.manifest_extractor(decorated)


def test_debug_logging_traces_execution_without_parameter_values(tmp_path, caplog):
    @ex.single_file_extractor
    @ex.parameter("secret", default="private-default")
    def extract(ctx, secret):
        write_output(ctx, "done")

    with caplog.at_level(logging.DEBUG, logger="nominal.experimental.extractor"):
        extract.run(env={"OUTPUT_DIR": str(tmp_path)}, exit=False)
        extract.run(env={"OUTPUT_DIR": str(tmp_path), "SECRET": "private-supplied"}, exit=False)
    for event in (
        "binding argument secret",
        "using default",
        "converting supplied parameter",
        "invoking callback",
        "finalizing outputs",
    ):
        assert event in caplog.text
    assert "private-default" not in caplog.text
    assert "private-supplied" not in caplog.text
