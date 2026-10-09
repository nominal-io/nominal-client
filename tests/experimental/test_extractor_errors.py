import inspect
import json
from functools import wraps
from pathlib import Path

import pytest

from nominal.experimental import extractor as ex
from nominal.experimental.extractor import (
    ExtractorContext,
    ExtractorError,
    manifest_extractor,
    single_file_extractor,
)


@pytest.mark.parametrize("decorator", [manifest_extractor, single_file_extractor])
def test_mapped_error(decorator, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Both decorators write structured error metadata and use the mapped exit status."""

    @decorator
    @ex.error(ValueError, code="BAD_INPUT", exit_code=65, retryable=True, message="Static catalog fallback")
    def extract(ctx: ExtractorContext) -> None:
        raise ValueError('bad "input"\nwith unicode: λ')

    log = tmp_path / "termination"
    with pytest.raises(SystemExit) as exc:
        extract.run(env={"OUTPUT_DIR": str(tmp_path)}, termination_log_path=log)
    assert exc.value.code == 65
    payload = {"code": "BAD_INPUT", "message": 'bad "input"\nwith unicode: λ', "retryable": True}
    assert json.loads(log.read_text()) == payload
    assert json.loads(capsys.readouterr().err.splitlines()[0]) == payload


@pytest.mark.parametrize("stage", ["startup", "finalization"])
def test_framework_errors_are_mapped(stage: str, tmp_path: Path) -> None:
    """Startup and output-finalization contract failures use the configured mappings."""

    @manifest_extractor
    @ex.error(ExtractorError, code="CONFIGURATION", exit_code=64)
    def extract(ctx: ExtractorContext) -> None:
        pass  # No outputs: finalization fails.

    env = {}
    if stage == "finalization":
        env["OUTPUT_DIR"] = str(tmp_path)
    with pytest.raises(SystemExit) as exc:
        extract.run(env=env, termination_log_path=tmp_path / "termination")
    assert exc.value.code == 64


def test_exit_false_reraises_without_reporting(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Tests receive the original exception without termination output when exit is False."""
    error = ValueError("original")

    @manifest_extractor
    @ex.error(ValueError, code="INPUT", exit_code=64)
    def extract(ctx: ExtractorContext) -> None:
        raise error

    log = tmp_path / "termination"
    with pytest.raises(ValueError) as exc:
        extract.run(env={"OUTPUT_DIR": str(tmp_path)}, termination_log_path=log, exit=False)
    assert exc.value is error
    assert not log.exists()
    assert capsys.readouterr().err == ""


def test_unwritable_log_preserves_exit_and_stderr(tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog) -> None:
    """A failed termination-file write still preserves structured stderr and the mapped status."""

    @manifest_extractor
    @ex.error(ValueError, code="INPUT", exit_code=64)
    def extract(ctx: ExtractorContext) -> None:
        raise ValueError("invalid")

    with caplog.at_level("DEBUG", logger="nominal.experimental.extractor"), pytest.raises(SystemExit) as exc:
        extract.run(env={"OUTPUT_DIR": str(tmp_path)}, termination_log_path=tmp_path)
    assert "mapped ValueError to code INPUT, exit 64" in caplog.text
    assert "termination log write failed" in caplog.text
    assert exc.value.code == 64
    assert json.loads(capsys.readouterr().err.splitlines()[0])["code"] == "INPUT"


@pytest.mark.parametrize("error", [RuntimeError("bug"), SystemExit(0), KeyboardInterrupt()])
def test_unmapped_errors_keep_existing_behavior(error: BaseException, tmp_path: Path, capsys) -> None:
    """Unmapped errors and process-control exceptions keep traceback reporting and exit status 1."""

    @manifest_extractor
    @ex.error(ValueError, code="INPUT", exit_code=64)
    def extract(ctx: ExtractorContext) -> None:
        raise error

    log = tmp_path / "termination"
    with pytest.raises(SystemExit) as exc:
        extract.run(env={"OUTPUT_DIR": str(tmp_path)}, termination_log_path=log)
    assert exc.value.code == 1
    assert not log.exists()
    assert "Traceback" in capsys.readouterr().err


@pytest.mark.parametrize("exit_code", [0, -1, 256, True, 1.5])
def test_invalid_exit_code(exit_code) -> None:
    """Mapping status codes must be non-boolean integers in the process exit range."""
    with pytest.raises(ValueError, match="exit_code"):
        ex.error(ValueError, code="INPUT", exit_code=exit_code)


@pytest.mark.parametrize("code", ["", "  "])
def test_empty_code(code: str) -> None:
    """Catalog error codes cannot be empty or whitespace-only."""
    with pytest.raises(ValueError, match="code"):
        ex.error(ValueError, code=code, exit_code=64)


@pytest.mark.parametrize("exception_type", [BaseException, object, ValueError("instance")])
def test_invalid_error_exception_type(exception_type) -> None:
    """Error declarations accept only Exception subclasses."""
    with pytest.raises(TypeError, match="Exception"):
        ex.error(exception_type, code="INPUT", exit_code=64)


def test_invalid_retryable() -> None:
    """Retryability is a boolean rather than a truthy string."""
    with pytest.raises(ValueError, match="retryable"):
        ex.error(ValueError, code="INPUT", exit_code=64, retryable="false")


@pytest.mark.parametrize("message", ["x" * 5000, '"\\\n\tλ🙂' * 2000], ids=["ascii", "escaped-unicode"])
def test_long_error_keeps_valid_bounded_json(message, tmp_path, capsys):
    """Long and escaped messages preserve complete JSON, code, and retryable metadata."""

    @manifest_extractor
    @ex.error(ValueError, code="INPUT", exit_code=65, retryable=True)
    def extract(ctx):
        raise ValueError(message)

    log = tmp_path / "termination"
    with pytest.raises(SystemExit) as error:
        extract.run(env={"OUTPUT_DIR": str(tmp_path)}, termination_log_path=log)
    assert error.value.code == 65
    assert len(log.read_bytes()) <= 4096
    payload = json.loads(log.read_text())
    assert payload["code"] == "INPUT"
    assert payload["retryable"] is True
    assert payload["message"] and message.startswith(payload["message"])
    assert len(payload["message"]) < len(message)
    stderr = capsys.readouterr().err
    assert json.loads(stderr.splitlines()[0]) == payload
    assert "Traceback" in stderr
    assert "test_extractor_errors.py" in stderr


@pytest.mark.parametrize("code", ["x" * 4096, "λ" * 1000], ids=["ascii", "unicode"])
def test_error_code_must_fit_termination_envelope(code):
    """Reject mappings whose code alone would exceed the complete termination JSON budget."""
    with pytest.raises(ValueError, match="code.*4096"):
        ex.error(ValueError, code=code, exit_code=65)


def test_environment_cannot_redirect_termination_output(tmp_path, monkeypatch):
    """Only the explicit run option chooses the path; neither environment mapping may overwrite a file."""
    ambient = tmp_path / "ambient"
    supplied = tmp_path / "supplied"
    actual = tmp_path / "termination"
    ambient.write_text("keep ambient")
    supplied.write_text("keep supplied")
    monkeypatch.setenv("TERMINATION_LOG_PATH", str(ambient))

    @manifest_extractor
    @ex.error(ValueError, code="INPUT", exit_code=65)
    def extract(ctx):
        raise ValueError("invalid")

    with pytest.raises(SystemExit):
        extract.run(
            env={"OUTPUT_DIR": str(tmp_path), "TERMINATION_LOG_PATH": str(supplied)}, termination_log_path=actual
        )
    assert ambient.read_text() == "keep ambient"
    assert supplied.read_text() == "keep supplied"
    assert json.loads(actual.read_text())["code"] == "INPUT"


def test_termination_budget_boundary(tmp_path):
    """A message at the byte limit is preserved; the next byte is removed without changing metadata."""
    empty = json.dumps({"code": "INPUT", "message": "", "retryable": False})
    budget = 4096 - len(empty.encode("utf-8"))
    for length in (budget - 1, budget, budget + 1):
        message = "x" * length

        @manifest_extractor
        @ex.error(ValueError, code="INPUT", exit_code=65)
        def extract(ctx):
            raise ValueError(message)

        log = tmp_path / "termination"
        with pytest.raises(SystemExit):
            extract.run(env={"OUTPUT_DIR": str(tmp_path)}, termination_log_path=log)
        assert json.loads(log.read_text())["message"] == message[:budget]
        assert len(log.read_bytes()) == min(4096, len(empty.encode("utf-8")) + length)


def test_code_can_fill_the_entire_termination_budget(tmp_path):
    """A valid maximal code leaves an empty message while preserving the error identity."""
    overhead = len(json.dumps({"code": "", "message": "", "retryable": False}).encode("utf-8"))
    code = "x" * (4096 - overhead)

    @manifest_extractor
    @ex.error(ValueError, code=code, exit_code=65)
    def extract(ctx):
        raise ValueError("no space for this message")

    log = tmp_path / "termination"
    with pytest.raises(SystemExit):
        extract.run(env={"OUTPUT_DIR": str(tmp_path)}, termination_log_path=log)
    assert len(log.read_bytes()) == 4096
    assert json.loads(log.read_text()) == {"code": code, "message": "", "retryable": False}


def test_duplicate_error_declarations_are_rejected() -> None:
    """Two declarations for the same exception class cannot silently override one another."""
    with pytest.raises(ValueError, match="duplicate"):

        @manifest_extractor
        @ex.error(ValueError, code="FIRST", exit_code=64)
        @ex.error(ValueError, code="SECOND", exit_code=65)
        def extract(ctx: ExtractorContext) -> None:
            pass


def test_error_decorator_requires_outer_extractor() -> None:
    """Error declarations belong below the outer extractor decorator."""
    with pytest.raises(TypeError, match="outermost"):

        @ex.error(ValueError, code="INPUT", exit_code=64)
        @manifest_extractor
        def extract(ctx: ExtractorContext) -> None:
            pass


def test_error_declarations_are_snapshotted_and_keep_callback_metadata(tmp_path) -> None:
    """Adding a declaration later does not change a previously constructed extractor."""

    @ex.error(Exception, code="GENERAL", exit_code=64)
    def extract(ctx: ExtractorContext) -> None:
        """Example callback."""
        raise OSError("disk")

    @wraps(extract)
    def wrapped(ctx: ExtractorContext) -> None:
        extract(ctx)

    first = manifest_extractor(wrapped)
    second = manifest_extractor(ex.error(OSError, code="IO", exit_code=74)(wrapped))
    assert inspect.signature(first) == inspect.signature(second) == inspect.signature(extract)
    assert first.__name__ == second.__name__ == "extract"
    assert first.__doc__ == second.__doc__ == "Example callback."
    for entrypoint, status in [(first, 64), (second, 74)]:
        with pytest.raises(SystemExit) as raised:
            entrypoint.run(env={"OUTPUT_DIR": str(tmp_path)}, termination_log_path=tmp_path / "termination")
        assert raised.value.code == status


@pytest.mark.parametrize("specific_first", [False, True])
def test_error_matching_is_independent_of_declaration_order(specific_first, tmp_path) -> None:
    """Broad mappings cannot shadow a closer exception type in either decorator order."""

    class SpecificError(ValueError):
        pass

    def extract(ctx: ExtractorContext) -> None:
        raise SpecificError("invalid")

    declarations = [
        ex.error(Exception, code="GENERAL", exit_code=64),
        ex.error(ValueError, code="INPUT", exit_code=65),
    ]
    if specific_first:
        declarations.reverse()
    for declaration in declarations:
        extract = declaration(extract)
    entrypoint = manifest_extractor(extract)
    with pytest.raises(SystemExit) as raised:
        entrypoint.run(env={"OUTPUT_DIR": str(tmp_path)}, termination_log_path=tmp_path / "termination")
    assert raised.value.code == 65
