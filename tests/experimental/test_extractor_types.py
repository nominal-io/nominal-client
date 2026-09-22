"""Parameter conversion constraints and author-safe diagnostics."""

from dataclasses import FrozenInstanceError

import pytest

from nominal.experimental import extractor as ex


def run_parameter(tmp_path, declaration, raw=None):
    received = []

    @ex.single_file_extractor
    @declaration
    def extract(ctx, value):
        received.append(value)
        output = ctx.output_dir / "result.csv"
        output.write_text("time,value\n0,1\n")
        ctx.set_output(output)

    env = {"OUTPUT_DIR": str(tmp_path)}
    if raw is not None:
        env["VALUE"] = raw
    extract.run(env=env, exit=False)
    return received[0]


@pytest.mark.parametrize("default,raw,expected", [(2, "3", 3), (1.5, "2.5", 2.5), (True, "OFF", False)])
def test_inferred_type_matches_concrete_default(tmp_path, default, raw, expected):
    """Primitive defaults select conversion while remaining unchanged when no value is supplied."""
    declaration = ex.parameter("value", default=default)
    assert run_parameter(tmp_path, declaration) is default
    actual = run_parameter(tmp_path, declaration, raw)
    assert actual == expected
    assert type(actual) is type(expected)


def test_explicit_converter_wins_without_executing_on_default(tmp_path):
    """Custom converters process supplied strings only and explicit type selection takes precedence."""
    calls = []
    default = object()

    def converter(raw):
        calls.append(raw)
        return len(raw)

    declaration = ex.parameter("value", type=converter, default=default)
    assert run_parameter(tmp_path, declaration) is default
    assert calls == []
    assert run_parameter(tmp_path, declaration, "123") == 3
    assert calls == ["123"]
    assert run_parameter(tmp_path, ex.parameter("value", type=str, default=None), "3") == "3"


@pytest.mark.parametrize("default", [None, pytest.param("fallback", id="string")])
def test_string_inference(tmp_path, default):
    """Absent or string defaults leave supplied values as strings."""
    assert run_parameter(tmp_path, ex.parameter("value", default=default), "4") == "4"
    assert run_parameter(tmp_path, ex.parameter("value"), "4") == "4"


def test_choice_snapshots_values_and_is_case_sensitive():
    """Choices snapshot their configuration and compare strings with exact case."""
    values = ["fast", "precise"]
    choice = ex.Choice(values)
    values.append("other")
    assert choice("fast") == "fast"
    with pytest.raises(ex.BadParameter, match="fast.*precise"):
        choice("FAST")
    with pytest.raises(ex.BadParameter):
        choice("other")
    with pytest.raises(FrozenInstanceError):
        choice.choices = ("other",)


@pytest.mark.parametrize("choices", [[], ["a", "a"], ["a", 2], "abc"])
def test_invalid_choices_rejected(choices):
    """Choices reject empty, duplicate, non-string, and bare-string configurations."""
    with pytest.raises((TypeError, ValueError)):
        ex.Choice(choices)


@pytest.mark.parametrize("kind", [ex.IntRange, ex.FloatRange])
def test_ranges_are_inclusive_and_immutable(kind):
    """Range converters accept their boundaries and reject out-of-range or malformed values."""
    converter = kind(min=1, max=3)
    assert converter("1") == 1
    assert converter("3") == 3
    assert converter("2") == 2
    for value in ("0", "4", "nan", "inf", "private-invalid-value"):
        with pytest.raises(ex.BadParameter) as error:
            converter(value)
        if value == "private-invalid-value":
            assert value not in str(error.value)
    with pytest.raises(FrozenInstanceError):
        converter.min = 0
    assert kind(min=1)("4") == 4
    assert kind(max=3)("-1") == -1


@pytest.mark.parametrize(
    "kind,kwargs",
    [
        (ex.IntRange, {"min": 3, "max": 1}),
        (ex.IntRange, {"min": 1.5}),
        (ex.IntRange, {"max": True}),
        (ex.FloatRange, {"min": float("nan")}),
        (ex.FloatRange, {"max": float("inf")}),
        (ex.FloatRange, {"min": True}),
        (ex.FloatRange, {"min": 3, "max": 1}),
    ],
)
def test_invalid_bounds_rejected(kind, kwargs):
    """Malformed range declarations raise author-facing TypeError or ValueError, not BadParameter."""
    with pytest.raises((TypeError, ValueError)) as error:
        kind(**kwargs)
    assert type(error.value) in (TypeError, ValueError)


@pytest.mark.parametrize(
    "kind,default",
    [
        (int, True),
        (int, "2"),
        (float, True),
        (float, float("nan")),
        (bool, 1),
        (str, 3),
        (ex.IntRange(min=1), 0),
        (ex.IntRange(min=1), True),
        (ex.FloatRange(min=1), float("inf")),
        (ex.Choice(["fast", "precise"]), "other"),
    ],
)
def test_invalid_typed_defaults_rejected_at_declaration(kind, default):
    """Invalid defaults identify the offending parameter before extraction starts."""
    with pytest.raises((TypeError, ValueError), match="parameter.*value"):
        ex.parameter("value", type=kind, default=default)


def test_optional_and_numeric_defaults_remain_typed(tmp_path):
    """Optional defaults stay None and numeric defaults are not reconverted."""
    for converter in (ex.IntRange(min=1), ex.FloatRange(max=3), ex.Choice(["a"])):
        assert run_parameter(tmp_path, ex.parameter("value", type=converter, default=None)) is None
    assert run_parameter(tmp_path, ex.parameter("value", type=ex.FloatRange(min=1), default=2)) == 2


def test_safe_diagnostic_includes_binding_context(tmp_path):
    """BadParameter exposes its safe diagnostic alongside the argument and environment names."""

    def converter(raw):
        raise ex.BadParameter("must be a positive count")

    with pytest.raises(ex.ExtractorError, match="value.*VALUE.*must be a positive count") as error:
        run_parameter(tmp_path, ex.parameter("value", type=converter), "private-input")
    assert "private-input" not in str(error.value)


@pytest.mark.parametrize("exception", [ValueError, TypeError])
def test_arbitrary_converter_diagnostic_is_sanitized(tmp_path, exception):
    """ValueError and TypeError conversion failures never echo supplied values."""

    def converter(raw):
        raise exception(raw)

    with pytest.raises(ex.ExtractorError) as error:
        run_parameter(tmp_path, ex.parameter("value", type=converter), "private-input")
    assert "private-input" not in str(error.value)


@pytest.mark.parametrize("raw", ["nan", "inf", "-inf"])
def test_float_rejects_nonfinite_supplied_values_like_defaults(tmp_path, raw):
    """Plain float conversion rejects NaN and infinities just as default validation does."""
    with pytest.raises(ex.ExtractorError, match="finite number"):
        run_parameter(tmp_path, ex.parameter("value", type=float), raw)
