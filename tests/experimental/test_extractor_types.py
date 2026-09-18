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
    declaration = ex.parameter("value", default=default)
    assert run_parameter(tmp_path, declaration) is default
    actual = run_parameter(tmp_path, declaration, raw)
    assert actual == expected
    assert type(actual) is type(expected)


def test_explicit_converter_wins_without_executing_on_default(tmp_path):
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
    assert run_parameter(tmp_path, ex.parameter("value", default=default), "4") == "4"
    assert run_parameter(tmp_path, ex.parameter("value"), "4") == "4"


def test_choice_snapshots_values_and_is_case_sensitive():
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
    with pytest.raises((TypeError, ValueError)):
        ex.Choice(choices)


@pytest.mark.parametrize("kind", [ex.IntRange, ex.FloatRange])
def test_ranges_are_inclusive_and_immutable(kind):
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
    with pytest.raises((TypeError, ValueError)):
        kind(**kwargs)


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
    with pytest.raises((TypeError, ValueError)):
        ex.parameter("value", type=kind, default=default)


def test_optional_and_numeric_defaults_remain_typed(tmp_path):
    for converter in (ex.IntRange(min=1), ex.FloatRange(max=3), ex.Choice(["a"])):
        assert run_parameter(tmp_path, ex.parameter("value", type=converter, default=None)) is None
    assert run_parameter(tmp_path, ex.parameter("value", type=ex.FloatRange(min=1), default=2)) == 2


def test_safe_diagnostic_includes_binding_context(tmp_path):
    def converter(raw):
        raise ex.BadParameter("must be a positive count")

    with pytest.raises(ex.ExtractorError, match="value.*VALUE.*must be a positive count") as error:
        run_parameter(tmp_path, ex.parameter("value", type=converter), "private-input")
    assert "private-input" not in str(error.value)


@pytest.mark.parametrize("exception", [ValueError, TypeError])
def test_arbitrary_converter_diagnostic_is_sanitized(tmp_path, exception):
    def converter(raw):
        raise exception(raw)

    with pytest.raises(ex.ExtractorError) as error:
        run_parameter(tmp_path, ex.parameter("value", type=converter), "private-input")
    assert "private-input" not in str(error.value)


@pytest.mark.parametrize("raw", ["nan", "inf", "-inf"])
def test_float_rejects_nonfinite_supplied_values_like_defaults(tmp_path, raw):
    with pytest.raises(ex.ExtractorError, match="finite number"):
        run_parameter(tmp_path, ex.parameter("value", type=float), raw)
