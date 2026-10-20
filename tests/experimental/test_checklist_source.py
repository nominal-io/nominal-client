"""Source publication contracts using the installed nominal-compute compiler."""

import json
from dataclasses import replace

import pytest
from conjure_python_client import ConjureDecoder
from nominal_api import scout_compute_api

import nominal.compute as nc
from nominal.compute.compiler import compile_expression
from nominal.core import Priority
from nominal.experimental.compute_as_code import ChannelBinding, CheckDefinition, ChecklistBuilder


def test_source_and_typed_bindings_are_saved_together() -> None:
    """Source, compiled graph, typed input variables, and the check reference are published together."""
    source = "hot = temperature > 100\nhot.ranges()"
    definition = ChecklistBuilder().add(
        CheckDefinition("Hot", source, priority=Priority.P1),
        inputs={"temperature": ChannelBinding("telemetry", "temperature_deg")},
    )
    inputs, expression = definition._variables
    assert inputs.value.compute_node.series_node.numeric is not None
    assert inputs.value.compute_node.context.variables["raw"].series.channel == "temperature_deg"
    assert expression.compute_expression.v1.python.expression == source
    assert expression.value.compute_node.context.variables["temperature"].checklist_variable == inputs.name
    compiled = compile_expression(source, references={"temperature": nc.NumericSeries.Reference("temperature")})
    assert expression.value.compute_node.series_node.ranges == ConjureDecoder.do_decode(
        json.loads(compiled.value.to_json()), scout_compute_api.RangeSeries
    )
    condition = definition._checks[0].condition.num_ranges_v3
    assert condition.variables["failure"].checklist_variable == expression.name
    assert condition.ranges.raw.name == "failure"


@pytest.mark.parametrize(
    ("source", "error", "message"),
    [
        ("(temperature_deg > 100).ranges()", ValueError, "undefined name 'temperature_deg'"),
        ("(temperature >", ValueError, "Failed to parse"),
        ("temperature", TypeError, "must produce a RangeSeries"),
        ("NumericSeries.Reference('missing').threshold_ranges().gt(1)", ValueError, "missing input bindings: missing"),
    ],
)
def test_failed_source_does_not_change_builder(source: str, error: type[Exception], message: str) -> None:
    """Each source failure reports its specific error and leaves no partially added check."""
    builder = ChecklistBuilder(strict=True)
    inputs = {"temperature": ChannelBinding("telemetry", "temperature")}
    with pytest.raises(error, match=message):
        builder.add(CheckDefinition("Invalid", source, priority=Priority.P1), inputs=inputs)
    builder.add(CheckDefinition("Valid", "(temperature > 100).ranges()", priority=Priority.P1), inputs=inputs)
    assert len(builder._checks) == 1
    assert [v.name for v in builder._variables] == ["a", "b"]


def test_source_namespacing_and_unused_bindings() -> None:
    """Check inputs are namespaced independently and strict mode rejects unused bindings."""
    inputs = {"temperature": ChannelBinding("telemetry", "temperature"), "unused": ChannelBinding("telemetry", "other")}
    builder = ChecklistBuilder()
    for key in ["first", "second"]:
        builder.add(CheckDefinition(key, "(temperature > 100).ranges()", key=key, priority=Priority.P1), inputs=inputs)
    definition = builder
    assert len({variable.name for variable in definition._variables}) == 4
    with pytest.raises(ValueError, match="unused input bindings: unused"):
        ChecklistBuilder(strict=True).add(
            CheckDefinition("Hot", "(temperature > 100).ranges()", priority=Priority.P1), inputs=inputs
        )


def test_categorical_source_input() -> None:
    """Categorical bindings supply matching compiler and frontend input types."""
    definition = ChecklistBuilder().add(
        CheckDefinition("Fault", 'mode.category_ranges().eq("FAULT")', priority=Priority.P1),
        inputs={"mode": ChannelBinding("telemetry", "mode", series_type="categorical")},
    )
    assert definition._variables[0].value.compute_node.series_node.enum is not None


def test_reusable_check_is_bound_independently() -> None:
    """A reusable source definition captures separate bindings without acquiring remote state."""
    check = CheckDefinition(title="Hot", expression="(temperature > 100).ranges()", priority=Priority.P1)
    tags = {"side": "left"}
    left = ChecklistBuilder().add(
        replace(check, key="hot.left"), inputs={"temperature": ChannelBinding("telemetry", "temperature", tags=tags)}
    )
    tags["side"] = "right"
    right = ChecklistBuilder().add(
        replace(check, key="hot.right"), inputs={"temperature": ChannelBinding("telemetry", "temperature", tags=tags)}
    )
    assert left._variables[0].value.compute_node.context.variables["raw"].series.tags == {"side": "left"}
    assert right._variables[0].value.compute_node.context.variables["raw"].series.tags == {"side": "right"}
    assert left._checks[0].check_lineage_rid != right._checks[0].check_lineage_rid
    assert check.key is None


def test_source_only_input_rejects_graph_objects() -> None:
    """The authoring API has one source-preserving path, even for untyped callers."""
    check = CheckDefinition("Hot", (nc.NumericSeries.Reference("x") > 1).ranges(), priority=Priority.P1)
    with pytest.raises(TypeError, match="source string"):
        ChecklistBuilder().add(check, inputs={"x": ChannelBinding("scope", "x")})


def test_source_assignments_retain_their_input_context() -> None:
    """Inputs referenced before the final expression remain available when reopening saved source."""
    check = CheckDefinition("Pressure", "unused = temperature\n(pressure > 1).ranges()", priority=Priority.P1)
    definition = ChecklistBuilder(strict=True).add(
        check,
        inputs={"temperature": ChannelBinding("scope", "temperature"), "pressure": ChannelBinding("scope", "pressure")},
    )
    context = definition._variables[-1].value.compute_node.context.variables
    assert set(context) == {"temperature", "pressure"}


def test_short_variable_names_keep_each_checks_bindings_separate() -> None:
    """Alphabetic names span checks and wrap after z without changing source aliases or display names."""
    builder = ChecklistBuilder()
    source = "(temperature > 100).ranges()"
    for i in range(14):
        builder.add(
            CheckDefinition(f"Check {i}", source, priority=Priority.P1, key=f"thermal.{i}"),
            inputs={"temperature": ChannelBinding("telemetry", f"temperature_{i}")},
        )
    assert [v.name for v in builder._variables] == list("abcdefghijklmnopqrstuvwxyz") + ["aa", "ab"]
    for i, check in enumerate(builder._checks):
        channel, expression = builder._variables[2 * i : 2 * i + 2]
        assert channel.display_name == f"Check {i}: temperature"
        assert channel.value.compute_node.context.variables["raw"].series.channel == f"temperature_{i}"
        assert expression.display_name == f"Check {i}"
        assert expression.compute_expression.v1.python.expression == source
        assert expression.value.compute_node.context.variables["temperature"].checklist_variable == channel.name
        assert check.condition.num_ranges_v3.variables["failure"].checklist_variable == expression.name
