---
myst:
  html_meta:
    description: "Create and revise checklists from Python, write compute-as-code checks, and evaluate test runs."
---

# Authoring checklists as code

{.lead}
Create and update checklists from Python using compute-as-code expressions.

Use {py:class}`~nominal.experimental.compute_as_code.ChecklistBuilder` to collect check definitions,
bind them to channels, and publish them as a checklist. Keep the definitions in your repository
and use the same builder API to publish later changes.

For background on checklists, violations, and reviewing results, start with
[Data review](data-review.md). This guide covers authoring; the
[test-stand example](../../examples/deploy_checklist.py.md) shows a complete script.

## Setup

Install the `compute` extra and configure a client profile using the
[authentication guide](../authentication.md):

```shell
pip install 'nominal[compute]'
```

Checklist authoring is experimental; these APIs may change between releases.

## Using the checklist builder

The workflow is to add definitions and channel bindings to a builder, then create a checklist
or commit the complete definition to an existing one. Here is a checklist with one temperature check:

```python
from nominal.core import NominalClient, Priority
from nominal.experimental.compute_as_code import ChannelBinding, CheckDefinition, ChecklistBuilder

client = NominalClient.from_profile("my-profile")
builder = ChecklistBuilder(strict=True)
builder.add(
    CheckDefinition(
        title="Engine overheating",
        expression="(temperature > 100).ranges()",
        priority=Priority.P1,
        key="engine.overheating",
    ),
    inputs={"temperature": ChannelBinding("telemetry", "engine.temperature")},
)
checklist = builder.create(client, "Engine acceptance")
print(checklist.rid)
print(checklist.nominal_url)
```

`add()` compiles the expression and collects its bindings locally. It does not look up channels or
write to Nominal. `create()` publishes the collected checks and returns a
{py:class}`~nominal.core.Checklist`. Save its RID for subsequent updates. Each call creates a new
checklist, even with the same name; an empty builder is rejected.

### Updating the same checklist

On the next deployment, rebuild the complete definition with your changes. Then load the saved
checklist and call `commit()` instead of `create()`:

```python
# Populate builder with all the checks you want in the new version.
checklist = client.get_checklist("your-saved-checklist-rid")
builder.commit(checklist, commit_message="Update engine acceptance checks")
```

`commit()` replaces all checks and variables, including any added in the UI. Include every check
you want to keep. Previous versions remain available; checklist metadata and streaming settings
are unchanged.

If someone publishes a change after `get_checklist()`, the commit fails. Review their changes before
loading the checklist again and retrying. A successful commit updates `checklist.commit_id` on the
same object. Every successful call creates a version, even when the contents are unchanged.

### Running the checklist

Publishing a definition does not execute it. To evaluate a historical run, use the returned checklist:

```python
run = client.get_run("run-rid")
review = checklist.execute(run, commit=checklist.commit_id).poll_for_completion()
print(review.nominal_url)
for event in review.get_events():
    print(event.name, event.start, event.duration)
```

Passing `commit_id` pins the evaluation to that version. Open the review to inspect the events
alongside your data. An empty event list can also mean data was missing, so check channel resolution,
evaluation status, and data coverage. [Data review](data-review.md) covers reviewing results and
applying multiple checklists.

For live assets, see [Trigger alerts on live data](../streaming/streaming-checklists.md). Streaming
follows the main checklist version asynchronously, including later commits. Publication alone does
not confirm that assets have adopted the version or that notifications were delivered.

## Check definitions

A {py:class}`~nominal.experimental.compute_as_code.CheckDefinition` contains the failure condition
and the information used to identify and describe a check. Channel bindings are supplied separately
when you add it to a builder, so the definition can be reused with different inputs.

```python
from nominal.core import EventType

overheating = CheckDefinition(
    title="Sustained overheating",
    expression="(temperature > 100).ranges(min_duration=Duration.Seconds(2))",
    priority=Priority.P1,
    key="engine.overheating",
    description="Engine temperature exceeds 100 °C for at least two seconds.",
    event_type=EventType.FLAG,
    event_labels=["thermal", "acceptance"],
)
```

### Configuration

Fields marked **\*** are required.

| Field | Meaning | When omitted |
| --- | --- | --- |
| `title` **\*** | The check's display name. | Required. |
| `expression` **\*** | Compute source producing the time ranges where the check is violated. | Required. |
| `priority` **\*** | Severity, from `Priority.P0` (most severe) to `Priority.P4` (least severe). Does not change the failure condition. | Required. |
| `description` | Additional explanation of the requirement. | Empty string. |
| `key` | Stable identity across versions, independent of title and expression. | A new identity each time the definition is added. |
| `event_type` | Category for generated events: `EventType.INFO`, `FLAG`, `ERROR`, or `SUCCESS`. | `EventType.ERROR`. |
| `event_labels` | Labels attached to events generated by this check. | No labels supplied by the check. |

### Expressions

The expression is a string in the frontend compute language, saved with the checklist for the code
editor. It can contain assignments, but the last expression must produce ranges. It does not include
imports; the compiler supplies names such as `Duration`.

Write the condition that should fail. `(temperature > 100).ranges()` selects intervals above 100.
Adding `min_duration=Duration.Seconds(2)` keeps intervals lasting at least two seconds.

### Stable keys

`key` identifies a check across versions, independently of its title or expression. Keep the same
key when changing a limit or renaming a check. For example, raising the temperature ceiling should
keep `key="engine.overheating"`.

Keys are optional, case-sensitive, and must be nonblank and unique within a builder. Without a key,
each addition gets a new identity. Use distinct keys for separate checks, including copies of a
reusable definition added to the same builder.

### Event metadata

`event_type` sets the category of generated events, and `event_labels` supplies their labels.
These are separate from priority and do not affect the failure condition. The labels apply to
events, not checklist grouping.

An empty `event_labels` list also supplies no labels. The builder copies labels when you call
`add()`. `EventType.UNKNOWN` is not allowed.
Choosing `EventType.SUCCESS` does not turn a violation check into a check that an expected event
occurred, or make missing data a pass. Streaming event creation also requires
`auto_create_events=True`.

### Channel bindings

Bind the names used in the expression when adding the definition:

```python
builder = ChecklistBuilder(strict=True)
builder.add(
    overheating,
    inputs={"temperature": ChannelBinding("telemetry", "engine.temperature")},
)
```

Here, `temperature` is the expression's input name. Its
{py:class}`~nominal.experimental.compute_as_code.ChannelBinding` selects `engine.temperature` from
`telemetry`, the data source reference on a run or data scope on an asset. Runs with the same scope
and channel names can use the same checklist. Values are compared in the channel's units; the
builder does not convert them.

Numeric bindings are the default. Set `series_type="categorical"` for string-valued states, and use
`tags={"side": "left"}` when tags are needed to select a series. Channels are resolved when the
checklist executes, not when the definition is compiled.

Missing inputs fail during `add()`. With `strict=True`, unused bindings also fail. A failed addition
leaves the builder unchanged. The builder allocates published variable names (`a`, `b`, …); your
source keeps its descriptive input names. Those variable names are separate from the check's key.

## Writing checks

The code blocks below are compute source for `CheckDefinition.expression`. Bind their input names
when calling `builder.add()`, as above. Use your own channel names, units, and limits.

### Thresholds and duration

Catch sustained overheating while allowing brief temperature spikes.
Add `min_duration` so the temperature must stay above the limit before the interval counts:

```python
(temperature > 90).ranges(min_duration=Duration.Seconds(2))
```

Bind `temperature` to a numeric temperature channel in °C.

Temperature must stay above 90 °C for at least two seconds. Separate excursions do not add up
toward that duration. Use `>=` if exactly 90 °C should also count.

### Numeric bands

Both a loss of supply pressure and excessive pressure need attention. Treat 2–5 bar as the
allowed operating band and flag either side:

```python
((pressure < 2) | (pressure > 5)).ranges()
```

Bind `pressure` to a numeric pressure channel in bar.

Use `|` for either condition and `&` for both, with parentheses around each comparison. Do not use
Python's scalar `or` and `and`. Here, exactly 2 or 5 bar remains inside the allowed band.

### Conditions that depend on another signal

Low oil pressure matters while the engine is running, but is expected during shutdown. Use shaft speed to apply the oil-pressure limit only above 1,000 rpm:

```python
running = speed > 1000
low_pressure = pressure < 2
(running & low_pressure).ranges(min_duration=Duration.Seconds(1))
```

Bind `speed` to shaft speed in rpm and `pressure` to oil pressure in bar.

Both conditions must hold for at least one second. Low pressure at rest is excluded. This check
does not require the engine to reach operating speed.

You can use assignments to name intermediate expressions, as above. The last expression must
produce ranges.

### Sensor disagreement

Redundant sensors should agree, even when both readings are within their individual limits. Flag a difference greater than 10 °C in either direction for two seconds:

```python
difference = left - right
((difference > 10) | (difference < -10)).ranges(min_duration=Duration.Seconds(2))
```

Bind `left` and `right` to the two sensors. For a channel distinguished by tags, the bindings are:

```python
inputs = {
    "left": ChannelBinding("telemetry", "engine.temperature", tags={"side": "left"}),
    "right": ChannelBinding("telemetry", "engine.temperature", tags={"side": "right"}),
}
```

Both inputs must use the same units. This expression does not specify how to align samples or fill
gaps, so verify the resulting intervals with your data, especially at different sample rates.
It does not detect a missing sensor.

### Categorical states

A controller-reported fault needs review even if the numeric channels look normal.
Select the intervals where its state is `FAULT`:

```python
mode.category_ranges().eq("FAULT")
```

Bind `mode` with `series_type="categorical"`. Use the exact value emitted by the controller.
`category_ranges().eq()` already returns ranges.
This checks for `FAULT`; it does not check that the controller entered `RUN`.

### Reusing checks

Each battery module needs an overheating check, but the modules have different temperature limits.
Use a Python loop to create the checks from that configuration:

```python
for module, limit in {"module_1": 55, "module_2": 60}.items():
    builder.add(
        CheckDefinition(
            title=f"{module} overheating",
            expression=f"(temperature > {limit}).ranges(min_duration=Duration.Seconds(2))",
            priority=Priority.P1,
            key=f"battery.{module}.overheating",
        ),
        inputs={"temperature": ChannelBinding("telemetry", f"battery.{module}.temperature")},
    )
```

The loop adds one check per module when the script runs. Keep the module name in the key and the
limit in the expression, so changing a limit preserves the check’s identity.

You can also reuse a `CheckDefinition` with different channel bindings. Give each copy a distinct
key if you add them to the same builder.

## Keeping checklists in source control

For repeatable deployments, save:

1. The complete set of definitions and channel bindings in source control.
2. The checklist RID in configuration, so subsequent deployments update the same resource.
3. A deployment record containing the source revision and published checklist `commit_id`.

If you also edit the checklist in the UI, copy those changes back to source before deploying.
`commit()` catches concurrent changes made after lookup, but it will overwrite UI edits that were
already present when you loaded the checklist. There is no automatic merge or drift detection.

`add()` catches syntax, type, and binding errors locally. Check the published version against runs
with known violations and acceptable intervals to verify the results. Live channel resolution and
streaming compatibility are checked during execution.

See the [test-stand example](../../examples/deploy_checklist.py.md) for a complete script, or the
[API reference](../../sdk/experimental/compute-as-code.md) for method signatures.
