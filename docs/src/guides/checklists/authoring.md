---
myst:
  html_meta:
    description: "Define checklists in Python with compute-as-code expressions, publish them and update them."
---

# Authoring checklists as code

{.lead}
A checklist is a set of checks, each a condition on {term}`channel` data that should never be true, such as a temperature over its limit.
{py:class}`~nominal.experimental.compute_as_code.ChecklistBuilder` lets you keep checklists in your repository and publish them from Python.

The examples also need the [`compute` extra](/guides/install.md#extras).
Checklist authoring is experimental, so the API may change between releases.
{download}`deploy_checklist.py </examples/deploy_checklist.py>` is a complete test-stand script.

```{include} /guides/_snippets/connect-the-client.md
```

## Publish a checklist

Add each check and its channel bindings to a builder, then create the checklist:

```python
from nominal.core import Priority
from nominal.experimental.compute_as_code import ChannelBinding, CheckDefinition, ChecklistBuilder

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
```

`add` compiles the expression and checks its inputs locally, without calling Nominal.
`create` publishes a new {py:class}`~nominal.core.Checklist` every time, even with the same name.
Save its {term}`RID` to update it later.

## Update a checklist

Build the complete set of checks again, then commit it to the saved checklist:

```python
checklist = client.get_checklist("<CHECKLIST_RID>")
builder.commit(checklist, commit_message="Raise overheating limit")
```

`commit` replaces every check and variable, including changes made in the UI, and creates a new version.
It fails if someone published a change after `get_checklist`. Review that change, then load the checklist again.

Keep the checks in source control and the checklist RID in configuration.
Copy any UI edits back to source before you deploy, because nothing merges them.

## Run a checklist

Publishing doesn't run the checklist. To check a {term}`run`, execute it and read the resulting {term}`events <event>`:

```python
review = checklist.execute(client.get_run("<RUN_RID>"), commit=checklist.commit_id).poll_for_completion()
for event in review.get_events():
    print(event.name, event.start, event.duration)
```

No events can also mean missing data, so check the review's status and the channels' coverage.
For several runs at once, see [Data review](/guides/checklists/data-review.md).
For live data, see [Trigger alerts](/guides/streaming/streaming-checklists.md).

## Check definitions

{py:class}`~nominal.experimental.compute_as_code.CheckDefinition` fields:

| Field | Meaning | Default |
| --- | --- | --- |
| `title` | Display name. | Required |
| `expression` | Compute expression returning the time ranges where the check fails. | Required |
| `priority` | Severity, from `Priority.P0`, the most severe, to `Priority.P4`. | Required |
| `description` | Longer explanation. | Empty |
| `key` | Identity that stays the same across versions. | New identity on each `add` |
| `event_type` | {py:class}`~nominal.core.EventType` of the events the check creates. `UNKNOWN` isn't allowed. | `ERROR` |
| `event_labels` | {term}`Labels <label>` on the events the check creates. | None |

Give each check a `key` and keep it when you change the limit or rename the check, so Nominal treats it as the same check.
Keys are case-sensitive and must be unique within a builder.

## Bind channels

`inputs` maps each name in the expression to a {py:class}`~nominal.experimental.compute_as_code.ChannelBinding`.
Its first argument is the {term}`ref name` on a run or the {term}`data scope` name on an {term}`asset`. Its second is the channel name.
Any run or asset with the same names can use the checklist.

```python
inputs = {
    "temperature": ChannelBinding("telemetry", "engine.temperature"),
    "mode": ChannelBinding("telemetry", "controller.mode", series_type="categorical"),
    "left": ChannelBinding("telemetry", "cell.temperature", tags={"side": "left"}),
}
```

- Use `series_type="categorical"` for string channels.
- Use `tags` to select one series of a channel.
- Values are compared in the channel's {term}`units <unit>`, without conversion.

`add` fails if an input is missing and, with `strict=True`, if one is unused.
Channels are resolved when the checklist runs.

## Expression recipes

An expression can assign names, and the last line must return ranges.
`Duration` is available without importing it.
Bind each input name with `inputs`, as above.

**Sustained limit.** Temperature above 90 for at least 2 seconds. Separate excursions don't add up.

```python
(temperature > 90).ranges(min_duration=Duration.Seconds(2))
```

**Outside a band.** Pressure below 2 or above 5. Use `|` and `&` with parentheses, not `or` and `and`.

```python
((pressure < 2) | (pressure > 5)).ranges()
```

**Conditional limit.** Low oil pressure only while the shaft turns faster than 1,000 rpm.

```python
running = speed > 1000
(running & (pressure < 2)).ranges(min_duration=Duration.Seconds(1))
```

**Sensor disagreement.** Two sensors differing by more than 10 for 2 seconds. Check the result when they sample at different rates.

```python
difference = left - right
((difference > 10) | (difference < -10)).ranges(min_duration=Duration.Seconds(2))
```

**Categorical state.** A controller reporting `FAULT`, with `mode` bound as categorical.

```python
mode.category_ranges().eq("FAULT")
```

**One check per component.** Use a Python loop, with the component in the key and its limit in the expression:

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

Test a new checklist on runs with known failures and known good intervals before you rely on it.
See the [compute-as-code reference](/sdk/experimental/compute-as-code.md) for all methods.
