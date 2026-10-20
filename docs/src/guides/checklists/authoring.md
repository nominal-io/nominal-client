---
myst:
  html_meta:
    description: "Author reusable checks in Python and deploy versioned checklist definitions."
---

# Authoring checklists as code

{.lead}
Author reusable checks in Python and deploy versioned checklist definitions.

Use {py:class}`~nominal.experimental.compute_as_code.ChecklistBuilder` to describe your checks,
publish them, and push later revisions to the same checklist. Every check retains its source for the frontend code editor.

:::{note}
Checklist authoring is experimental and requires the `compute` extra. The API may change between releases.
:::

Configure a client profile using the [authentication guide](../authentication.md), then install the extra:

```shell
pip install 'nominal[compute]'
```

## Describe your checks

This example fails when temperature stays above 100 for at least two seconds:

```python
from nominal.core import NominalClient, Priority
from nominal.experimental.compute_as_code import ChannelBinding, CheckDefinition, ChecklistBuilder

client = NominalClient.from_profile("my-profile")
builder = ChecklistBuilder(strict=True)

overheating = CheckDefinition(
    title="Sustained overheating",
    expression="(temperature > 100).ranges(min_duration=Duration.Seconds(2))",
    priority=Priority.P1,
    key="engine.overheating",
)

builder.add(
    overheating,
    inputs={
        "temperature": ChannelBinding(
            data_scope="telemetry",
            channel="engine.temperature",
        ),
    },
)
```

There are three objects to know:

| Object | Purpose |
| --- | --- |
| {py:class}`~nominal.experimental.compute_as_code.CheckDefinition` | The condition that counts as a failure, its title, and its priority. |
| {py:class}`~nominal.experimental.compute_as_code.ChannelBinding` | The actual channel that supplies an expression's input. |
| {py:class}`~nominal.experimental.compute_as_code.ChecklistBuilder` | Collects the checks, then creates or updates a checklist. |

`temperature` is the name used in the expression. The binding connects it to `engine.temperature`
in the `telemetry` scope. That scope must exist on the run or asset where you execute the checklist.
Values are compared in the channel's units; the builder does not convert them.

The expression uses the frontend compute language, not arbitrary Python. Omit imports; names such
as `Duration` are supplied by the compiler. `.ranges()` turns a condition into its failure intervals.
Multiple assignments followed by a final range expression are also supported.

`add()` checks the source and captures its bindings locally. It does not create or change anything
in Nominal. Missing inputs always fail; `strict=True` also catches extra unused bindings. The error
leaves the builder unchanged. Numeric channels are the default; use `series_type="categorical"`
for categorical channels and `tags={"side": "left"}` to select a tagged series.

The optional `key` is a stable name for the check across revisions. Keep it unchanged when changing
the title or condition to preserve lineage. Keys must be nonblank and unique within the builder.
Without a key, each addition gets a new identity automatically. You never need to handle UUIDs.

Published variables use the frontend's short naming convention: `a`, `b`, … `z`, `aa`, and so on.
Their display names identify the check and its input. Your source still uses the names you chose,
such as `temperature`; the builder connects those local names to the published variables.
Variable names are allocated in addition order and may change when rebuilding or reordering a
checklist. They are separate from the stable check identities maintained by `key`.

## Create the checklist once

After adding all your checks, run:

```python
checklist = builder.create(client, "Engine checks")
print(checklist.rid)
print(checklist.nominal_url)
```

Open the printed URL to inspect the checklist. Save the RID in your deployment configuration.

**Run `create()` only when you want a new checklist.** Every call creates another resource, even
if its name matches an existing checklist. Creation publishes the checks but does not start execution.
An empty builder is rejected before any network request.

## Update that same checklist

For your next deployment, edit the source above—for example, change `100` to `110`—and rerun the
code that constructs the builder and adds **all** the checks you want. Then use this instead of `create()`:

```python
checklist = client.get_checklist("your-saved-checklist-rid")
builder.commit(checklist, commit_message="Raise temperature limit to 110")
print(checklist.nominal_url)
```

**`commit()` replaces the complete definition.** Include all checks you want to keep. Checks and
variables omitted from the builder disappear from the new version, including UI-authored content.
Earlier versions remain available. Checklist metadata and streaming settings stay unchanged.
The builder does not load, merge, or edit the existing definition.

The loaded checklist version protects against overwriting concurrent changes. If someone publishes
a revision after your lookup, the commit fails; review their changes before fetching and trying again.
A successful commit refreshes the same `checklist` object with its new `commit_id`. Each successful
call creates a version, even if its contents are unchanged.

You can keep using a builder after publication. Adding another check does not change a published
checklist until you call `create()` or `commit()` again.

## Run the published checklist

For an existing historical run:

```python
run = client.get_run("run-rid")
review = checklist.execute(run, commit=checklist.commit_id).poll_for_completion()
```

Passing `commit_id` pins the version you just published. See [Data review](data-review.md)
for applying multiple checklists to runs, notification integrations, and reviewing results.

For streaming, start execution and inspect its status:

```python
asset = client.get_asset("asset-rid")
checklist.execute_streaming([asset], notifications=[], auto_create_events=True)
status = checklist.get_streaming_status([asset])[asset.rid]
print(status.commit_id, status.checks)

# Stop when finished:
checklist.stop_streaming_for_assets([asset])
```

This example enables automatic events; the API default remains `auto_create_events=False`.

Streaming follows main asynchronously, including subsequent commits. Compare the executing commit
with `checklist.commit_id` to observe adoption. A successful publication does not mean execution
has succeeded or notifications have been delivered. Stable check identity does not guarantee
uninterrupted alert state. See the [streaming guide](../streaming/streaming-checklists.md)
for configuration and notification details.

## Reuse a check

A check definition has no channel bindings or remote state. You can add the same definition to
another builder with a different channel:

```python
other_builder = ChecklistBuilder(strict=True)
other_builder.add(
    overheating,
    inputs={"temperature": ChannelBinding("telemetry", "backup_engine.temperature")},
)
```

For variations, ordinary Python functions or `dataclasses.replace` can produce a new definition.
For example, when adding two versions to the **same** builder, give them different stable keys:

```python
from dataclasses import replace

backup_overheating = replace(
    overheating,
    title="Backup engine overheating",
    key="backup_engine.overheating",
)
builder.add(
    backup_overheating,
    inputs={"temperature": ChannelBinding("telemetry", "backup_engine.temperature")},
)
```

## Deploy from source control

The [deployment example](../../examples/deploy_checklist.py.md) generates checks with different
thresholds using ordinary Python. It creates a checklist on the first invocation and takes the
saved RID for subsequent deployments. Each check has a stable key; changing its title, threshold,
or position does not require changing that key.

The script prints a deployment record containing the source revision, checklist RID, checklist
commit, and URL. Save that record as a CI artifact. Use the recorded checklist commit—not the
current latest commit—to reproduce an earlier historical evaluation:

```python
import json
from pathlib import Path

record = json.loads(Path("deployment.json").read_text())
checklist = client.get_checklist(record["checklist_rid"])
review = checklist.execute(
    run, commit=record["checklist_commit"],
).poll_for_completion()
```

Treat these checklists as **owned by the source repository**. The next deployment replaces the
entire definition. Concurrency protection catches changes made after lookup; it does not detect
UI edits that happened before lookup. There is no automatic import, merge, drift detection, or
unchanged-deployment suppression. Coordinate UI edits back into source before deploying.

Static Python expansion can generate a known collection of channels and thresholds. It does not
provide runtime wildcard discovery, property-driven thresholds, or group-by evaluation.

## Choose generated event metadata

Use the existing SDK event category independently of the check's priority:

```python
from nominal.core import EventType

warning = CheckDefinition(
    "Temperature warning",
    "(temperature > 90).ranges()",
    priority=Priority.P2,
    key="engine.temperature_warning",
    event_type=EventType.FLAG,
    event_labels=["thermal", "acceptance"],
)
```

`event_labels` applies to generated events; it is not a way to group checks in the checklist UI.
Both fields are optional and preserve service defaults when omitted. Explicit empty event labels
request no labels. The builder copies the labels on `add()`.

Event category does not change evaluation logic. In particular, choosing `EventType.SUCCESS`
does not prove that an expected condition occurred, turn missing data into a pass, or automatically
accept a procedure step. Streaming event creation still requires `auto_create_events=True`.

## Core recipes

These expressions illustrate the initial source-authoring core. Bind each channel name as above.

| Purpose | Source |
| --- | --- |
| Upper threshold | `(temperature > 100).ranges()` |
| Sustained violation | `(temperature > 100).ranges(min_duration=Duration.Seconds(2))` |
| Outside a numeric band | `((temperature < 10) | (temperature > 100)).ranges()` |
| Combined condition | `((temperature > 100) & (pressure < 20)).ranges()` |
| Categorical fault | `mode.category_ranges().eq("FAULT")` |

Compilation checks syntax, types, and local bindings. It does not check live channel availability,
frontend edit/save behavior, historical results, or streaming compatibility. Test representative
data and inspect execution status before enabling alerts. Missing data must not be assumed
to mean a pass. The SDK uses the installed compiler rather than a separate operation allowlist;
these examples do not guarantee support for every composition or execution mode.

## Next steps

- [Data review](data-review.md): evaluate published checks against historical runs.
- [Trigger alerts on live data](../streaming/streaming-checklists.md): configure streaming and notifications.
- [Compute-as-code API reference](../../sdk/experimental/compute-as-code.md): classes, methods, and related compute helpers.
