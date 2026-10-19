# Compute as code

Author expressions with `nominal.compute` and use the client to evaluate them or create
checklists and derived datasets. Install the optional dependency with:

```sh
pip install 'nominal[compute]'
```

The compute extra uses `nominal-compute` 0.1480.0. Older scripts must replace
`import nominal_compute as nc` with `import nominal.compute as nc`.

## Author a checklist

`ChecklistBuilder` accepts compute **range expressions describing violations**. For example,
this check detects temperatures above 100 for at least two seconds:

```python
from uuid import UUID

import nominal.compute as nc
from nominal.core import NominalClient, Priority
from nominal.experimental.compute_as_code import ChannelBinding, ChecklistBuilder

client = NominalClient.from_profile("my-profile")
temperature = nc.NumericSeries.Reference("temperature")
overheated = (temperature > 100).ranges(min_duration=nc.Duration.Seconds(2))

# Generate this UUID once and keep it in source for every revision of this check.
OVERHEATING_ID = UUID("a71bf343-f827-4180-9714-199e149e4c18")
builder = ChecklistBuilder().add_check(
    "Sustained overheating",
    overheated,
    check_id=OVERHEATING_ID,
    inputs={"temperature": ChannelBinding("telemetry", "engine.temperature")},
    priority=Priority.P1,
    description="Temperature exceeds 100 for at least two seconds.",
)
checklist = builder.create(client, "Engine acceptance", labels=["acceptance"])
```

Adding checks makes no network requests. The builder snapshots each expression and its
bindings when added. `create` creates a **published** checklist in the client's default
workspace, assigned to the authenticated user unless specified otherwise. It does not execute
the checklist. Each call creates a new resource; it does not update a previously created checklist.

`ChannelBinding` uses the data source reference or data scope name and channel name,
so the same checklist can run against different runs or assets. Its optional `tags` mapping
selects a particular tagged series. Adding a check always raises `ValueError` if an expression
reference has no binding. For example, referencing `temperature_deg` while only supplying
`temperature` fails immediately, before the builder is changed or any network request is made.

Unused bindings are allowed by default and omitted from the published check, so multiple checks
can share one input mapping without depending on channels they do not use.
Opt into rejecting them with:

```python
builder = ChecklistBuilder(strict=True)
```

Errors identify the check and the missing or unused names. Validation checks reference names; channel availability and type compatibility are
still resolved by the service at execution time.

Execute through the existing checklist API:

```python
run = client.get_run("run-rid")
review = checklist.execute(run).poll_for_completion()
print(review.nominal_url)
```

Completion means evaluation has finished, not that all checks passed. For multi-asset runs,
use `client.data_review_builder().execute_checklist(run, checklist, asset=asset).initiate()`
to select the asset explicitly.

## Commit a new version

Keep the checklist RID from creation in your deployment configuration. On later deployments,
load that checklist and rebuild the complete desired definition using the same check UUIDs:

```python
checklist = client.get_checklist("checklist-rid")
updated = ChecklistBuilder().add_check(
    "Sustained high temperature",  # Renaming does not change the check's identity.
    (temperature > 110).ranges(min_duration=nc.Duration.Seconds(2)),
    check_id=OVERHEATING_ID,
    inputs={"temperature": ChannelBinding("telemetry", "engine.temperature")},
    priority=Priority.P1,
).commit(checklist, commit_message="Raise the temperature limit to 110")

# Pin evaluation to the version just committed, even if somebody edits main later.
review = updated.execute(run, commit=updated.commit_id).poll_for_completion()
```

`check_id` is a persistent lineage UUID, not a server-generated check version RID. Generate it
once, save it in source, and retain it across renames, reordering, and expression changes.
Use a new UUID for a new logical check. Duplicate UUIDs in one builder are rejected.
Creation may omit IDs for compatibility, but `commit` requires one on every check. To adopt an
existing checklist, reuse its existing lineage UUIDs; inventing new IDs starts new lineages.
This builder does not discover or match existing checks by display name.

A commit **replaces the entire check definition and checklist variables**, including content
created in the UI. Omitted checks are removed from the new version; historical versions remain.
An empty builder is rejected. Checklist metadata and streaming settings are not part of this
replacement. Every successful call creates a commit, even with unchanged expressions.

The service checks the loaded `checklist.commit_id` before committing. Concurrent edits cause
a conflict; the builder does not fetch a newer version and silently overwrite it. Reconcile
those edits before loading the latest checklist and retrying. The same `checklist` object is refreshed from the response
and returned with its new commit ID. Save the old ID before committing if you need it later. A manually constructed checklist without
a commit ID must be loaded with `refresh()` first.

## Use the same checklist for reviews and streaming

The authoring builder produces the regular `Checklist` resource. For batched reviews,
notifications, or a multi-asset run, use the existing data review builder with an explicit version:

```python
reviews = (
    client.data_review_builder()
    .execute_checklist(run, updated, commit=updated.commit_id, asset=asset)
    .add_integration(integration)
    .initiate()
)
```

Both review entry points use the latest version if `commit` is omitted, even when passed a
previously loaded checklist object. Waiting for review completion does not establish that
all checks passed or that notifications were delivered.

Streaming execution remains on `Checklist`:

```python
updated.execute_streaming([asset], integration_rids=[integration.rid])
```

This configures execution for the selected assets; it is not needed after each definition
commit. Streaming evaluates main rather than a caller-pinned commit. Main-branch commits can
update running evaluations asynchronously, just as publishing from the UI does. A successful
commit does not confirm adoption by every asset or successful channel resolution. The builder
does not issue an extra reload or replace notification settings. See the
[streaming checklist guide](../../guides/streaming/streaming-checklists.md) for configuration.

## Reuse a condition across channels

For several sensors with the same rule, reuse the expression in an ordinary Python loop:

```python
builder = ChecklistBuilder()
for sensor in ("left", "right"):
    builder.add_check(
        f"{sensor} sustained overheating",
        overheated,
        inputs={"temperature": ChannelBinding("telemetry", "engine.temperature", tags={"sensor": sensor})},
        priority=Priority.P1,
    )
checklist = builder.create(client, "Engine sensors")
```

Each call adds a separately named check. Use Python functions and loops to share logic;
checks can have different descriptions, priorities, or expressions. Grouping multiple
implementations under one check identity is outside this builder's scope.

## Evaluation and authoring scope

Use compute operations to express persistence, intersections, unions, and other range logic.
The builder requires a `RangeSeries`; convert a boolean expression with `.ranges()`.
Alignment, fill, and missing-data handling use the service's checklist defaults. In particular,
an empty range result should not be used as a local assertion that input data was present.

This API stores the serialized compute graph, not Python source. Keep authoring code in your
repository. Source-string compilation, Python-source round trips in the UI, draft editing,
and branch-based editing are not supported by this builder.

For series evaluation and derived datasets, see the
[package walkthrough](https://github.com/nominal-io/nominal-client/blob/main/nominal/experimental/compute_as_code/README.md).

## API reference

```{eval-rst}
.. automodule:: nominal.experimental.compute_as_code
```
