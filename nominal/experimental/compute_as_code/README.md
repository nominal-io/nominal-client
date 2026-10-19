# Compute-as-code

This module executes expressions authored with the separate `nominal-compute` library, installed with the `compute` extra:

```sh
pip install 'nominal[compute]'
```

The compute extra now uses `nominal-compute` 0.1480.0. Update older scripts from
`import nominal_compute as nc` to `import nominal.compute as nc`. The client helpers
accept expressions from the installed version; expression objects from the older
package are not interchangeable with the new types.

## Authoring checklists

Use `ChecklistBuilder` to collect failure-range expressions and create a published checklist:

```py
import nominal.compute as nc

from nominal.core import NominalClient, Priority
from nominal.experimental.compute_as_code import ChannelBinding, ChecklistBuilder

client = NominalClient.from_profile("...")
temperature = nc.NumericSeries.Reference("temperature")
checklist = (
    ChecklistBuilder()
    .add_check(
        "Sustained overheating",
        (temperature > 100).ranges(min_duration=nc.Duration.Seconds(2)),
        inputs={"temperature": ChannelBinding("telemetry", "engine.temperature")},
        priority=Priority.P1,
    )
    .create(client, "Engine acceptance")
)
```

Bindings resolve against the run or asset where the checklist executes. Adding checks is local;
`create` creates and publishes a new checklist on every call without executing it. Use
`checklist.execute(run).poll_for_completion()` to execute and wait for evaluation.

Reuse expressions in ordinary Python loops, calling `add_check` for each channel or tag
selection. Each call creates a separate check. Missing bindings always fail locally; unused
bindings are omitted unless `ChecklistBuilder(strict=True)` rejects them. This API stores expression graphs,
not Python source. It can commit complete replacement definitions to published checklists;
it does not create drafts. See the
[checklist authoring guide](../../../docs/src/sdk/experimental/compute-as-code.md)
for binding, execution, and evaluation semantics.

For versioned authoring, supply `check_id=UUID("a71bf343-f827-4180-9714-199e149e4c18")` on every `add_check`, retaining the
same UUID for each logical check across revisions. Then rebuild the desired checks and call
`builder.commit(checklist, commit_message="Update limits")`. The loaded checklist's commit ID
protects against stale writes; the checklist is refreshed in place with the new `commit_id`. The full
definition is replaced, including UI-authored checks and checklist variables. Streaming main
may adopt the update asynchronously. Use `checklist.execute(run, commit=checklist.commit_id)`
or `DataReviewBuilder.execute_checklist(..., commit=checklist.commit_id)` to pin a review.
See the authoring guide above for the complete create, update, and execution walkthrough.

## Derived datasets

A derived dataset is a regular dataset whose contents are computed from a `nominal.compute` graph instead of
ingested files. A derived dataset that is a union of input datasets — each optionally filtered by tags,
labelled with a tag, and shifted in time — needs none of this: use `nominal.experimental.derived_datasets`,
whose `create_derived_dataset` and `DerivedDataset.*_input_dataset*` methods work without the `compute` extra
and can edit the definition afterwards.

Reach for this module when the definition is a compute graph that shape cannot express. Note that
`nominal.compute` has no tag filter of its own, so a graph built here cannot filter its inputs by tag.
Create the dataset with `create_derived_dataset`, and manage its definition over time with
`get_derived_definition` and `commit_derived_definition`:

```py
import nominal.compute as nc

from nominal.core import NominalClient
from nominal.experimental.compute_as_code import (
    commit_derived_definition,
    create_derived_dataset,
    get_derived_definition,
)

client = NominalClient.from_profile("...")
dataset = client.get_dataset("...")

# Define the derived dataset as an existing dataset shifted forward by 5 seconds.
spec = nc.Dataset.Saved(dataset.rid).time_shift(nc.Duration.Seconds(5))
derived = create_derived_dataset(client, "my derived dataset", spec)

# Later: replace the definition with a new commit.
definition = get_derived_definition(client, derived)
new_spec = nc.Dataset.Saved(dataset.rid).time_shift(nc.Duration.Seconds(10))
commit_derived_definition(client, derived, new_spec, message="shift by 10s", latest_commit=definition.commit.id)
```

## Computing a series

`compute_series` applies a `nominal.compute` expression to concrete channels and returns the computed values as a
`pandas.Series` indexed by timestamp. References in the expression are bound to channels at execution time via
`inputs`. Example:

```py
import nominal.compute as nc

from nominal.core import NominalClient
from nominal.experimental.compute_as_code import compute_series

client = NominalClient.from_profile("...")
dataset = client.get_dataset("...")

# Author an expression over named references, then bind each reference to a concrete channel.
velocity_error = nc.NumericSeries.Reference("measured") - nc.NumericSeries.Reference("commanded")
series = compute_series(
    client,
    velocity_error,
    inputs={
        "measured": dataset.get_channel("engine.velocity_measured"),
        "commanded": dataset.get_channel("engine.velocity_commanded"),
    },
    name="velocity_error",
)
```

When a channel's name and data source map to more than one series (e.g. the same channel logged per-vehicle), pass
`tags_by_reference` (keyed by the same reference names as `inputs`) to select the series you want.

```py
series = compute_series(
    client,
    velocity_error,
    inputs={
        "measured": dataset.get_channel("engine.velocity_measured"),
        "commanded": dataset.get_channel("engine.velocity_commanded"),
    },
    tags_by_reference={"measured": {"vehicle": "car_1"}, "commanded": {"vehicle": "car_1"}},
    name="velocity_error",
)
```
