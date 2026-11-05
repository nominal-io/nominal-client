# Compute-as-code

This module executes expressions authored with the separate `nominal-compute` library, installed with the `compute` extra:

```sh
pip install 'nominal[compute]'
```

The compute extra now uses `nominal-compute` 0.1495.1. Update older scripts from
`import nominal_compute as nc` to `import nominal.compute as nc`. The client helpers
accept expressions from the installed version; expression objects from the older
package are not interchangeable with the new types.

## Authoring checklists

Use `CheckDefinition` for reusable source and metadata, and `ChecklistBuilder.add(check, inputs=...)`
to bind channels. Every check is compiled with nominal-compute and saved with its source for the
frontend code editor. Use `dataclasses.replace` or Python functions for variations; bindings have
one owner, the builder's `add()` call.

Use `builder.create(client, name)` once and `builder.commit(checklist, commit_message=...)`
for complete revisions to the same checklist. No separate build step or compiled definition is needed. Missing bindings
always fail locally; unused bindings are allowed unless `strict=True`. UUIDs are automatic, with
optional stable keys for continuity. Updates use the loaded commit as a conflict precondition and
replace all checks and variables, including frontend edits. Checks can set `event_type` and
`event_labels` for generated events. Graph-only checklist authoring is not exposed.

See the [Authoring checklists as code](../../../docs/src/guides/checklists/authoring.md)
for reusable checks, core source recipes, and the create/update/evaluate workflow.

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
