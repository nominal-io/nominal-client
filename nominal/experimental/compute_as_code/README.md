# Compute-as-code

This module executes expressions authored with the separate `nominal-compute` library, installed with the `compute` extra:

```sh
pip install 'nominal[compute]'
```

## Derived datasets

A derived dataset is a regular dataset whose contents are computed from a `nominal_compute` graph instead of
ingested files. Create one with `create_derived_dataset`, and manage its definition over time with
`get_derived_definition` and `commit_derived_definition`:

```py
import nominal_compute as nc

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

`compute_series` applies a `nominal_compute` expression to concrete channels and returns the computed values as a
`pandas.Series` indexed by timestamp. References in the expression are bound to channels at execution time via
`inputs`—nothing is persisted back to Nominal:

```py
import nominal_compute as nc

from nominal.core import NominalClient
from nominal.experimental.compute_as_code import compute_series

client = NominalClient.from_profile("...")
dataset = client.get_dataset("...")

# Author an expression over named references, then bind each reference to a concrete channel.
expr = nc.NumericSeries.Reference("a") - nc.NumericSeries.Reference("b")
series = compute_series(client, expr, inputs={"a": dataset.get_channel("a"), "b": dataset.get_channel("b")})
```

When a channel's name and data source map to more than one series (e.g. the same channel logged per-vehicle), pass
`tags`—keyed by the same reference names as `inputs`—to select the series you want.

```py
series = compute_series(
    client,
    expr,
    inputs={"a": dataset.get_channel("a"), "b": dataset.get_channel("b")},
    tags={"a": {"vehicle": "1"}, "b": {"vehicle": "1"}},
)
```
