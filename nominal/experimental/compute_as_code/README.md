# Compute-as-code

In addition to the Python-internal DSL in `nominal.experimental.compute`, this module executes expressions authored
with the separate `nominal-compute` library, installed with the `compute` extra:

```sh
pip install 'nominal[compute]'
```

## Derived datasets

A derived dataset is a regular catalog dataset whose contents are computed from a `nominal_compute` graph instead of
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

# Later: replace the definition with a new commit, passing the current commit for optimistic concurrency.
definition = get_derived_definition(client, derived)
new_spec = nc.Dataset.Saved(dataset.rid).time_shift(nc.Duration.Seconds(10))
commit_derived_definition(client, derived, new_spec, message="shift by 10s", latest_commit=definition.commit.id)
```
