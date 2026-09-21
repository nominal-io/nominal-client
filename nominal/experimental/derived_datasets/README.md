# Derived Datasets



A derived dataset is a dataset whose contents are the union of other datasets, each optionally filtered by tag, labelled with a tag, and shifted in time.

## Creating a derived dataset

Build one `DerivedDatasetInput` per source dataset and pass them to `create_derived_dataset`.

```py
from nominal.core import NominalClient
from nominal.experimental.derived_datasets import DerivedDatasetInput, create_derived_dataset

client = NominalClient.from_profile("...")
flight = client.get_dataset("ri.catalog....flight")
ground = client.get_dataset("ri.catalog....ground")

derived = create_derived_dataset(
    client,
    "flight + ground",
    inputs=[
        DerivedDatasetInput.create(flight),
        DerivedDatasetInput.create(ground),
    ],
)
```

## Filtering inputs by tag

`TagFilter.in_` and `TagFilter.not_in` restrict which series from an input dataset are included.

```py
from nominal.experimental.derived_datasets import TagFilter

DerivedDatasetInput.create(
    flight,
    filters=[
        TagFilter.in_("vehicle", ["v1", "v2"]),
        TagFilter.not_in("sensor", "debug"),
    ],
)
```

## Labelling and shifting inputs

Each input can carry a tag of its own and a time offset applied to its data.

```py
from datetime import timedelta

DerivedDatasetInput.create(ground, add_tag=("source", "ground"), offset=timedelta(seconds=5))
```

## Editing the inputs

`add_input_dataset` and `remove_input_dataset` make one change per commit; `set_input_datasets` replaces the whole sequence in a single commit.

```py
derived.add_input_dataset(DerivedDatasetInput.create("ri.catalog....telemetry"))
derived.remove_input_dataset(ground)

inputs = derived.list_input_datasets()
derived.set_input_datasets(
    [*inputs[1:], DerivedDatasetInput.create(ground, offset=timedelta(seconds=10))],
    message="align ground against flight",
)
```

## Reading the definition

`get_derived_dataset` looks one up, and `get_definition` / `commit_definition` work with the raw definition directly.

```py
from nominal.experimental.derived_datasets import get_derived_dataset

derived = get_derived_dataset(client, "ri.catalog....derived")
definition = derived.get_definition()
derived.commit_definition(definition.spec, "no-op recommit", latest_commit=definition.commit.id)
```
