---
myst:
  html_meta:
    description: "Overview and recipes for working with Nominal's Run primitive in Python"
---

# Runs in Nominal with Python

{.lead}
Overview and recipes for working with Nominal's Run primitive in Python

```{include} /guides/_snippets/install-warning.md
```

A Run is Nominal's primitive for test data that shares a common time domain.
This guide details common patterns for working with Runs in Python.

## Connect to Nominal

```{include} /guides/_snippets/python/auth.md
```

## Create a run

It's possible to create an empty run without any data. Runs must have a start and end time expressed in absolute time.
All Datasets added to the run should overlap with this time domain.

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

flight_simulator_run = client.create_run(
    name="High precipitation flight",
    start="2024-06-08T05:58:42Z",
    end="2024-06-08T06:00:06Z",
)

print("Created run:", flight_simulator_run.rid)
```

## Create a run off of an asset

It's also possible to create a run off of an existing asset:

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

asset = client.create_asset("Aircraft", "My flight simulator aircraft")

flight_simulator_run = client.create_run(
    name="High precipitation flight",
    start="2024-09-09T12:35:00Z",
    end="2024-09-09T13:18:00Z",
    assets=[asset.rid],
)

print("Created run:", flight_simulator_run.rid)
```

## Add data to a run

To add a Dataset to a run, use {py:obj}`Run.add_dataset() <nominal.core.Run.add_dataset>`:

```python
import polars as pl

df = pl.read_csv("hf://datasets/nominal-io/frosty-flight/frosty_flight_1k_rows.csv")
df.write_csv("frosty_flight_1k_rows.csv")

csv_dataset = client.create_dataset("Frosty Flight")
csv_dataset.add_tabular_data(
    "frosty_flight_1k_rows.csv",
    timestamp_column="source_time",
    timestamp_type="iso_8601",
)

flight_simulator_run.add_dataset(
    dataset=csv_dataset, ref_name="high-precipitation-flight"
)
```

```{include} /guides/_snippets/what-is-a-dataset.md
```

The example above sets the start and end times of the run manually. You can also set the run to span the Dataset:

```python
for f in csv_dataset.list_files(successful_only=False):
    f.poll_until_ingestion_completed()

# After ingestion, we can query the data bound timestamps
csv_dataset.refresh()
bounds = csv_dataset.bounds

# And update the run accordingly
flight_simulator_run.update(start=bounds.start, end=bounds.end)
```

## Wait for ingestion before creating a run

When a run spans several datasources that ingest at different rates (for example, multiple video streams and a telemetry channel from the same test event), lower-bitrate sources typically finish ingesting first. Selecting a run from the runs page before every attached datasource finishes ingesting can cause inconsistent behavior in the workbook UI: the `align-to-run` action can appear available and display the wrong video if the referenced video dataset hasn't finished ingesting.

Poll every file for ingestion completion before creating the run:

```python
for f in files:
    f.poll_until_ingestion_completed()

run = client.create_run(name="my_run", start=start, end=end)
for dataset in datasets:
    run.add_dataset(dataset=dataset, ref_name=dataset.name)
```

Apply the same pattern before creating a workbook from a template: poll every attached file for ingestion completion, then create the workbook. This keeps runs and workbooks hidden from users until their data finishes ingesting.

## Run data with ref names

To add a Dataset to a run along with a reference name, set the `ref_name` parameter in {py:obj}`Run.add_dataset() <nominal.core.Run.add_dataset>`.

```python
flight_simulator_run.add_dataset(
    dataset=csv_dataset, ref_name="high-precipitation-flight"
)
```

```{include} /guides/_snippets/what-is-a-ref-name.md
```

## Check if a run exists

You can check for a run's existence with {py:obj}`client.search_runs() <nominal.core.NominalClient.search_runs>`.

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

found_runs = client.search_runs(name_substring="High precipitation flight")

if len(found_runs) == 0:
    flight_simulator_run = client.create_run(
        name="High precipitation flight",
        start="2024-09-09T12:35:00Z",
        end="2024-09-09T13:18:00Z",
    )
```

## Update a run

Run metadata can be updated with {py:obj}`Run.update() <nominal.core.Run.update>`:

For example, to set a run's end time to the present moment:

```python
import datetime

flight_simulator_run.update(end=datetime.datetime.now())
```

To update a run's title:

```python
flight_simulator_run.update(name="Low precipitation flight")
```

To add labels to a run:

```python
new_labels = ("high altitude", "simulated")
old_labels = flight_simulator_run.labels
flight_simulator_run.update(labels=old_labels + new_labels)
```

Please see {py:obj}`Run.update() <nominal.core.Run.update>` for all updatable metadata.

## Run attachments

File attachments such as PDF reports or PowerPoints can be added to Runs:

```python
filepath = "/path/to/my_report.pdf"
with open(filepath, "rb") as f:
    attachment = client.create_attachment_from_io(
        f,
        name="KittyHawk.pdf",
    )
my_run.add_attachments(attachments=[attachment])
```

## Retrieve a run

Like Datasets, Runs can be retrieved by their resource ID ("RID"):

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

run = client.get_run(
    "ri.scout.cerulean-staging.run.22697726-5454-4fad-a3ea-fe45e9fa9f09"
)
run.update(labels=["X-PLANE"])
```

To retrieve a Run's RID, visit its detail page and click on the clipboard icon next to "ID" in the right-hand drawer:

![run-metadata](/guides/images/b4/run_metadata_drawer_jq7iww.png)

```{include} /guides/_snippets/what-is-a-rid.md
```

## Query Runs

Runs can be queried with {py:obj}`client.search_runs() <nominal.core.NominalClient.search_runs>`.

For example, to retrieve all runs with the label "X-PLANE":

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

x_plane_runs = client.search_runs(labels=["X-PLANE"])
```

See {py:obj}`client.search_runs() <nominal.core.NominalClient.search_runs>` for all run search parameters.

## Remove Run Data Sources

The list `data_sources` can contain Connection, Dataset, Video instances, or rids as string.

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

run = client.get_run(
    "ri.scout.cerulean-staging.run.6c41d7fd-a48f-4d60-baa7-f34492264156"
)

run.remove_data_sources(
    data_sources=[
        "ri.catalog.cerulean-staging.dataset.d4f413c4-4787-4259-a142-1286587b50af"
    ]
)
```
