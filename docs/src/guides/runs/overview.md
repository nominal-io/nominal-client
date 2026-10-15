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

It's possible to create an empty run without any data. Supply an absolute start time and either an absolute end time or `end=None` for an ongoing run.
Use timezone-aware datetimes or integer epoch nanoseconds.
All Datasets added to the run should overlap with this time domain.

```python
from datetime import datetime, timezone

from nominal.core import NominalClient

client = NominalClient.from_profile("default")

flight_simulator_run = client.create_run(
    name="High precipitation flight",
    start=datetime(2024, 6, 8, 5, 58, 42, tzinfo=timezone.utc),
    end=datetime(2024, 6, 8, 6, 0, 6, tzinfo=timezone.utc),
)

print("Created run:", flight_simulator_run.rid)
```

## Create a run off of an asset

Use {py:meth}`~nominal.core.Asset.create_run` to associate a run with an asset.
The run can use the asset's data scopes without attaching each dataset directly:

```python
from datetime import datetime, timezone

from nominal.core import NominalClient

client = NominalClient.from_profile("default")

asset = client.create_asset("Aircraft", "My flight simulator aircraft")

flight_simulator_run = asset.create_run(
    name="High precipitation flight",
    start=datetime(2024, 9, 9, 12, 35, tzinfo=timezone.utc),
    end=datetime(2024, 9, 9, 13, 18, tzinfo=timezone.utc),
)

print("Created run:", flight_simulator_run.rid)
```

## Add data to a run

To attach a dataset directly to the run, use {py:obj}`Run.add_dataset() <nominal.core.Run.add_dataset>`.
Install `huggingface_hub` to download the sample CSV. Wait for ingestion before attaching it:

```python
from huggingface_hub import hf_hub_download

csv_path = hf_hub_download(
    repo_id="nominal-io/frosty-flight",
    filename="frosty_flight_1k_rows.csv",
    repo_type="dataset",
)

csv_dataset = client.create_dataset("Frosty Flight")
dataset_file = csv_dataset.add_tabular_data(
    csv_path,
    timestamp_column="source_time",
    timestamp_type="iso_8601",
)
dataset_file.poll_until_ingestion_completed()

flight_simulator_run.add_dataset(
    dataset=csv_dataset, ref_name="high-precipitation-flight"
)
```

```{include} /guides/_snippets/what-is-a-dataset.md
```

The earlier examples set the run's times manually. To make it span this uploaded file,
use the file's bounds after polling. These are integer epoch nanoseconds; dataset-wide
bounds may also include other uploads.

```python
bounds = dataset_file.bounds
if bounds is None:
    raise ValueError("Ingested file has no timestamp bounds")

flight_simulator_run.update(start=bounds.start, end=bounds.end)
```

## Wait for ingestion before creating a run

When a run spans several datasources that ingest at different rates (for example, multiple video streams and a telemetry channel from the same test event), lower-bitrate sources typically finish ingesting first. Selecting a run from the runs page before every attached datasource finishes ingesting can cause inconsistent behavior in the workbook UI: the `align-to-run` action can appear available and display the wrong video if the referenced video dataset hasn't finished ingesting.

Retain the file handles returned by your uploads in `files`, and their datasets in
`datasets`. Supply `start` and `end` in the timestamp types described above. Poll every
file for ingestion completion before creating the run; a failed ingest stops this sequence:

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
from datetime import datetime, timezone

from nominal.core import NominalClient

client = NominalClient.from_profile("default")

found_runs = client.search_runs(name_substring="High precipitation flight")

if len(found_runs) == 0:
    flight_simulator_run = client.create_run(
        name="High precipitation flight",
        start=datetime(2024, 9, 9, 12, 35, tzinfo=timezone.utc),
        end=datetime(2024, 9, 9, 13, 18, tzinfo=timezone.utc),
    )
```

## Update a run

Run metadata can be updated with {py:obj}`Run.update() <nominal.core.Run.update>`:

For example, to set a run's end time to the present moment:

```python
flight_simulator_run.update(end=datetime.now(timezone.utc))
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
attachment = client.create_attachment(filepath)
flight_simulator_run.add_attachments(attachments=[attachment])
```

## Retrieve a run

Like Datasets, Runs can be retrieved by their resource ID ("RID"). Replace
`<RUN_RID>` with a run RID from your workspace:

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

run = client.get_run(
    "<RUN_RID>"
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

The list `data_sources` can contain Connection, Dataset, or Video instances, or their RIDs.
Replace the placeholders below with resources from your workspace. This removes direct
references from the run; it does not delete the source or remove an asset-owned data scope.

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

run = client.get_run(
    "<RUN_RID>"
)

run.remove_data_sources(
    data_sources=[
        "<DATASET_RID>"
    ]
)
```
