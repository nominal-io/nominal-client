---
myst:
  html_meta:
    description: "Create runs on assets or from datasets, then find, update and annotate them."
---

# Runs

{.lead}
A run names a span of time for one test or analysis, such as a flight or a test-stand firing.
It references data rather than copying it.

```{include} /guides/_snippets/connect-the-client.md
```

## Create a run on an asset

When the data already belongs to an [asset](/guides/assets.md), create the run from the asset with the test's time window.
The run sees the asset's datasets through their data scopes, so you don't attach them again.

```python
from datetime import datetime, timezone

asset = client.get_asset("<ASSET_RID>")
run = asset.create_run(
    "Flight acceptance test",
    start=datetime(2026, 9, 10, 14, 38, tzinfo=timezone.utc),
    end=datetime(2026, 9, 10, 14, 48, tzinfo=timezone.utc),
)
```

Start and end accept timezone-aware datetimes or epoch nanoseconds.
Pass `end=None` for a test that's still running.
For a run across several assets, use {py:meth}`~nominal.core.NominalClient.create_run` with `assets=[...]`.

## Create a run from datasets

Without an asset, attach each dataset to the run under a ref name with {py:meth}`~nominal.core.Run.add_dataset`.
This example sets the run's bounds from an uploaded file.
```{include} /guides/_snippets/sample-flight-csv.md
```

```python
dataset = client.create_dataset("Frosty flight")
file = dataset.add_tabular_data("flight.csv", timestamp_column="time", timestamp_type="iso_8601")
file.poll_until_ingestion_completed()
assert file.bounds is not None  # set once ingestion succeeds

run = client.create_run("High precipitation flight", start=file.bounds.start, end=file.bounds.end)
run.add_dataset("telemetry", dataset)
```

The ref name, here `"telemetry"`, is the dataset's name within the run.
Use the same ref name for the same kind of data on every run, so workbook templates and checklists work across runs.
`add_dataset` also takes `series_tags`, to select [tagged data](/guides/streaming/overview.md#add-tags-to-filter-data-in-a-run-or-asset), and a time `offset`.

{py:meth}`~nominal.core.Run.get_dataset` and {py:meth}`~nominal.core.Run.list_datasets` return only datasets attached this way.
For data on an associated asset, use {py:meth}`~nominal.core.Asset.get_dataset`.

{py:meth}`~nominal.core.Run.remove_data_sources` detaches a dataset without deleting it:

```python
run.remove_data_sources(ref_names=["telemetry"])
```

## Find runs

Retrieve one run by the RID on its page in Nominal, or search with {py:meth}`~nominal.core.NominalClient.search_runs`:

```python
run = client.get_run("<RUN_RID>")
review_runs = client.search_runs(labels=["needs-review"], asset=asset)
```

`name_substring` matches names, descriptions, labels and properties, so results may not contain the text in their name.

## Update a run

{py:meth}`~nominal.core.Run.update` replaces each field you pass and leaves the others unchanged.
To add a property, merge it with the existing ones:

```python
run.update(labels=["reviewed"], properties={**run.properties, "reviewer": "flight-test"})
```

To close a run that's still going, set its end:

```python
run.update(end=datetime.now(timezone.utc))
```

Changing bounds changes the window the run shows, not the stored data.

## Attachments, workbooks and events

Attach files about this test to the run, and files about the system to its [asset](/guides/assets.md#update-metadata-and-attachments):

```python
run.add_attachments([client.create_attachment("flight_report.pdf")])
```

Create a workbook for the run from a template whose inputs match the run's ref names or data scope names. See [Workbooks](/guides/workbooks.md).

Mark moments inside the run with [events](/guides/events.md).

## Archive a run

{py:meth}`~nominal.core.Run.archive` hides the run without deleting its data:

```python
run.archive()
```
