---
myst:
  html_meta:
    description: "Create standalone runs or select time windows on asset data, then manage run metadata and analysis"
---

# Runs in Python

{.lead}
Create standalone runs or select time windows on asset data, then manage run metadata and analysis

A **run** identifies a time interval for a test or analysis. It selects data to view together;
it does not store a separate copy of that data.

There are two common ways to organize a run:

| Workflow | Where the data is referenced | Use it when |
| --- | --- | --- |
| Standalone run | Datasets are attached directly to the run under reference names. | You have recordings for a particular test and want to assemble them into one run. |
| Run on an asset | The run is associated with an asset and uses the asset's data scopes. | Data already belongs to a vehicle, test stand, or other system, and this run selects a test interval. |

These are two workflows using the same `Run` object, not different run types. A run can combine
asset data with directly attached datasets. In either case, the run can have its own name,
properties, labels, attachments, and workbooks. Assets can also have their own metadata,
attachments, and workbooks; choose the resource that the information describes.

The following sections demonstrate the workflows separately. Choose one, then use the shared
operations later in the page.

## Connect to Nominal

Install the SDK and [save an authentication profile](/guides/authentication.md), then create a
client. Both workflows below use these imports and this client:

```python
from datetime import datetime, timezone

from nominal.core import NominalClient

client = NominalClient.from_profile("default")
```

## Standalone run: attach the test's datasets

A standalone run has no associated asset. Upload the test recording to a dataset, wait for
its ingestion, then create the run and attach the dataset directly.

This example uses the sample CSV from the [CSV guide](/guides/datasets/csv.md).
Install `huggingface_hub` with `pip install huggingface_hub` to download it:

```python
from huggingface_hub import hf_hub_download

csv_path = hf_hub_download(
    repo_id="nominal-io/frosty-flight",
    filename="frosty_flight_1k_rows.csv",
    repo_type="dataset",
)
dataset = client.create_dataset("Frosty Flight")
dataset_file = dataset.add_tabular_data(
    csv_path,
    timestamp_column="source_time",
    timestamp_type="iso_8601",
)
dataset_file.poll_until_ingestion_completed()

bounds = dataset_file.bounds
if bounds is None:
    raise ValueError("The uploaded recording has no time bounds")

run = client.create_run(
    name="High precipitation flight",
    start=bounds.start,
    end=bounds.end,
)
run.add_dataset(dataset=dataset, ref_name="telemetry")
print("Created run:", run.nominal_url)
```

The run spans this file's bounds, expressed as integer epoch nanoseconds. Dataset-wide bounds
may include other recordings, so they need not describe this particular test.
`run.add_dataset()` references the dataset; it does not copy or re-upload its data.

For additional test datasets, call `run.add_dataset()` with a distinct reference name for each.
Choose run bounds covering the intended test interval across those recordings. When a dataset
contains multiple recordings in that interval, use `series_tags` to select the relevant tagged
data; a reference can also specify a time `offset`. See
[Filtering data with tags](/guides/streaming/overview.md#add-tags-to-filter-data-in-a-run-or-asset).

### Reference names

`"telemetry"` is the dataset's name **within the run**, not its display name or RID. Use consistent
reference names across similar runs so workbook templates and checklists can resolve the same
logical inputs. For example, `"telemetry"` and `"ground_station"` distinguish two datasets that
both contain a channel named `temperature`.

## Run on an asset: select a test interval

Use this workflow when the recordings or live data already belong to an asset. Creating the run
associates it with that asset; you do not need to attach each of its datasets to the run again.

Replace `<ASSET_RID>` with an existing asset that has data during your chosen interval. For setup,
see [Add data to an asset](/guides/assets/overview.md#add-data-to-an-asset).
This is an alternative to the standalone example:

```python
asset = client.get_asset("<ASSET_RID>")
run = asset.create_run(
    name="Flight acceptance test",
    start=datetime(2026, 9, 10, 14, 38, 19, tzinfo=timezone.utc),
    end=datetime(2026, 9, 10, 14, 48, 19, tzinfo=timezone.utc),
)
print("Created run:", run.nominal_url)
```

Choose the time window for this test, even when the asset's datasets contain many tests or a
continuous stream. The asset's data scopes determine the data available through the association.
For a run involving several assets, use `client.create_run(..., assets=[asset_a, asset_b])` with
the assets you have retrieved.

A run created this way can still attach a test-specific dataset directly with
`run.add_dataset()`. For example, an asset may provide continuous telemetry while a directly
attached dataset provides measurements from temporary test equipment.

### Resolve data from its owner

`run.get_dataset("telemetry")` and `run.list_datasets()` resolve **direct run references**. They
do not search the associated assets. For asset data, use `asset.get_dataset("telemetry")` with
the asset's data scope name. `run.list_assets()` retrieves the run's associated assets.

Resolving a dataset returns the underlying dataset, not a time-clipped copy. When exporting,
apply the run's bounds and any scope filters or offsets explicitly; see
[Export data](/guides/datasets/export-data.md).

## Choose bounds and wait for data

Both creation methods accept timezone-aware datetimes or integer epoch nanoseconds. Use
`end=None` for an ongoing run whose end is not yet known. Creating a run does not start or stop
recording; it only defines the analysis interval and associations.

For a completed test, wait for all required uploads before creating the run or its workbook.
Keep the `DatasetFile` handles returned by the uploads and call
`poll_until_ingestion_completed()` on each, as in the standalone example. For extractor or batch
jobs, follow [Ingest jobs](/guides/ingest/ingestion-jobs.md).

This avoids presenting an analysis before its required data has ingested. It is a workflow
choice, not a requirement to create a run: an ongoing run can exist while data is still arriving.

## Retrieve and find runs

The remaining operations work with either workflow. In a later session, retrieve the run by the
RID shown on its page in Nominal:

```python
run = client.get_run("<RUN_RID>")
print(run.name, run.nominal_url)
```

To discover runs instead, search by name or labels:

```python
matching_runs = client.search_runs(name_substring="Flight")
review_runs = client.search_runs(labels=["Needs review"])

for match in review_runs:
    print(match.name, match.rid)
```

A name search can match several runs; it is not a unique lookup or a guarantee that creating
another run is safe. Use a known RID when you need a particular run. See
{py:meth}`~nominal.core.NominalClient.search_runs` for the available filters.

## Update a run

Use `update()` for the run's own metadata, regardless of where its data is referenced:

```python
run.update(
    name="Flight acceptance test — reviewed",
    description="Post-flight review of the acceptance test",
    labels=["Reviewed"],
    properties={"review_status": "complete"},
)
```

Supplied labels and properties replace those fields; omitted fields remain unchanged. To add
metadata while preserving existing values, refresh and merge them first:

```python
run.refresh()
run.update(properties={**run.properties, "reviewer": "flight-test-team"})
```

This is a read-modify-write operation, not an atomic merge with other writers.
Updating run metadata does not update the associated asset's metadata.

To close an ongoing run, set its end time:

```python
run.update(end=datetime.now(timezone.utc))
```

Changing time bounds changes the interval selected for analysis, not the stored data or any
active ingestion process. See {py:meth}`~nominal.core.Run.update` for other updatable fields.

## Add attachments and workbooks

Attach reports to the run when they describe this test. Use the asset's attachments for material
about the system across tests, such as a hardware specification.

```python
attachment = client.create_attachment("/path/to/flight_report.pdf")
run.add_attachments(attachments=[attachment])
```

Both standalone and asset-associated runs can be used to create workbooks from templates.
Choose a template whose input names match the run references or asset data scopes:

```python
template = client.get_workbook_template("<TEMPLATE_RID>")
template.create_workbook(
    run=run.rid,
    title="Flight review",
    description="Review of this test interval",
)
```

See [Workbooks](/guides/workbooks/overview.md) for finding template RIDs and opening the result.

## Remove directly attached data

To remove a direct reference from the selected run:

```python
run.remove_data_sources(ref_names=["telemetry"])
```

This does not delete the dataset or remove an asset's data scope. It applies to the standalone
example's `"telemetry"` reference, or any direct reference you added to an asset-associated run.
You can also pass resource objects or RIDs through `data_sources=`. See
{py:meth}`~nominal.core.Run.remove_data_sources` for details.
