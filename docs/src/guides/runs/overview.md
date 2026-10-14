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

It's possible to create an empty run without any data. Runs need an absolute start time; pass an absolute end time or `end=None` for an open-ended run.
All Datasets added to the run should overlap with this time domain.

```{literalinclude} /guides/_snippets/code/sdk/python/runs/create_run.py
:language: python
```

## Create a run off of an asset

It's also possible to create a run off of an existing asset:

```{literalinclude} /guides/_snippets/code/sdk/python/runs/create_run_from_asset.py
:language: python
```

## Add data to a run

To add a Dataset to a run, use {py:obj}`Run.add_dataset() <nominal.core.Run.add_dataset>`.
The sample download below uses `huggingface_hub`; install it with `pip install huggingface_hub`.

```{literalinclude} /guides/_snippets/code/sdk/python/runs/add_data_to_run.py
:language: python
```

```{include} /guides/_snippets/what-is-a-dataset.md
```

The example above sets the start and end times manually. To bound the run to this test's uploaded files, keep the handles returned by each upload and wait for them to finish ingesting. Use their extents rather than `Dataset.bounds`, which spans every test accumulated on that dataset:

```{literalinclude} /guides/_snippets/code/sdk/python/runs/update_run_time_bounds.py
:language: python
```

## Wait for ingestion before creating a run

When a run spans several datasources that ingest at different rates (for example, multiple video streams and a telemetry channel from the same test event), lower-bitrate sources typically finish ingesting first. Selecting a run from the runs page before every attached datasource finishes ingesting can cause inconsistent behavior in the workbook UI: the `align-to-run` action can appear available and display the wrong video if the referenced video dataset hasn't finished ingesting.

Poll every file for ingestion completion before creating the run. This example assumes dataset names are unique within the run; otherwise, choose distinct refnames for the mapping:

```python
for f in files:
    f.poll_until_ingestion_completed()

run = client.create_run(name="my_run", start=start, end=end)
run.add_datasets({dataset.name: dataset for dataset in datasets})
```

Apply the same pattern before creating a workbook from a template: poll every attached file for ingestion completion, then create the workbook. This keeps runs and workbooks hidden from users until their data finishes ingesting.

## Run data with ref names

To add a Dataset to a run along with a reference name, set the `ref_name` parameter in {py:obj}`Run.add_dataset() <nominal.core.Run.add_dataset>`.

```{literalinclude} /guides/_snippets/code/sdk/python/runs/add_dataset_with_ref_name.py
:language: python
```

```{include} /guides/_snippets/what-is-a-ref-name.md
```

## Check if a run exists

You can check for a run's existence with {py:obj}`client.search_runs() <nominal.core.NominalClient.search_runs>`.

```{literalinclude} /guides/_snippets/code/sdk/python/runs/check_run_exists.py
:language: python
```

## Update a run

Run metadata can be updated with {py:obj}`Run.update() <nominal.core.Run.update>`:

For example, to set a run's end time to the present moment:

```{literalinclude} /guides/_snippets/code/sdk/python/runs/update_run_end_time.py
:language: python
```

To update a run's title:

```{literalinclude} /guides/_snippets/code/sdk/python/runs/update_run_title.py
:language: python
```

To add labels to a run:

```{literalinclude} /guides/_snippets/code/sdk/python/runs/add_labels_to_run.py
:language: python
```

Please see {py:obj}`Run.update() <nominal.core.Run.update>` for all updatable metadata.

## Run attachments

File attachments such as PDF reports or PowerPoints can be added to Runs:

```{literalinclude} /guides/_snippets/code/sdk/python/runs/add_run_attachments.py
:language: python
```

## Retrieve a run

Like Datasets, Runs can be retrieved by their resource ID ("RID"):

```{literalinclude} /guides/_snippets/code/sdk/python/runs/retrieve_run.py
:language: python
```

To retrieve a Run's RID, visit its detail page and click on the clipboard icon next to "ID" in the right-hand drawer:

![run-metadata](/guides/images/b4/run_metadata_drawer_jq7iww.png)

```{include} /guides/_snippets/what-is-a-rid.md
```

## Query Runs

Runs can be queried with {py:obj}`client.search_runs() <nominal.core.NominalClient.search_runs>`.

For example, to retrieve all runs with the label "X-PLANE":

```{literalinclude} /guides/_snippets/code/sdk/python/runs/query_runs.py
:language: python
```

See {py:obj}`client.search_runs() <nominal.core.NominalClient.search_runs>` for all run search parameters.

## Remove Run Data Sources

The list `data_sources` can contain Connection, Dataset, Video instances, or rids as string.

```{literalinclude} /guides/_snippets/code/sdk/python/runs/remove_run_data_sources.py
:language: python
```
