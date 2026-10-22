---
myst:
  html_meta:
    description: "Create datasets, upload files to them, wait for ingestion and inspect their channels."
---

# Datasets

{.lead}
A dataset holds timestamped data as named channels, such as `altitude` or `temperature`.
Files you upload and points you stream add to the same dataset over time.

```{include} /guides/_snippets/connect-the-client.md
```

## Create a dataset and upload a file

Create a dataset with {py:meth}`~nominal.core.NominalClient.create_dataset`, or on an asset with {py:meth}`~nominal.core.Asset.get_or_create_dataset` (see [Assets](/guides/assets.md#add-data-with-data-scopes)).
Then upload a file to it:

```python
dataset = client.create_dataset("Flight telemetry")
file = dataset.add_tabular_data("flight_001.csv", timestamp_column="time", timestamp_type="iso_8601")
file.poll_until_ingestion_completed()  # raises NominalIngestError if ingestion fails
print(dataset.nominal_url)
```

Each column other than `timestamp_column` becomes a channel.
`timestamp_type` describes the timestamp values, as in [Timestamp formats](/guides/datasets/csv.md#timestamp-formats).

Upload methods return a {py:class}`~nominal.core.DatasetFile` before ingestion finishes.
Wait for it before you read the new channels or the file's bounds.

Uploading another file adds to the dataset rather than replacing its data.
Keep channel names, types and units the same across files.

## File formats

| Format | Method | Guide |
| --- | --- | --- |
| CSV, Parquet | {py:meth}`~nominal.core.Dataset.add_tabular_data` | [CSV](/guides/datasets/csv.md) |
| Journal JSON logs | {py:meth}`~nominal.core.Dataset.add_journal_json` | [Journal JSON](/guides/datasets/journal-json.md) |
| HDF5 | convert, then {py:meth}`~nominal.core.Dataset.add_tabular_data` | [HDF5](/guides/datasets/hdf-5.md) |
| PX4 ULog | convert, then {py:meth}`~nominal.core.Dataset.add_tabular_data` | [PX4](/guides/datasets/px-4.md) |
| Avro streams | {py:meth}`~nominal.core.Dataset.add_avro_stream` | [Avro streams](/guides/datasets/avro-streams.md) |
| MCAP | {py:meth}`~nominal.core.Dataset.add_mcap` | [MCAP video ingest](/guides/video/mcap-video-ingest.md) |
| Video | {py:meth}`~nominal.core.Dataset.add_video` | [Video ingest](/guides/video/video-ingest.md) |
| Other binary formats | {py:meth}`~nominal.core.Dataset.add_containerized` | [File extractors](/guides/extractors/overview.md) |

To send values as they're produced, [stream](/guides/streaming/overview.md) them instead.

## Inspect channels and files

```python
dataset.refresh()
print(dataset.bounds)  # start and end in epoch nanoseconds, or None

for channel in dataset.get_channels():
    print(channel.name, channel.data_type, channel.unit)

for f in dataset.list_files(successful_only=False):  # include failed and pending files
    print(f.name, f.ingest_status.name)
```

The dataset's bounds span all its files. `file.bounds` covers one file.

To set channel types and units, see [Channels and data types](/guides/datasets/channels.md).

## Retrieve a dataset

Retrieve a dataset by the RID on its page in Nominal, or search by name and labels with {py:meth}`~nominal.core.NominalClient.search_datasets`:

```python
dataset = client.get_dataset("<DATASET_RID>")
```

This returns the dataset's metadata, not its data. To download data, see [Export data](/guides/datasets/export-data.md).

## Update metadata

{py:meth}`~nominal.core.Dataset.update` replaces each field you pass and leaves the others unchanged.
To add a property, merge it with the existing ones:

```python
dataset.update(labels=["flight-test"], properties={**dataset.properties, "campaign": "2026-09"})
```

Labels and properties describe the whole dataset.
To select part of its data, use [tags](/guides/streaming/overview.md#add-tags-to-filter-data-in-a-run-or-asset).
