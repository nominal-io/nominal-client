---
myst:
  html_meta:
    description: "Create a dataset, ingest and inspect data, and add recordings over time with the Nominal Python Client."
---

# Datasets in Python

{.lead}
Create a dataset, ingest and inspect data, and add recordings over time with the Nominal Python Client.

A **dataset** is a named resource that holds timestamped data in channels. It can receive data
from multiple uploaded files or from a live stream. Depending on the ingestion method, those
channels can contain numeric measurements, strings, structured values, logs, or video.
A dataset is not a single file, and creating one does not upload any data.

These objects play different roles:

| Object | Role |
| --- | --- |
| Dataset | Holds data and channel metadata, plus a name, labels, properties, and a resource ID (RID). |
| Dataset file | Tracks an individual ingested file, including its ingestion status and time bounds. Streaming points does not require uploading a file. |
| Channel | A named signal within a dataset, with a data type and optional unit and description. |
| Asset | Organizes data around a system, such as a vehicle or test stand, using named data scopes. |
| Run | Selects a time interval for analysis, using directly attached datasets, associated assets, or both. |

Attaching a dataset to an asset or run references the existing data; it does not upload a copy.
For an asset's ongoing recordings, you can create or reuse its dataset with
{py:meth}`~nominal.core.Asset.get_or_create_dataset`. The example below creates a dataset directly;
see [Assets](/guides/assets.md) and [Runs](/guides/runs.md) for organizing it.

## Connect to Nominal

Install the client with `pip install nominal` and
[save an authentication profile](/guides/api-keys-setup.md) before running the examples.
The Python blocks below continue the same workflow unless labeled as an alternative.

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")
```

## Upload a dataset

First save this small, synthetic recording as `flight_001.csv` in your working directory:

```text
source_time,altitude,temperature
2026-09-10T14:38:19Z,100.0,20.0
2026-09-10T14:38:20Z,101.5,20.2
2026-09-10T14:38:21Z,103.0,20.4
```

Create an empty dataset, then upload the file into it. `source_time` is the timestamp column;
`"iso_8601"` describes the values in that column.

```python
dataset = client.create_dataset("Flight telemetry")
dataset_file = dataset.add_tabular_data(
    "flight_001.csv",
    timestamp_column="source_time",
    timestamp_type="iso_8601",
)

dataset_file.poll_until_ingestion_completed()
print("Ingested dataset:", dataset.rid)
print("Open in Nominal:", dataset.nominal_url)
```

`add_tabular_data()` returns a `DatasetFile` before ingestion finishes. Keep that handle and poll
it before using the uploaded channels or file bounds. Polling raises if ingestion fails; the
success message above is reached only after the wait completes.

`create_dataset()` creates a new dataset each time. To continue working with an existing one,
retrieve it by RID as described [below](#retrieve-a-dataset). Repeating an upload adds another
file; reusing a dataset does not make the upload itself idempotent.

For your own CSV or Parquet file, change the path, timestamp column, and timestamp type to match
its contents. The [CSV guide](/guides/datasets/csv.md) covers input formatting and a larger sample.

## Inspect the ingested data

Refresh the dataset to retrieve its latest metadata, then inspect its channels and time bounds:

```python
dataset.refresh()
print("Dataset bounds:", dataset.bounds)
print("Uploaded file bounds:", dataset_file.bounds)

for channel in dataset.get_channels():
    print(channel.name, channel.data_type, channel.unit)
```

The dataset's bounds describe its overall time range, which can include other uploads. The file's
bounds describe this recording. Bounds can be `None` when unavailable; when present, their
`start` and `end` values are integer epoch nanoseconds.

To inspect ingestion status across files, include unsuccessful and pending files explicitly:

```python
for uploaded_file in dataset.list_files(successful_only=False):
    print(uploaded_file.name, uploaded_file.ingest_status.name)
```

`list_files()` defaults to successfully ingested files. Listing files does not wait for ingestion.
For a batch that can produce new files while running, use the job workflow in
[Ingest jobs](/guides/ingest/ingestion-jobs.md).

## Append to a dataset

Save the next recording as `flight_002.csv`:

```text
source_time,altitude,temperature
2026-09-10T14:38:22Z,104.0,20.5
2026-09-10T14:38:23Z,105.5,20.7
```

Upload it to the same dataset:

```python
next_file = dataset.add_tabular_data(
    "flight_002.csv",
    timestamp_column="source_time",
    timestamp_type="iso_8601",
)
next_file.poll_until_ingestion_completed()
dataset.refresh()
print("Updated dataset bounds:", dataset.bounds)
```

This adds data rather than replacing the first recording. Keep channel names, types, and units
consistent across recordings. To control channel types before any data arrives, see
[Channels and data types](/guides/datasets/channels.md).

## Set channel units

Units describe channel values, not the dataset as a whole. After ingesting the sample, assign
meters to altitude and degrees Celsius to temperature:

```python
dataset.set_channel_units(
    {"altitude": "m", "temperature": "Cel"},
    validate_schema=True,
)
```

`validate_schema=True` rejects names that are not channels in the dataset. With the default
`False`, unknown names are ignored. Setting unit metadata does not rescale the stored values;
choose units that describe the data you uploaded.

Nominal validates unit symbols against UCUM by default. These units support workbook unit
conversion and analysis. Use `allow_display_only_units=True` for custom labels that should not
participate in UCUM conversions, such as an encoder's `"encoder_ticks"`. This is for values
already expressed in that custom unit; it is not a way to convert the sample's altitude.

To update one channel, use {py:meth}`~nominal.core.Channel.update`; passing `unit=None` clears its
unit. You can also clear a unit with `dataset.set_channel_units({"altitude": None})`.
Use {py:meth}`~nominal.core.NominalClient.get_unit` to check a symbol,
{py:meth}`~nominal.core.NominalClient.get_commensurable_units` for compatible units, or
{py:meth}`~nominal.core.NominalClient.get_all_units` to list available units.

To register channel types, units, and descriptions before uploading or streaming, follow
[Channels and data types](/guides/datasets/channels.md#pre-registering-a-channel).

## Relative timestamps

The sample uses absolute ISO 8601 timestamps. If your file instead records elapsed time, such as
`0.0`, `0.5`, and `1.0` seconds, use `nominal.ts.Relative` to specify both the unit and the absolute
time at which the recording began:

```python
from datetime import datetime, timezone

from nominal import ts

relative_time = ts.Relative(
    "seconds",
    start=datetime(2026, 9, 10, 14, 38, 19, tzinfo=timezone.utc),
)
```

For that alternative file, pass `timestamp_type=relative_time` and name its elapsed-time column
in `timestamp_column`. An elapsed value of `1.0` then represents `2026-09-10T14:38:20Z`.
Do not use this format for the ISO 8601 sample above.

Choose the unit that matches the recorded values. For epoch timestamps, use a format such as
`"epoch_seconds"` or `"epoch_nanoseconds"`. See [Timestamp formats](/sdk/ts/index.md) for supported
units and custom formats.

## Retrieve a dataset

Save the RID printed after ingestion, or copy it from the dataset's page in Nominal. In a later
session, retrieve the same dataset instead of creating another one:

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")
dataset = client.get_dataset("<DATASET_RID>")
print(dataset.name, dataset.labels, dataset.properties)
```

Replace `<DATASET_RID>` with your dataset's RID. `get_dataset()` retrieves a resource object and
its metadata; it does not download the stored samples. To download files or export channel data
for a time interval, use [Export data](/guides/datasets/export-data.md).

## Set labels and properties

Labels and properties describe the dataset. Use `update()` to replace the fields you supply;
omitted fields remain unchanged:

```python
dataset.update(
    description="Altitude and temperature from the flight test",
    labels=["Flight test"],
    properties={"campaign": "September 2026"},
)
```

To add to existing metadata, refresh it and merge the new values before updating:

```python
dataset.refresh()
dataset.update(properties={**dataset.properties, "review_status": "pending"})
```

This is a read-modify-write operation, not an atomic merge with other writers. Pass `labels=[]`
or `properties={}` to clear those fields. Dataset metadata does not filter individual samples;
use [data tags](/guides/streaming/overview.md#add-tags-to-filter-data-in-a-run-or-asset)
when you need to select a subset of the data.

## Choose an ingestion workflow

- [CSV](/guides/datasets/csv.md): upload tabular recordings with a timestamp column.
- [HDF5](/guides/datasets/hdf-5.md) and [PX4](/guides/datasets/px-4.md): prepare format-specific data.
- [Avro streams](/guides/datasets/avro-streams.md): ingest files with channels sampled at different times.
- [Stream data](/guides/streaming/overview.md): write live measurements and use tags to distinguish recordings.
- [Video ingest](/guides/video/video-ingest.md): add video channels with aligned timestamps.
- [Ingest jobs](/guides/ingest/ingestion-jobs.md): submit and track batches of files.
- [File extractors](/guides/extractors/overview.md): run reusable conversion logic during ingestion.

For the complete API, see {py:class}`~nominal.core.Dataset`.
