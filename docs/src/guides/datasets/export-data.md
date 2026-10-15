---
myst:
  html_meta:
    description: "Download stored dataset files or export selected channel data to Polars DataFrames, CSV, and Parquet"
---

# Export data

{.lead}
Download stored dataset files or export selected channel data to Polars DataFrames, CSV, and Parquet

```{include} /guides/_snippets/install-warning.md
```

Choose the output you need:

- **Stored files:** download whole dataset files in their ingested format. A time filter selects
  overlapping files; it does not trim their contents.
- **Channel samples:** select channels and a time range, then export DataFrames to CSV or Parquet
  with `PolarsExportHandler`.

For an interactive CLI workflow, see [Download data](/guides/cli/downloading-data.md).

## Download dataset files

This example downloads files from datasets **directly attached to a run**. `run.list_datasets()`
does not include datasets owned by the run's assets. For asset-owned data, resolve the dataset
from the asset as shown [below](#export-asset-owned-data).

```python
from pathlib import Path

from nominal.core import NominalClient

client = NominalClient.from_profile("default")
run = client.get_run("<RUN_RID>")

for _, dataset in run.list_datasets():
    for dataset_file in dataset.search_files(start=run.start, end=run.end):
        output_dir = Path("downloaded_files") / dataset.rid / dataset_file.id
        output_dir.mkdir(parents=True, exist_ok=True)
        path = dataset_file.download(output_dir)
        print(f"Downloaded {dataset_file.name} to {path}")
```

`search_files()` selects files whose time ranges overlap the requested interval, with search
bounds truncated to whole seconds. For an ongoing run, `end=None` leaves the search open-ended;
this lists current matches, not future uploads. `download()` retrieves each **whole file**, including any
samples outside the run. Use channel export below when you need samples limited to a time range.

Separate directories prevent collisions between different files with the same name. Downloading
again to an existing destination file raises `FileExistsError`. For extractor inputs rather than
ingested output files, use `DatasetFile.download_original_files()`.

## Export channel samples

Install Polars first:

```bash
pip install polars
```

:::{warning}
`PolarsExportHandler` is experimental. It supports `DOUBLE`, `INT`, and `STRING` channels;
unsupported channel types are skipped with a warning. Failed batch downloads can be logged and
skipped, so exhausting the iterator does not prove that every requested sample was exported.
Inspect export logs and validate the resulting coverage before relying on a complete export.
:::

### Export a dataset attached directly to a run

This example resolves one dataset by its run reference name, selects two channels by their full
names, and writes ten-minute batches to Parquet. Replace `"telemetry"`, `"temperature"`, and
`"speed"` with your reference and channel names.

The examples assume no data-scope time offset or tag filter. Resolving a dataset returns the
underlying dataset; it does not apply the run or asset scope's filters or offsets. See
[Filter by tags](#filter-by-tags) for scoped data selection. For an offset scope, translate the
run bounds into the dataset's time coordinates before exporting.

```python
from datetime import timedelta
from pathlib import Path
from time import time_ns

from nominal.core import NominalClient
from nominal.thirdparty.polars.polars_export_handler import PolarsExportHandler

client = NominalClient.from_profile("default")
run = client.get_run("<RUN_RID>")
dataset = run.get_dataset("telemetry")
channels = [dataset.get_channel("temperature"), dataset.get_channel("speed")]

# Capture one end bound for an ongoing run.
end = run.end if run.end is not None else time_ns()
output_dir = Path("exports") / run.rid / dataset.rid
output_dir.mkdir(parents=True, exist_ok=True)
exporter = PolarsExportHandler(client)

for idx, df in enumerate(
    exporter.export(
        channels,
        start=run.start,
        end=end,
        batch_duration=timedelta(minutes=10),
        timestamp_type="epoch_seconds",
    )
):
    df.write_parquet(output_dir / f"batch_{idx:04d}.parquet")
```

Use `df.write_csv(output_dir / f"batch_{idx:04d}.csv")` for CSV instead. Polars quotes column names
and string values when needed; read the result with a CSV parser that supports quoted fields.
Use a new output directory for each export to avoid overwriting previous batches or leaving
stale batches from a longer earlier export.

To repeat this workflow for labeled runs, iterate over `client.search_runs(labels=["Failure"])`
and resolve each run's dataset and bounds inside the loop. `run.list_datasets()` supplies
`(reference_name, dataset)` pairs when you want to process all directly attached datasets;
exporting each separately keeps identically named channels in different datasets distinct.

### Export asset-owned data

A run can instead select a time interval on an asset's data. Resolve the dataset from the asset's
data scope, and use the run only for the export bounds. You do not need to attach another copy of
the dataset directly to the run.

For a run associated with exactly one asset, replace the preceding dataset lookup with:

```python
(asset,) = run.list_assets()
dataset = asset.get_dataset("telemetry")
```

If the run has several assets, select the intended one from `run.list_assets()` instead of
unpacking a single result. Use its data scope name in `get_dataset()`. Then select channels from this
dataset and export with the same `start` and `end`. `run.get_dataset()` and `run.list_datasets()`
resolve direct run references; they do not search the associated assets.

### Choose time bounds and batch output

`start` and `end` must be integer epoch **nanoseconds**. `run.start` already uses this unit;
`run.end` can be `None` for an ongoing run, which is why the example captures `time_ns()`.
These input units are independent of `timestamp_type`, which controls output timestamps and
defaults to `"epoch_seconds"` (also explicit in the examples).

Numeric output timestamps are currently stored as `Float64`, including when requesting
`"epoch_nanoseconds"`. At modern epoch values, this cannot distinguish every nanosecond;
nearby timestamps can round to the same value. Choosing nanosecond units or writing Parquet
does not restore that precision. These examples do not provide a lossless nanosecond export.

For a custom interval, convert timezone-aware datetimes without floating-point arithmetic:

```python
from datetime import datetime, timedelta, timezone

start_time = datetime(2026, 3, 23, 23, 50, 27, tzinfo=timezone.utc)
end_time = datetime(2026, 3, 23, 23, 58, 31, tzinfo=timezone.utc)
epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
start_ns = ((start_time - epoch) // timedelta(microseconds=1)) * 1_000
end_ns = ((end_time - epoch) // timedelta(microseconds=1)) * 1_000

for df in exporter.export(channels, start=start_ns, end=end_ns):
    print(df.head())
```

This preserves Python `datetime`'s microsecond precision in the **request bounds**. Pass integer
nanoseconds directly if your source has finer precision; this does not change the output
precision limit described above.

`export()` always yields Polars DataFrames. `batch_duration` controls the time window per batch;
if omitted, the exporter chooses a duration from estimated channel rates. It does not guarantee
a fixed row count or file size.

With `join_batches=True` (the default), channel groups for each time slice are outer-joined on
the timestamp column. Missing channel values are Polars **nulls**, not automatically `NaN`.
With `join_batches=False`, each yielded frame contains one channel group for that time slice;
multiple frames can cover the same interval.

## Filter by tags

If the run reference or asset data scope selects a subset of the dataset with tags, pass those
same tags to `export()`. A resolved dataset and its channels do not carry that scope selection
into the export request.

Using the dataset, channels, run, and captured `end` from the earlier example:

```python
tags = {"ingest_uuid": "<INGEST_UUID>"}

for df in exporter.export(
    channels,
    start=run.start,
    end=end,
    tags=tags,
    timestamp_type="epoch_seconds",
):
    print(df.head())
```

Use the tags configured on your run reference or asset data scope, such as those shown on the
asset detail page in Nominal. One `tags` mapping applies to all channels in that export call;
export scopes with different tag selections separately. These are channel-data filters.
For whole-file downloads, `dataset.search_files(file_tags=...)` filters file metadata and still
downloads the entire matching files.

## See also

- [Download data (CLI)](/guides/cli/downloading-data.md): interactive CLI wizard for downloading data to disk
- [Decimate data](/guides/datasets/decimating-data.md): retrieve decimated data points for visualization
- [Datasets overview](/guides/datasets/overview.md): create, upload, and manage datasets
- [SQL interface](https://docs.nominal.io/developers/sql/overview): query data with `SELECT` statements and export a result set as CSV
