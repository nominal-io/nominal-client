---
myst:
  html_meta:
    description: "Read channel data into DataFrames, export it in batches to CSV or Parquet, or download stored files."
---

# Export data

{.lead}
Read channel data into a DataFrame, export long time ranges in batches, or download the files a dataset was built from.

To download data from the terminal instead, see [Download data](/guides/cli/downloading-data.md).

```{include} /guides/_snippets/connect-the-client.md
```

## Read a dataset into pandas

{py:func}`~nominal.thirdparty.pandas.datasource_to_dataframe` returns a DataFrame indexed by timestamp, with one column per channel:

```python
from nominal.thirdparty.pandas import datasource_to_dataframe

dataset = client.get_dataset("<DATASET_RID>")
df = datasource_to_dataframe(dataset, channel_exact_match=["temperature"], start="2026-09-10T14:38:00Z")
```

Leave out the filters to read every channel. `start` and `end` also take datetimes and epoch nanoseconds.
For charts of long recordings, read fewer points with [Decimate data](/guides/datasets/decimating-data.md).

## Export in batches

{py:class}`~nominal.thirdparty.polars.polars_export_handler.PolarsExportHandler` yields Polars DataFrames one time window at a time, so long exports don't need to fit in memory.
This example writes a run's `temperature` and `speed` channels to Parquet:

```python
import time
from datetime import timedelta
from pathlib import Path

from nominal.thirdparty.polars.polars_export_handler import PolarsExportHandler

run = client.get_run("<RUN_RID>")
dataset = run.get_dataset("telemetry")
channels = [dataset.get_channel("temperature"), dataset.get_channel("speed")]
end = run.end or time.time_ns()  # a run that's still going has no end

out = Path("exports") / run.rid
out.mkdir(parents=True, exist_ok=True)
for i, df in enumerate(
    PolarsExportHandler(client).export(channels, start=run.start, end=end, batch_duration=timedelta(minutes=10))
):
    df.write_parquet(out / f"batch_{i:04d}.parquet")  # or df.write_csv(...)
```

`start` and `end` are integer epoch nanoseconds.
For data on the run's asset, get the dataset with {py:meth}`~nominal.core.Asset.get_dataset` instead, as in [Runs](/guides/runs.md#create-a-run-from-datasets).

Some behavior to check before you rely on an export:

- The handler is experimental. It exports `DOUBLE`, `INT` and `STRING` channels, and skips other types with a warning.
- A batch that fails to download is logged and skipped, so check the logs and the row coverage.
- Output timestamps are `Float64`, which can't tell apart every nanosecond at current epoch values.
- Batches outer-join channels on timestamp by default, so missing values are nulls. Pass `join_batches=False` for one frame per channel group.

## Filter by tags

A run or asset can select part of a dataset by [tags](/guides/streaming/overview.md#add-tags-to-filter-data-in-a-run-or-asset).
Exports don't apply that selection automatically, so pass the same tags:

```python
PolarsExportHandler(client).export(channels, start=run.start, end=end, tags={"test_id": "flight-042"})
```

## Download stored files

{py:meth}`~nominal.core.Dataset.search_files` finds the files whose time range overlaps an interval, and {py:meth}`~nominal.core.DatasetFile.download` saves each whole file:

```python
for file in dataset.search_files(start=run.start, end=run.end):
    out = Path("downloads") / file.id  # one folder per file, as names can repeat
    out.mkdir(parents=True, exist_ok=True)
    file.download(out)
```

Downloads include samples outside the interval.
For the inputs an [extractor](/guides/extractors/overview.md) processed, use {py:meth}`~nominal.core.DatasetFile.download_original_files`.

To query data with SQL, see the [SQL interface](https://docs.nominal.io/developers/sql/overview).
