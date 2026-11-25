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

For raw channels that each match one series in a stored dataset, the handler downloads typed Parquet and aligns channels in Polars. A channel split across ingest files can join a raw job when exactly one series matches its tags in that job’s time window. Channels that still match multiple series in that window use compute. Decimated and derived channels also use compute. Both paths require async export jobs in the selected workspace.

Set `batch_duration` when you know how much time each frame should cover. This skips the sample-rate queries used for automatic sizing unless you also set `points_per_request`. Each job can include up to 256 channels by default. The handler yields time windows in order. Joined exports can process two adjacent windows concurrently. With `join_batches=False`, the handler can use spare workers for later windows when each window contains only a few jobs. Use `num_workers` to tune concurrency for your deployment and available memory.


- The handler is experimental. It exports `DOUBLE`, `INT` and `STRING` channels, and skips other types with a warning.
- The deployment needs export storage configured. Iteration raises if a job or its download fails. Batches yielded before the failure are still available.
- The default epoch timestamps are `Float64`. Use `timestamp_type="iso_8601"` for UTC datetimes with nanosecond precision.
- Batches outer-join channels on timestamp by default, so missing values are nulls.

### Identify the source dataset

Frames carry channel names without a source dataset RID. If the requested names are unique, keep a lookup alongside the export:

```python
source_by_channel = {channel.name: channel.data_source for channel in channels}
```

Suppose two datasets both have an `rpm` channel. With the default `join_batches=True`, requesting both raises `ValueError` on the first iteration, before any export job starts. With `join_batches=False`, both datasets can yield a frame with an `rpm` column. Time windows arrive in order, but jobs within each window can finish in any order. To retain the source for same-named channels, export each dataset separately and label its output:

```python
handler = PolarsExportHandler(client)
for dataset in (engine_dataset, pump_dataset):
    folder = out / dataset.rid
    folder.mkdir(parents=True, exist_ok=True)
    for i, frame in enumerate(handler.export([dataset.get_channel("rpm")], start=run.start, end=end)):
        frame.write_parquet(folder / f"batch_{i:04d}.parquet")
```

### Export channels with different sample rates

For hundreds of slow channels alongside a few high-rate channels, pass all the channels in one call with `join_batches=False`. Within each raw SQL job and time window, channels with identical timestamps share a frame. A dropped sample or a clock offset can put two channels of the same nominal rate into separate frames. Each frame contains only channels with samples in that window. Compute jobs yield one frame per job.

Four 10 kHz channels contribute 4.8 million samples per two-minute window. Shorten the window or lower `num_workers` to reduce peak memory.

```python
frames = PolarsExportHandler(client).export(
    channels,
    start=run.start,
    end=end,
    batch_duration=timedelta(minutes=2),
    join_batches=False,
    timestamp_type="iso_8601",
)
for i, frame in enumerate(frames):
    frame.write_parquet(out / f"native_{i:04d}.parquet")
```

## Filter by tags

A run or asset can select part of a dataset by [tags](/guides/streaming/overview.md#add-tags-to-filter-data-in-a-run-or-asset).
Exports don't apply that selection automatically, so pass the same tags:

```python
for df in PolarsExportHandler(client).export(channels, start=run.start, end=end, tags={"test_id": "flight-042"}):
    print(df.head())
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
