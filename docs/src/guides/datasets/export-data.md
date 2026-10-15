---
myst:
  html_meta:
    description: "Export time-series data from Nominal to Polars DataFrames and Parquet files using the Python SDK"
---

# Export data

{.lead}
Export time-series data from Nominal to Polars DataFrames and Parquet files using the Python SDK

```{include} /guides/_snippets/install-warning.md
```

:::{warning}

The `PolarsExportHandler` is experimental. If you encounter bugs or unexpected behavior, report them to the Nominal team.
:::

This guide shows how to programmatically export data from Nominal using the Python SDK. If you prefer an interactive CLI-based workflow, see [Download data](/guides/cli/downloading-data.md).

## Download dataset files

If your dataset contains uploaded files (CSV, Parquet, HDF5, etc.), you can download those files directly using `dataset.list_files()` and `file.download()`. The following example collects all datasets attached to a run and downloads their files within the run's time bounds:

```python
from pathlib import Path

from nominal.core import NominalClient

client = NominalClient.from_profile("default")

run = client.get_run("<run rid>")

output_dir = Path("downloaded_files")
output_dir.mkdir(exist_ok=True)

for _, dataset in run.list_datasets():
    for file in dataset.list_files(start=run.start, end=run.end):
        path = file.download(output_dir)
        print(f"Downloaded {file.name} to {path}")
```

Each `DatasetFile` is downloaded in its ingested format to the specified directory.

## Export data from runs

Use `PolarsExportHandler` to export {abbr}`channel (A named signal for a series of measurements or computed values (example: voltage, pressure, system state).)` data from runs as [Polars](https://pola.rs/) DataFrames. Install the additional dependency first:

```bash
pip install polars
```

The following example searches for runs by label, collects channels across all datasets on each run using `run.list_datasets()`, and exports each run's data to CSV files:

```python
import re
from datetime import timedelta

import polars as pl
from nominal.core import NominalClient
from nominal.thirdparty.polars.polars_export_handler import PolarsExportHandler

client = NominalClient.from_profile("default")

runs = client.search_runs(labels=["Failure"])

for run in runs:
    channels = []
    for _, dataset in run.list_datasets():
        channels.extend(list(dataset.search_channels(exact_match=["temperature"])))
        channels.extend(list(dataset.search_channels(exact_match=["speed"])))

    exporter = PolarsExportHandler(client)

    for idx, df in enumerate(
        exporter.export(
            channels,
            start=run.start,
            end=run.end,
            batch_duration=timedelta(seconds=600),
        )
    ):
        if isinstance(df, pl.Series):
            df = df.to_frame()

        safe_name = re.sub(r"[^\w\-_.]", "_", run.name)
        df.write_csv(f"{safe_name}_{run.run_number}_{idx}.csv")
```

:::{note}

The `start` and `end` parameters accept `run.start` and `run.end` directly, but if you specify your own time range, you must provide the time as epoch nanoseconds in integer form. You can convert a `datetime` to epoch nanoseconds using `int(dt.timestamp() * 1e9)`:

```python
from datetime import datetime, timedelta, timezone

start = datetime(2026, 3, 23, 23, 50, 27, tzinfo=timezone.utc)
end   = datetime(2026, 3, 23, 23, 58, 31, tzinfo=timezone.utc)

for idx, df in enumerate(
    exporter.export(
        channels,
        start=int(start.timestamp() * 1e9),
        end=int(end.timestamp() * 1e9),
        batch_duration=timedelta(seconds=600),
    )
):
    ...
```
:::

Use `batch_duration` to control how much data each batch covers. The exporter may yield `pl.Series` in some cases, so the snippet converts them to DataFrames before writing.

:::{tip}

Use `dataset.search_channels(exact_match=["keyword"])` to find channels by name. To combine results from multiple keyword searches, call `search_channels` multiple times and concatenate the results.
:::

The `export()` method yields DataFrames in batches. With `join_batches=True`, each yielded DataFrame contains all requested channels joined on a shared timestamp column.

Timestamps where a channel has no data are filled with `NaN`.

With `join_batches=False`, each yielded DataFrame contains a subset of channels for that batch of time. This can be faster for large channel counts.

:::{note}

You can also write to binary Parquet with `df.write_parquet(path)` for better compression and faster read times on large datasets.
:::

:::{note}

CSV output may include quotes around column names and string (enum) values. Ensure your CSV parser is configured to handle quoted fields.
:::

## Filter by tags

If your dataset was ingested using data scopes with tags (e.g. `ingest_uuid`), pass the same tags when exporting to select the correct data:

```python
from nominal.core import NominalClient
from nominal.thirdparty.polars.polars_export_handler import PolarsExportHandler

client = NominalClient.from_profile("default")

run = client.get_run("<run rid>")

tags = {"ingest_uuid": "your-ingest-uuid"}

channels = []
for _, dataset in run.list_datasets():
    channels.extend(list(dataset.search_channels(exact_match=["temperature"])))

exporter = PolarsExportHandler(client)

for df in exporter.export(
    channels,
    start=run.start,
    end=run.end,
    tags=tags,
):
    print(df.head())
```

You can find the tags for a dataset's data scope on the asset detail page in the Nominal UI, or by inspecting the data source programmatically.

## See also

- [Download data (CLI)](/guides/cli/downloading-data.md): interactive CLI wizard for downloading data to disk
- [Decimate data](/guides/datasets/decimating-data.md): retrieve decimated data points for visualization
- [Datasets overview](/guides/datasets/overview.md): create, upload, and manage datasets
- [SQL interface](https://docs.nominal.io/developers/sql/overview): query data with `SELECT` statements and export a result set as CSV
