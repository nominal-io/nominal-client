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

Use `dataset.search_files(start=..., end=...)` to find ingested files whose time bounds overlap a
window, then `file.download()` to download each complete file. This example searches the datasets
attached directly to a run. For an ongoing run, it captures the current time as the window's end:

```{literalinclude} /guides/_snippets/code/data_retrieval/export_data_download.py
:language: python
```

The search bounds are truncated to whole seconds. Downloads contain the complete ingested files,
including data outside the search window; downloading a file does not clip it to the run's bounds.
For the original input files of a containerized extractor, use
{py:meth}`~nominal.core.DatasetFile.download_original_files` instead. Those inputs can differ from
the ingested outputs returned by `download()`.

## Export data from runs

Use `PolarsExportHandler` to export {abbr}`channel (A named signal for a series of measurements or computed values (example: voltage, pressure, system state).)` data from runs as [Polars](https://pola.rs/) DataFrames. Install the additional dependency first:

```bash
pip install polars
```

The following example searches for runs by label and exports selected channels from datasets
attached directly to each run using `run.list_datasets()`. It assumes those references have no tag
filters or offsets. Data scopes inherited from assets are not included by `run.list_datasets()`.

```{literalinclude} /guides/_snippets/code/data_retrieval/export_data_1.py
:language: python
```

:::{note}

The `start` and `end` parameters require integer epoch nanoseconds. `run.start` and a completed
run's `run.end` already use those units. An ongoing run has `run.end=None`, so the recipes capture
a cutoff with `time.time_ns()` before exporting.

For a custom range, convert timezone-aware datetimes using integer timedelta arithmetic to avoid
float rounding. This preserves the datetime's microsecond precision; use original integer
nanoseconds when your source has finer precision:

```python
from datetime import datetime, timedelta, timezone

start = datetime(2026, 3, 23, 23, 50, 27, tzinfo=timezone.utc)
end = datetime(2026, 3, 23, 23, 58, 31, tzinfo=timezone.utc)
epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
start_ns = (start - epoch) // timedelta(microseconds=1) * 1_000
end_ns = (end - epoch) // timedelta(microseconds=1) * 1_000

for idx, df in enumerate(
    exporter.export(
        channels,
        start=start_ns,
        end=end_ns,
        batch_duration=timedelta(seconds=600),
    )
):
    ...
```
:::

Use `batch_duration` to control how much time each DataFrame covers. The exporter supports DOUBLE,
INT, and STRING channels; it filters out other channel types with a warning.

:::{tip}

Use `dataset.search_channels(exact_match=["keyword"])` to find channels by name. To combine results from multiple keyword searches, call `search_channels` multiple times and concatenate the results.
:::

The `export()` method yields DataFrames in batches. With `join_batches=True`, each yielded DataFrame contains all requested channels joined on a shared timestamp column.

At timestamps where a channel has no data, its values are Polars nulls.

With `join_batches=False`, each yielded DataFrame contains a subset of channels for that batch of time. This can be faster for large channel counts.

:::{note}

You can also write to binary Parquet with `df.write_parquet(path)` for better compression and faster read times on large datasets.
:::

:::{note}

CSV output may include quotes around column names and string (enum) values. Ensure your CSV parser is configured to handle quoted fields.
:::

## Export a dataset inherited from an asset

The [data integration tutorial](/guides/data-integration-tutorial.md) creates runs with
`asset.create_run()` and keeps datasets on the asset. For that workflow, retrieve the known
asset's dataset by its refname, then export its channels over the run's time window:

```python
from time import time_ns

from nominal.core import NominalClient
from nominal.thirdparty.polars.polars_export_handler import (
    PolarsExportHandler,
)

client = NominalClient.from_profile("default")
run = client.get_run("<run rid>")
asset = client.get_asset("<asset rid>")
dataset = asset.get_dataset("flight_data")
channels = list(
    dataset.search_channels(exact_match=["temperature"]),
)
end = run.end if run.end is not None else time_ns()

exporter = PolarsExportHandler(client)
for index, df in enumerate(
    exporter.export(channels, start=run.start, end=end),
):
    df.write_parquet(f"flight_data_{run.run_number}_{index}.parquet")
```

Use the RID of the asset associated with this run, its dataset refname (`flight_data` in the
tutorial), and the channel-name substring you want to export. This selects one asset and dataset
explicitly. Like the direct-run examples, it assumes the reference has no timestamp offset or tag
filter. Resolving the dataset does not apply those settings; use the tag-filter recipe below when
needed. The [download CLI](/guides/cli/downloading-data.md) also lets you select an asset and dataset
interactively.

## Filter by tags

Resolving a run reference to a `Dataset` does not apply its data scope's tag filter or timestamp
offset. Pass the intended `tags` when exporting filtered data. The following example applies one
filter to every selected dataset; export scopes separately when they need different filters.
These examples use timestamps as stored in the dataset and assume references without offsets:

```{literalinclude} /guides/_snippets/code/data_retrieval/export_data_2.py
:language: python
```

Use the tag values configured for the data scope you want to export, which you can inspect on the
asset or run detail page in the Nominal UI.

## See also

- [Download data (CLI)](/guides/cli/downloading-data.md): interactive CLI wizard for downloading data to disk
- [Decimate data](/guides/datasets/decimating-data.md): retrieve decimated data points for visualization
- [Datasets overview](/guides/datasets/overview.md): create, upload, and manage datasets
- [SQL interface](https://docs.nominal.io/developers/sql/overview): query data with `SELECT` statements and export a result set as CSV
