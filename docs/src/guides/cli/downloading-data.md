---
myst:
  html_meta:
    description: "Select an asset's dataset and export channel samples to CSV or Parquet with the Nominal CLI"
---

# Download data with the CLI

{.lead}
Select an asset's dataset and export channel samples to CSV or Parquet

`nom download` is an interactive wizard for exporting channel samples over a time interval.
It starts from an **asset**, lets you choose one of its datasets, and writes the selected data
as local CSV or Parquet parts. It does not download the original uploaded files.

For a dataset that is only attached directly to a run, or for whole-file downloads, use the
[Python export guide](/guides/datasets/export-data.md).

## Start the wizard

Install the client with `pip install nominal` and
[save a connection profile](/guides/api-keys-setup.md), then run:

```bash
nom download --profile default
```

Replace `default` with your profile name. Parquet is the default output format; to write CSV:

```bash
nom download --profile default --format csv
```

The wizard asks for an asset, a dataset, channels, an output directory, and time bounds, then
asks you to confirm the export. Use `nom download --help` for the installed version's options.

## Select the data

### Asset and dataset

Choose an asset by its displayed index. Assets are sorted by their last update time, newest
first. The next selection lists datasets attached to that asset with their data scope names
(shown as **Refname**). If there is only one dataset, the wizard selects it automatically.

The export applies the selected asset data scope's **series tags**. It does not apply that scope's
time offset. If your scope uses an offset, supply bounds in the dataset's time coordinates;
returned timestamps remain in the dataset's time coordinates.

### Channels

At the channel prompt, enter comma-separated substrings or `*` for all channels:

```text
Enter exact channel substrings, separated by a comma [* for all]: RPM, TEMP
```

Each substring is searched separately, so `RPM, TEMP` selects matches from either search.
These are case-insensitive substring matches, not regular expressions or full-name equality.
A prefix such as `VEHICLE.ENGINE_1` also matches names containing that text elsewhere.

You can view the selected names, descriptions, types, and units before proceeding. The text
editor can **remove names from the current selection**; adding a name that was not selected
does not add its channel. To broaden the selection, decline the final channel confirmation and
search again. Overlapping searches can include the same channel more than once, so prefer
non-overlapping terms or select `*` and remove unwanted names.

The CLI warns above 500 selected channels, but this is not a hard limit. Select only what you
need, especially for long intervals or high-rate data.

### Output directory

Enter a directory, for example `./flight_data`. The wizard creates it if needed.
Use a fresh directory for each export: filenames are based on the dataset RID and part number,
so exporting the same dataset into the same directory can overwrite parts and leave stale files
from an earlier, longer export.

### Time bounds

Choose one of three sources for the time interval:

| Choice | What it supplies |
| --- | --- |
| `event` | The event's start and duration. If the duration is zero, the wizard asks you to enter one, such as `5m`. |
| `run` | The run's start and end. For an ongoing run, the wizard asks for an explicit end timestamp. |
| `custom` | Start and end timestamps you enter directly. |

**Selecting a run or event changes only the time bounds.** It does not change the selected asset,
dataset, channels, or tag filters. Check that the interval belongs to the data you intend to export.

For custom bounds, enter UTC timestamps with an explicit `Z`, for example:

```text
Enter start timestamp (UTC), e.g. 2025-09-03T10:15:00Z: 2026-09-10T14:00:00Z
Enter end timestamp (UTC): 2026-09-10T15:00:00Z
```

Review the UTC bounds shown by the wizard and edit them if needed. For an ongoing run, the
suggested end time uses your machine's local clock without a timezone, but the wizard interprets
it as UTC. Unless your machine uses UTC, replace the suggestion with the intended UTC end time,
including `Z`. Finally, confirm the duration and selected channel count to begin downloading.

## Understand the output

The CLI writes files named after the final component of the dataset RID:

```text
flight_data/
  abc123-part_0.parquet
  abc123-part_1.parquet
  abc123-part_2.parquet
```

`abc123` is illustrative; use the actual prefix from your output. CSV exports use the same pattern
with `.csv`. Each part covers a time slice, with channel groups joined on their sample timestamps.

| Output | Meaning |
| --- | --- |
| Time column | Normally `timestamp`: floating-point epoch **seconds** in UTC, not integer nanoseconds. If a channel already uses this name, the exporter chooses another time-column name. |
| Channel columns | Exported numeric or string values. The current exporter supports `DOUBLE`, `INT`, and `STRING` channels. |
| Missing samples | Nulls from joining channels with different sample times. CSV readers may infer their own missing-value representation. |

Numeric timestamps use `Float64`, so the output is not a lossless nanosecond representation.
CSV also relies on the reading application's type inference; inspect types before analysis.

:::{warning}
The CLI uses the experimental Polars exporter. Unsupported channel types are skipped with a
warning, and failed batch downloads can be logged and skipped. Channels without data in the
selected interval may be absent. Inspect logs and check expected channels and time coverage;
reaching the final loading instructions alone does not establish that the export is complete.
:::

## Read the downloaded parts

Inspect the time-column name and the columns in your files first. The examples below assume the
time column is `timestamp` and use the illustrative prefix `abc123`.

### Python with Polars

For Parquet:

```python
from pathlib import Path

import polars as pl

files = sorted(Path("flight_data").glob("abc123-part_*.parquet"))
frames = [pl.read_parquet(path) for path in files]
nominal_data = pl.concat(frames, how="diagonal_relaxed").sort("timestamp")
print(nominal_data.head())
```

For CSV, replace the file-selection and reading lines with:

```python
files = sorted(Path("flight_data").glob("abc123-part_*.csv"))
frames = [pl.read_csv(path) for path in files]
nominal_data = pl.concat(frames, how="diagonal_relaxed").sort("timestamp")
```

`diagonal_relaxed` aligns columns by name and fills absent columns with nulls, accommodating parts
with different channel columns. It can also coerce differing types to a common type, so inspect
`nominal_data.schema`. These examples load all parts into memory; process individual files if the
full export is too large. If no files match, check the directory, prefix, and export logs before
concatenating.

### Python with pandas

Install `pandas` and a Parquet engine such as `pyarrow` if your environment does not already have
them. For Parquet:

```python
from pathlib import Path

import pandas as pd

files = sorted(Path("flight_data").glob("abc123-part_*.parquet"))
nominal_data = pd.concat(
    [pd.read_parquet(path) for path in files], ignore_index=True
).sort_values("timestamp")
```

For CSV, use the `.csv` pattern and `pd.read_csv(path)` instead of `pd.read_parquet(path)`.
pandas aligns columns by name and fills missing values. Check the inferred dtypes, particularly
for CSV identifiers that look numeric or contain leading zeros.

### MATLAB

The wizard prints MATLAB loading instructions for the chosen format. Use a **Parquet reader for
Parquet** and a **text-table reader for CSV**; changing only the filename extension is not enough.

For example, read Parquet parts into a cell array of tables:

```matlab
files = dir(fullfile("flight_data", "abc123-part_*.parquet"));
parts = cell(numel(files), 1);
for k = 1:numel(files)
    parts{k} = parquetread(fullfile(files(k).folder, files(k).name));
end
```

For CSV:

```matlab
files = dir(fullfile("flight_data", "abc123-part_*.csv"));
parts = cell(numel(files), 1);
for k = 1:numel(files)
    parts{k} = readtable(fullfile(files(k).folder, files(k).name));
end
```

Inspect the tables' variable names and types before combining them. When all parts have matching
columns and compatible types, concatenate and sort:

```matlab
nominal_data = sortrows(vertcat(parts{:}), "timestamp");
```

If parts have different channel columns, align their schemas first or analyze them individually.
Convert epoch seconds to datetimes with `datetime(..., "ConvertFrom", "posixtime", "TimeZone", "UTC")`
when needed; do not interpret the timestamp values as nanoseconds.

## Tune download performance

| Option | Default | What it controls |
| --- | --- | --- |
| `--format` | `parquet` | Output format: `parquet` or `csv`. |
| `--channels-per-request` | `10` | Channel grouping within export requests; this is not the number of worker threads. |
| `--points-per-file` | `25000000` | Target point budget used with estimated channel rates to choose time slices. It is not a strict row-count, byte-size, or memory limit. |

Both numeric options must be at least 1. Start with the defaults on a representative time window,
then change one setting at a time while keeping the channels and interval the same.
Use a fresh output directory for each trial.

### Many channels: try larger request groups

When exporting many low-rate channels, larger groups can reduce the number of requests. For
example, compare the default of 10 with 50:

```bash
nom download --profile default --channels-per-request 50 --format parquet
```

The exporter also limits groups using estimated sample rates, so a group may contain fewer
channels than this setting. Increasing it does not increase the worker count, and larger
requests are not always faster.

### Few channels: try smaller request groups

If all selected channels fit into one request, smaller groups can create more independent work
for the download workers. For a small channel selection, try:

```bash
nom download --profile default --channels-per-request 1 --format parquet
```

This adds request overhead, so compare elapsed time before using it for a longer export.
The useful setting depends on sample rates and request sizes, not just the number of CPU cores.

### Memory pressure: reduce the batch target

Lower `--points-per-file` to request smaller time slices:

```bash
nom download --profile default --points-per-file 1000000 --format parquet
```

Smaller slices can reduce memory used to assemble each part and make downstream processing more
manageable, at the cost of more requests and output files. The target uses estimated rates;
it is not a hard memory ceiling. If memory remains a problem, select fewer channels or a shorter
interval, and read the resulting parts individually rather than loading them all at once.

### Output format and selection

Prefer Parquet when your analysis tools support it: the CLI writes compressed Parquet and
preserves column types. CSV is useful for text-based tools, but readers must parse values and
infer types. The format controls the local output files; both choices use the same export pipeline.

Selecting fewer channels and a shorter interval reduces the data requested. Use subsystem
substrings to narrow the selection, then remove unused channels in the editor. For repeated
analysis, retain the downloaded files rather than exporting the same interval again.

## Troubleshoot a selection

- **No assets available:** check the profile's workspace and your access to assets. `nom config profile list`
  shows saved profiles; it does not verify server access.
- **No datasets for this asset:** choose an asset with an attached dataset. For a dataset outside
  an asset, use [Python export](/guides/datasets/export-data.md).
- **No channels found:** broaden the substring query or use `*`, then remove unwanted names.
- **Failed to get an event or run:** copy its RID from Nominal and check that your profile can
  access it. The wizard allows another attempt.
- **Missing output data:** review exporter warnings and errors, channel types, the selected time
  interval, and the asset scope's tags. Do not assume the requested channel count equals the
  number of exported columns.

## See also

- [Export data with Python](/guides/datasets/export-data.md): bounded channel exports and whole-file downloads.
- [Datasets overview](/guides/datasets/overview.md): dataset ownership, ingestion, and metadata.
- [CLI overview](/guides/cli/overview.md): other available commands.
