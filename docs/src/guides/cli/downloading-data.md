---
myst:
  html_meta:
    description: "Export an asset's channel data to CSV or Parquet with the nom download wizard."
---

# Download data

{.lead}
`nom download` asks for an asset, a dataset, channels and a time range, then saves the channel data as CSV or Parquet files.
It exports samples, not the files that were uploaded.

You need a profile from [API keys setup](/guides/api-keys-setup.md).
To export from Python, or download the uploaded files, see [Export data](/guides/datasets/export-data.md).

## Run the wizard

```bash
nom download --profile default  # Parquet
nom download --profile default --format csv
```

The wizard asks for:

1. **An asset**, newest first.
1. **A dataset** on that asset, listed with its data scope name. An asset with one dataset skips this step.
1. **Channels**, as comma-separated text to match, such as `RPM, TEMP`, or `*` for all.
   Matching is case-insensitive and finds the text anywhere in a name.
   You can then remove channels in an editor, but not add them. To add channels, decline the selection and search again.
1. **An output folder**, created if needed. Use a new folder for each export, because files from an earlier export can be overwritten or left behind.
1. **A time range** from an event, a run or timestamps you enter.
   Use UTC timestamps ending in `Z`, such as `2026-09-10T14:00:00Z`.
   For a run that's still going, the suggested end is your local time read as UTC, so replace it unless your machine uses UTC.

Choosing a run or event sets only the time range, not the asset, dataset or channels.
The export applies the data scope's tags, but not its time offset.

## The output

Files are named after the end of the dataset's RID, such as `abc123-part_0.parquet`. Each covers a slice of time.

- The `timestamp` column holds epoch seconds as floats, so it can't represent every nanosecond.
- Channels sampled at different times have nulls where they don't line up.
- `DOUBLE`, `INT` and `STRING` channels are exported. Others are skipped with a warning.
- Batches that fail are logged and skipped, so check the warnings and the time coverage.

To combine the parts in Python:

```python
from pathlib import Path

import polars as pl

parts = sorted(Path("flight_data").glob("abc123-part_*.parquet"))
data = pl.concat([pl.read_parquet(p) for p in parts], how="diagonal_relaxed").sort("timestamp")
```

`diagonal_relaxed` fills columns missing from some parts with nulls.
In MATLAB, read Parquet parts with `parquetread` and CSV parts with `readtable`, and convert timestamps with `datetime(t, "ConvertFrom", "posixtime", "TimeZone", "UTC")`.

## Tune large downloads

| Option | Default | Effect |
| --- | --- | --- |
| `--channels-per-request` | `10` | Channels per request. Raise it for many low-rate channels, lower it to spread a few channels across workers. |
| `--points-per-file` | `25000000` | Target points per file. Lower it to use less memory per part. |

Change one option at a time on a short range, into a new folder each time.
Selecting fewer channels or a shorter range helps most.

If the wizard finds no assets, check the profile's workspace with `nom config profile list`.
For a dataset that isn't on an asset, [export it from Python](/guides/datasets/export-data.md).
