---
myst:
  html_meta:
    description: "Upload CSV and Parquet files to a dataset, and choose the timestamp format."
---

# CSV and Parquet

{.lead}
Upload CSV and Parquet files with {py:meth}`~nominal.core.Dataset.add_tabular_data`.
Each file needs one timestamp column. Every other column becomes a channel.

For creating datasets and waiting for ingestion, see [Datasets](/guides/datasets/overview.md).

```{include} /guides/_snippets/connect-the-client.md
```

## Upload a file

```{include} /guides/_snippets/sample-flight-csv.md
```

```python
dataset = client.create_dataset("Frosty flight")
file = dataset.add_tabular_data("flight.csv", timestamp_column="time", timestamp_type="iso_8601")
file.poll_until_ingestion_completed()
```

`add_tabular_data` accepts `.csv`, `.csv.gz`, `.parquet` and `.parquet.gz`, plus `.parquet.tar`, `.parquet.tar.gz` and `.parquet.zip` archives.

CSV files must use commas between values and periods for decimals, such as `3.14`.
Convert files that use semicolons and decimal commas, such as `3,14`, before you upload them.

To upload a pandas DataFrame without writing a file, use {py:func}`~nominal.thirdparty.pandas.upload_dataframe_to_dataset`.

## Timestamp formats

`timestamp_type` describes the values in the timestamp column:

| Values | `timestamp_type` |
| --- | --- |
| `2024-06-08T05:58:42.000Z` | `"iso_8601"` |
| `1717826322` | `"epoch_seconds"` |
| `1717826322000000` | `"epoch_microseconds"` |
| Other units since the Unix epoch | `"epoch_nanoseconds"`, `"epoch_milliseconds"`, `"epoch_minutes"`, and so on |
| Time since the start of the recording | {py:class}`ts.Relative <nominal.ts.Relative>` |
| Any other absolute format | {py:class}`ts.Custom <nominal.ts.Custom>` |

Relative timestamps need the unit and the absolute time the recording started:

```python
from datetime import datetime, timezone

from nominal import ts

recording_start = datetime(2026, 9, 10, 14, 38, 19, tzinfo=timezone.utc)
dataset.add_tabular_data(
    "bench.csv",
    timestamp_column="elapsed_s",
    timestamp_type=ts.Relative("seconds", start=recording_start),
)
```

A value of `1.0` in `elapsed_s` then means `2026-09-10T14:38:20Z`.
See [Timestamps](/sdk/ts/index.md) for all formats.

## Tag the data

Tags label the points in a file so you can filter them later, such as by test or by sensor.
`tags` applies the same tags to every row, and `tag_columns` reads a tag's value from a column:

```python
dataset.add_tabular_data(
    "flight.csv",
    timestamp_column="time",
    timestamp_type="iso_8601",
    tags={"test_id": "flight-042"},
    tag_columns={"aircraft": "tail_number"},  # tag key: column name
)
```

See [tags](/guides/streaming/overview.md#add-tags-to-filter-data-in-a-run-or-asset) for filtering by them.
