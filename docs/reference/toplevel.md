# nominal

## nominal.create_run

```python
create_run(
    name: str,
    start: datetime | str | IntegralNanosecondsUTC,
    end: datetime | str | IntegralNanosecondsUTC,
    description: str | None = None,
) -> Run
```

Create a run in the Nominal platform.

To add a dataset to the run, use `run.add_dataset()`.

## nominal.create_run_csv

```python
create_run_csv(
    file: Path | str,
    name: str,
    timestamp_column: str,
    timestamp_type: Literal[
        "iso_8601",
        "epoch_days",
        "epoch_hours",
        "epoch_minutes",
        "epoch_seconds",
        "epoch_milliseconds",
        "epoch_microseconds",
        "epoch_nanoseconds",
    ],
    description: str | None = None,
) -> Run
```

Create a dataset from a CSV file, and create a run based on it.

This is a convenience function that combines `upload_csv()` and `create_run()` and can only be used with absolute
timestamps. For relative timestamps or custom formats, use `upload_dataset()` and `create_run()` separately.

The name and description are added to the run. The dataset is created with the name "Dataset for Run: {name}".
The reference name for the dataset in the run is "dataset".

The run start and end times are created from the minimum and maximum timestamps in the CSV file in the timestamp
column.

## nominal.download_attachment

```python
download_attachment(rid: str, file: Path | str) -> None
```

Retrieve an attachment from the Nominal platform and save it to `file`.

## nominal.get_attachment

```python
get_attachment(rid: str) -> Attachment
```

Retrieve an attachment from the Nominal platform by its RID.

## nominal.get_dataset

```python
get_dataset(rid: str) -> Dataset
```

Retrieve a dataset from the Nominal platform by its RID.

## nominal.get_default_client

```python
get_default_client() -> NominalClient
```

Retrieve the default client to the Nominal platform.

## nominal.get_log_set

```python
get_log_set(rid: str) -> LogSet
```

Retrieve a log set from the Nominal platform by its RID.

## nominal.get_run

```python
get_run(rid: str) -> Run
```

Retrieve a run from the Nominal platform by its RID.

## nominal.get_video

```python
get_video(rid: str) -> Video
```

Retrieve a video from the Nominal platform by its RID.

## nominal.search_runs

```python
search_runs(
    *,
    start: str | datetime | IntegralNanosecondsUTC | None = None,
    end: str | datetime | IntegralNanosecondsUTC | None = None,
    exact_name: str | None = None,
    label: str | None = None,
    property: tuple[str, str] | None = None
) -> list[Run]
```

Search for runs meeting the specified filters.

Filters are ANDed together, e.g. `(run.label == label) AND (run.end <= end)`

- `start` and `end` times are both inclusive
- `exact_name` is case-insensitive
- `property` is a key-value pair, e.g. ("name", "value")

## nominal.set_base_url

```python
set_base_url(base_url: str) -> None
```

Set the default Nominal platform base url.

For production environments: "https://api.gov.nominal.io/api".
For staging environments: "https://api-staging.gov.nominal.io/api".
For local development: "https://api.nominal.test".

## nominal.upload_attachment

```python
upload_attachment(
    file: Path | str, name: str, description: str | None = None
) -> Attachment
```

Upload an attachment to the Nominal platform.

## nominal.upload_csv

```python
upload_csv(
    file: Path | str,
    name: str | None,
    timestamp_column: str,
    timestamp_type: TimestampColumnType,
    description: str | None = None,
    *,
    wait_until_complete: bool = True
) -> Dataset
```

Create a dataset in the Nominal platform from a .csv or .csv.gz file.

If `name` is None, the dataset is created with the name of the file.

## nominal.upload_pandas

```python
upload_pandas(
    df: pd.DataFrame,
    name: str,
    timestamp_column: str,
    timestamp_type: TimestampColumnType,
    description: str | None = None,
    *,
    wait_until_complete: bool = True
) -> Dataset
```

Create a dataset in the Nominal platform from a pandas.DataFrame.

## nominal.upload_polars

```python
upload_polars(
    df: pl.DataFrame,
    name: str,
    timestamp_column: str,
    timestamp_type: TimestampColumnType,
    description: str | None = None,
    *,
    wait_until_complete: bool = True
) -> Dataset
```

Create a dataset in the Nominal platform from a polars.DataFrame.

## nominal.upload_video

```python
upload_video(
    file: Path | str,
    name: str,
    start: datetime | str | IntegralNanosecondsUTC,
    description: str | None = None,
) -> Video
```

Upload a video to Nominal from a file.
