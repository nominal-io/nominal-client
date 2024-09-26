# nominal.core

## nominal.core.IntegralNanosecondsUTC

```python
IntegralNanosecondsUTC = int

```

## nominal.core.Attachment

```python
Attachment(
    rid: str,
    name: str,
    description: str,
    properties: Mapping[str, str],
    labels: Sequence[str],
    _client: NominalClient,
) -> None
```

### description

```python
description: str

```

### labels

```python
labels: Sequence[str]

```

### name

```python
name: str

```

### properties

```python
properties: Mapping[str, str]

```

### rid

```python
rid: str

```

### get_contents

```python
get_contents() -> BinaryIO
```

Retrieve the contents of this attachment.
Returns a file-like object in binary mode for reading.

### update

```python
update(
    *,
    name: str | None = None,
    description: str | None = None,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] | None = None
) -> Self
```

Replace attachment metadata.
Updates the current instance, and returns it.

Only the metadata passed in will be replaced, the rest will remain untouched.

Note: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
calling this method. E.g.:

```
new_labels = ["new-label-a", "new-label-b", *attachment.labels]
attachment = attachment.update(labels=new_labels)
```

### write

```python
write(path: Path, mkdir: bool = True) -> None
```

Write an attachment to the filesystem.

`path` should be the path you want to save to, i.e. a file, not a directory.

## nominal.core.CustomTimestampFormat

```python
CustomTimestampFormat(format: str, default_year: int = 0) -> None
```

### default_year

```python
default_year: int = 0

```

### format

```python
format: str

```

## nominal.core.Dataset

```python
Dataset(
    rid: str,
    name: str,
    description: str | None,
    properties: Mapping[str, str],
    labels: Sequence[str],
    _client: NominalClient,
) -> None
```

### description

```python
description: str | None

```

### labels

```python
labels: Sequence[str]

```

### name

```python
name: str

```

### properties

```python
properties: Mapping[str, str]

```

### rid

```python
rid: str

```

### add_csv_to_dataset

```python
add_csv_to_dataset(
    path: Path | str,
    timestamp_column: str,
    timestamp_type: TimestampColumnType,
) -> None
```

Append to a dataset from a csv on-disk.

### add_to_dataset_from_io

```python
add_to_dataset_from_io(
    dataset: BinaryIO,
    timestamp_column: str,
    timestamp_type: TimestampColumnType,
    file_type: tuple[str, str] | FileType = FileTypes.CSV,
) -> None
```

Append to a dataset from a file-like object.

file_type: a (extension, mimetype) pair describing the type of file.

### poll_until_ingestion_completed

```python
poll_until_ingestion_completed(
    interval: timedelta = timedelta(seconds=1),
) -> None
```

Block until dataset ingestion has completed.
This method polls Nominal for ingest status after uploading a dataset on an interval.

**Raises:**

- <code>[NominalIngestFailed](#nominal.exceptions.NominalIngestFailed)</code> – if the ingest failed
- <code>[NominalIngestError](#nominal.exceptions.NominalIngestError)</code> – if the ingest status is not known

### update

```python
update(
    *,
    name: str | None = None,
    description: str | None = None,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] | None = None
) -> Self
```

Replace dataset metadata.
Updates the current instance, and returns it.

Only the metadata passed in will be replaced, the rest will remain untouched.

Note: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
calling this method. E.g.:

```
new_labels = ["new-label-a", "new-label-b"]
for old_label in dataset.labels:
    new_labels.append(old_label)
dataset = dataset.update(labels=new_labels)
```

## nominal.core.LogSet

```python
LogSet(
    rid: str,
    name: str,
    timestamp_type: LogTimestampType,
    description: str | None,
    _client: NominalClient,
) -> None
```

### description

```python
description: str | None

```

### name

```python
name: str

```

### rid

```python
rid: str

```

### timestamp_type

```python
timestamp_type: LogTimestampType

```

### stream_logs

```python
stream_logs() -> Iterable[Log]
```

Iterate over the logs.

## nominal.core.NominalClient

```python
NominalClient(
    _auth_header: str,
    _run_client: scout.RunService,
    _upload_client: upload_api.UploadService,
    _ingest_client: ingest_api.IngestService,
    _catalog_client: scout_catalog.CatalogService,
    _attachment_client: attachments_api.AttachmentService,
    _video_client: scout_video.VideoService,
    _logset_client: datasource_logset.LogSetService,
) -> None
```

### create

```python
create(
    base_url: str,
    token: str | None,
    trust_store_path: str | None = None,
) -> Self
```

Create a connection to the Nominal platform.

base_url: The URL of the Nominal API platform, e.g. "https://api.gov.nominal.io/api".
token: An API token to authenticate with. By default, the token will be looked up in ~/.nominal.yml.
trust_store_path: path to a trust store CA root file to initiate SSL connections. If not provided,
certifi's trust store is used.

### create_attachment_from_io

```python
create_attachment_from_io(
    attachment: BinaryIO,
    name: str,
    file_type: tuple[str, str] | FileType = FileTypes.BINARY,
    description: str | None = None,
    *,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] = ()
) -> Attachment
```

Upload an attachment.
The attachment must be a file-like object in binary mode, e.g. open(path, "rb") or io.BytesIO.
If the file is not in binary-mode, the requests library blocks indefinitely.

### create_csv_dataset

```python
create_csv_dataset(
    path: Path | str,
    name: str | None,
    timestamp_column: str,
    timestamp_type: TimestampColumnType,
    description: str | None = None,
    *,
    labels: Sequence[str] = (),
    properties: Mapping[str, str] | None = None
) -> Dataset
```

Create a dataset from a CSV file.

If name is None, the name of the file will be used.

See `create_dataset_from_io` for more details.

### create_dataset_from_io

```python
create_dataset_from_io(
    dataset: BinaryIO,
    name: str,
    timestamp_column: str,
    timestamp_type: TimestampColumnType,
    file_type: tuple[str, str] | FileType = FileTypes.CSV,
    description: str | None = None,
    *,
    labels: Sequence[str] = (),
    properties: Mapping[str, str] | None = None
) -> Dataset
```

Create a dataset from a file-like object.
The dataset must be a file-like object in binary mode, e.g. open(path, "rb") or io.BytesIO.
If the file is not in binary-mode, the requests library blocks indefinitely.

Timestamp column types must be a `CustomTimestampFormat` or one of the following literals:
"iso_8601": ISO 8601 formatted strings,
"epoch\_{unit}": epoch timestamps in UTC (floats or ints),
"relative\_{unit}": relative timestamps (floats or ints),
where {unit} is one of: nanoseconds | microseconds | milliseconds | seconds | minutes | hours | days

### create_log_set

```python
create_log_set(
    name: str,
    logs: (
        Iterable[Log]
        | Iterable[tuple[datetime | IntegralNanosecondsUTC, str]]
    ),
    timestamp_type: LogTimestampType = "absolute",
    description: str | None = None,
) -> LogSet
```

Create an immutable log set with the given logs.

The logs are attached during creation and cannot be modified afterwards. Logs can either be of type `Log`
or a tuple of a timestamp and a string. Timestamp type must be either 'absolute' or 'relative'.

### create_run

```python
create_run(
    name: str,
    start: datetime | IntegralNanosecondsUTC,
    end: datetime | IntegralNanosecondsUTC,
    description: str | None = None,
    *,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] = (),
    attachments: Iterable[Attachment] | Iterable[str] = ()
) -> Run
```

Create a run.

### create_video_from_io

```python
create_video_from_io(
    video: BinaryIO,
    name: str,
    start: datetime | IntegralNanosecondsUTC,
    description: str | None = None,
    file_type: tuple[str, str] | FileType = FileTypes.MP4,
    *,
    labels: Sequence[str] = (),
    properties: Mapping[str, str] | None = None
) -> Video
```

Create a video from a file-like object.

The video must be a file-like object in binary mode, e.g. open(path, "rb") or io.BytesIO.

### get_attachment

```python
get_attachment(rid: str) -> Attachment
```

Retrieve an attachment by its RID.

### get_attachments

```python
get_attachments(rids: Iterable[str]) -> Sequence[Attachment]
```

Retrive attachments by their RIDs.

### get_dataset

```python
get_dataset(rid: str) -> Dataset
```

Retrieve a dataset by its RID.

### get_datasets

```python
get_datasets(rids: Iterable[str]) -> Sequence[Dataset]
```

Retrieve datasets by their RIDs.

### get_log_set

```python
get_log_set(log_set_rid: str) -> LogSet
```

Retrieve a log set along with its metadata given its RID.

### get_run

```python
get_run(rid: str) -> Run
```

Retrieve a run by its RID.

### get_video

```python
get_video(rid: str) -> Video
```

Retrieve a video by its RID.

### get_videos

```python
get_videos(rids: Iterable[str]) -> Sequence[Video]
```

Retrieve videos by their RID.

### search_runs

```python
search_runs(
    start: datetime | IntegralNanosecondsUTC | None = None,
    end: datetime | IntegralNanosecondsUTC | None = None,
    exact_name: str | None = None,
    label: str | None = None,
    property: tuple[str, str] | None = None,
) -> Sequence[Run]
```

Search for runs meeting the specified filters.
Filters are ANDed together, e.g. `(run.label == label) AND (run.end <= end)`

- `start` and `end` times are both inclusive
- `exact_name` is case-insensitive
- `property` is a key-value pair, e.g. ("name", "value")

## nominal.core.Run

```python
Run(
    rid: str,
    name: str,
    description: str,
    properties: Mapping[str, str],
    labels: Sequence[str],
    start: IntegralNanosecondsUTC,
    end: IntegralNanosecondsUTC | None,
    _client: NominalClient,
) -> None
```

### description

```python
description: str

```

### end

```python
end: IntegralNanosecondsUTC | None

```

### labels

```python
labels: Sequence[str]

```

### name

```python
name: str

```

### properties

```python
properties: Mapping[str, str]

```

### rid

```python
rid: str

```

### start

```python
start: IntegralNanosecondsUTC

```

### add_attachments

```python
add_attachments(
    attachments: Iterable[Attachment] | Iterable[str],
) -> None
```

Add attachments that have already been uploaded to this run.

`attachments` can be `Attachment` instances, or attachment RIDs.

### add_dataset

```python
add_dataset(ref_name: str, dataset: Dataset | str) -> None
```

Add a dataset to this run.

Datasets map "ref names" (their name within the run) to a Dataset (or dataset rid). The same type of datasets
should use the same ref name across runs, since checklists and templates use ref names to reference datasets.

### add_datasets

```python
add_datasets(datasets: Mapping[str, Dataset | str]) -> None
```

Add multiple datasets to this run.

Datasets map "ref names" (their name within the run) to a Dataset (or dataset rid). The same type of datasets
should use the same ref name across runs, since checklists and templates use ref names to reference datasets.

### add_log_set

```python
add_log_set(ref_name: str, log_set: LogSet | str) -> None
```

Add a log set to this run.

Log sets map "ref names" (their name within the run) to a Log set (or log set rid).

### add_log_sets

```python
add_log_sets(log_sets: Mapping[str, LogSet | str]) -> None
```

Add multiple log sets to this run.

Log sets map "ref names" (their name within the run) to a Log set (or log set rid).

### list_attachments

```python
list_attachments() -> Sequence[Attachment]
```

### list_datasets

```python
list_datasets() -> Sequence[tuple[str, Dataset]]
```

List the datasets associated with this run.
Returns (ref_name, dataset) pairs for each dataset.

### remove_attachments

```python
remove_attachments(
    attachments: Iterable[Attachment] | Iterable[str],
) -> None
```

Remove attachments from this run.
Does not remove the attachments from Nominal.

`attachments` can be `Attachment` instances, or attachment RIDs.

### update

```python
update(
    *,
    name: str | None = None,
    description: str | None = None,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] | None = None
) -> Self
```

Replace run metadata.
Updates the current instance, and returns it.
Only the metadata passed in will be replaced, the rest will remain untouched.

Note: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
calling this method. E.g.:

```
new_labels = ["new-label-a", "new-label-b"]
for old_label in run.labels:
    new_labels.append(old_label)
run = run.update(labels=new_labels)
```

## nominal.core.Video

```python
Video(
    rid: str,
    name: str,
    description: str | None,
    properties: Mapping[str, str],
    labels: Sequence[str],
    _client: NominalClient,
) -> None
```

### description

```python
description: str | None

```

### labels

```python
labels: Sequence[str]

```

### name

```python
name: str

```

### properties

```python
properties: Mapping[str, str]

```

### rid

```python
rid: str

```

### poll_until_ingestion_completed

```python
poll_until_ingestion_completed(
    interval: timedelta = timedelta(seconds=1),
) -> None
```

Block until video ingestion has completed.
This method polls Nominal for ingest status after uploading a video on an interval.

**Raises:**

- <code>[NominalIngestFailed](#nominal.exceptions.NominalIngestFailed)</code> – if the ingest failed
- <code>[NominalIngestError](#nominal.exceptions.NominalIngestError)</code> – if the ingest status is not known

### update

```python
update(
    *,
    name: str | None = None,
    description: str | None = None,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] | None = None
) -> Self
```

Replace video metadata.
Updates the current instance, and returns it.

Only the metadata passed in will be replaced, the rest will remain untouched.

Note: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
calling this method. E.g.:

```
new_labels = ["new-label-a", "new-label-b"]
for old_label in video.labels:
    new_labels.append(old_label)
video = video.update(labels=new_labels)
```
