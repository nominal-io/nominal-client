# Performant Multi-File Ingest Job Machinery

> EXPERIMENTAL / UNSTABLE — backed by an in-development backend service whose contract may
> change without notice.

Traditionally, files are ingested into Nominal as individual, atomic units.
Each file gets its own per-file ingest tag and shows up in the files page of its dataset.
All of the typical client methods work this way today (with one notable exception, `add_containerized`):

```python
dataset = client.get_dataset(...)
dataset_file1 = dataset.add_tabular_data(...)
dataset_file2 = dataset.add_mcap(...)
```

`IngestionJob` is a new mechanism for representing *batches* of files within Nominal.
Imagine this typical scenario:

- You have a large recording from your aircraft, 250+ GiB.
- To work with the data effectively (on your machines and those hosting the backend), you split it into 1–2 GiB chunks.
- What is really one "flight archive" is now 250+ separate files in Nominal, and difficult to keep track of.

Now those files can all be uploaded as one combined `IngestionJob`, and you can track their ingest progress as a unit —
no more scrolling the files page counting how many are done vs. ingesting vs. enqueued.

## How to Use It

```python
from datetime import UTC, datetime

from nominal.core import NominalClient
from nominal.experimental.ingest import IngestBuilder

client = NominalClient.from_profile("default")
dataset = client.get_dataset("rid...")

# tags here are applied as defaults to every file, overrideable per file
builder = IngestBuilder(client, dataset, tags={"aircraft": "test-tail-1"})

# add Parquet files (use add_csv for CSV, or add_tabular_data to infer the format)
builder.add_parquet(
    "test.parquet",
    timestamp_column="time",
    timestamp_type="epoch_seconds",
    tags={"subsystem": "nav"},
)
builder.add_parquet(
    "test2.parquet",
    timestamp_column="time2",
    timestamp_type="epoch_nanoseconds",
    tags={"subsystem": "airframe"},
)

# add a dataflash file
builder.add_ardupilot_dataflash("test.bin")

# add a video, frames timestamped from a known start instant
# (or pass frame_timestamps=[...] for one epoch-nanosecond timestamp per frame)
builder.add_video(
    "cam_front.mp4",
    channel="camera.front",
    start=datetime(2026, 7, 26, 6, 0, tzinfo=UTC),
)

# run a containerized extractor over named inputs
builder.add_containerized(
    extractor="extractor.rid...",
    sources={
        "input_a": "path/to/special.abc",
        "input_b": "path/to/other.abc",
    },
    arguments={"ARG_A": "banana"},
    tags={"host": "machine.123", "subsystem": "black box"},
)

# upload every file in parallel, then trigger ONE ingest job.
# Builders are single-use: one submit() per builder, successful or not.
job = builder.submit()

# track the whole batch as a unit
for file in job.as_files_ingested():
    print(f"ingested {file.name}")
```

By default `submit()` is atomic: any file failing permanently cancels the batch and raises an
exception group naming each failed file, and nothing is ingested. Pass
`submit(allow_partial=True)` to instead drop failed items (logged as errors) and ingest
everything that uploaded cleanly.

## Uploading files in a batch

`submit()` sends local files through `nominal.core.MultipartUploader`. Small files use a single request to
Nominal. Larger files share the core uploader's storage workers and payload memory budget.
One gate across both routes backs off and retries when Nominal throttles requests.

When using `MultipartUploader.create()` directly, set `max_buffer_bytes` to limit the total
bytes held for small request bodies and multipart chunks across the batch. The largest
individual request body must fit this limit. Use a smaller `enqueue_file(part_size=...)` for
hosts with limited memory. HTTP and executor overhead require additional memory.

A transient connection failure can retry the affected local file from the beginning for up to
`file_retry_timeout` (one hour by default). Permanent failures surface on the file's future.
A failed multipart upload waits for its outstanding PUTs before aborting the remote upload.

## Tags Note

Advanced Nominal power users will note that every file gets its own `nominal_ingest` tag when
ingested — this is what enables fast per-file deletes and group-bys over individual files. In
reality that tag is per-*ingest-job*, not per-file, so all files in one job share the same value.
If the previous behavior with per-file UUIDs was useful to you, opt in explicitly by passing your
own per-file tag on each `add_*` call, e.g. `tags={"FILE_UUID": str(uuid.uuid4())}`.

## Channel units and CSV rows

`add_csv`, `add_parquet`, `add_tabular_data`, and `add_avro_stream` accept `units={"pressure": "Pa"}`.
Units are passed to ingestion; values are not converted by the client.

On `add_csv`, `header_row`, `units_row`, and `data_row` are **one-based record
numbers**, all positive. The backend validates these values and their ordering
when the job is submitted. Blank lines are ignored and a quoted multiline record counts once.
The header defaults to record 1 and data defaults to the next record. A units
record must differ from the header and precede the first data record:

```python
builder.add_csv(
    "readings.csv", "time", "epoch_seconds",
    units_row=2, data_row=3,
    units={"pressure": "kPa"},
)
```

The units record needs a placeholder cell for the timestamp column. An empty
cell supplies no unit. The explicit map overrides the units record per channel.
`add_tabular_data` forwards to `add_csv` or `add_parquet` based on the file
extension, exposing only their shared options. Parquet archives accept unit
maps through this builder; the returned job tracks all extracted files.
