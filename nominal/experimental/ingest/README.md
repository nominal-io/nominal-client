# Performant Multi-File Ingest Job Machinery

> EXPERIMENTAL / UNSTABLE — backed by an in-development backend service whose contract may
> change without notice.

Traditionally, files are ingested into Nominal as individual, atomic units.
Each file gets its own per-file ingest tag and shows up in the files page of its dataset.
All of the typical client methods work this way today (with one notable exception, `add_containerized`):

```python
dataset = client.get_dataset(...)
dataset_file1 = dataset.add_tabular(...)
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
from datetime import datetime, timezone

from nominal.core import NominalClient
from nominal.experimental.ingest import IngestBuilder

client = NominalClient.from_profile("default")
dataset = client.get_dataset("rid...")

# tags here are applied as defaults to every file, overrideable per file
builder = IngestBuilder(client, dataset, tags={"aircraft": "test-tail-1"})

# add tabular files (csv or parquet, inferred from the extension)
builder.add_tabular_data(
    "test.parquet",
    timestamp_column="time",
    timestamp_type="epoch_seconds",
    tags={"subsystem": "nav"},
)
builder.add_tabular_data(
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
    start=datetime(2026, 7, 26, 6, 0, tzinfo=timezone.utc),
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

## New Upload Machinery

Under the hood, `submit()` uploads through a new experimental uploader (`MultipartUploader`, also
exported from this package) that runs every file concurrently: small files take a one-request
route, large files fan multipart parts out across a pool of direct-to-storage streams, and every
Nominal API request passes through a fixed-width admission lane with retry and backoff handling
tuned to the backend's throttling behavior.

The uploader also rides out network weather on its own: transient failures (dropped connections,
timeouts, throttling) retry each affected file on a backoff for up to an hour by default (the
`file_retry_timeout` knob), so a wifi blip mid-batch pauses the upload rather than killing it.
Permanent failures — a broken file, a rejected request — still surface immediately.

A typical experience on a strong network:

- ~15 Mbps for many tiny (4 KiB) files — bound by per-file request rate limits, not bandwidth.
- 400–500 Mbps for large (256 MiB+) files — bound by how quickly 64 MB parts stream to the
  storage layer.

Previously, the meta was uploading files sequentially or spinning up a threadpool over
`Dataset.add_*` calls. The former suffers poor parallelism (especially under retries, or when
files are a single part); the latter thrashes the system with too many threads and slows down.

## Tags Note

Advanced Nominal power users will note that every file gets its own `nominal_ingest` tag when
ingested — this is what enables fast per-file deletes and group-bys over individual files. In
reality that tag is per-*ingest-job*, not per-file, so all files in one job share the same value.
If the previous behavior with per-file UUIDs was useful to you, opt in explicitly by passing your
own per-file tag on each `add_*` call, e.g. `tags={"FILE_UUID": str(uuid.uuid4())}`.
