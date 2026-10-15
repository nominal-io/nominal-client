---
myst:
  html_meta:
    description: "Track batched uploads as a unit, and build batches yourself with IngestBuilder"
---

# Ingest jobs

{.lead}
Track batched uploads as a unit, and build batches yourself with IngestBuilder

Uploading data to Nominal is asynchronous. The single-file methods (`Dataset.add_tabular_data()`,
`add_mcap()` and the rest) ingest each file as its own atomic unit with its own entry on the
dataset's files page, and return a `DatasetFile` you can poll.

An **ingest job** is a batch of files tracked as one thing. Two routes produce one:

- [**A file extractor**](/guides/extractors/overview.md). `Dataset.add_containerized()`
  returns a job because the extractor decides how many files it emits, and you find out only after
  its container exits.
- **`IngestBuilder`.** You accumulate the files yourself and submit them together.

Either way the result is an `IngestionJob`, and the rest of this page applies to both.

## Batching files yourself

`IngestBuilder` exists for two reasons.

**It keeps one logical artifact together.** A 250 GiB recording split into 1-2 GiB chunks for
practical handling, or a conversion script that turns one capture into a hundred Parquet files, is
otherwise a hundred loose files that happen to share a dataset. As a job they are one row on the
[Ingestion page](https://app.gov.nominal.io/ingestion) with one status and one pair of input and output file
counts, rather than a files page you scroll to count how many are done, ingesting, or still
enqueued.

**It uploads files concurrently.** `submit()` uploads the registered files before triggering
one ingest job. The uploader retries transient failures such as network interruptions and
throttling. Throughput depends on file sizes, network capacity, and service limits.

:::{warning}

`IngestBuilder` is experimental and its API may change between releases. The `add_*` methods on
`Dataset` are the stable path for single files.
:::

### Build and submit

Add files with the `add_*` method for each format, then call `submit()`:

```python
from nominal.core import NominalClient
from nominal.experimental.ingest import IngestBuilder

client = NominalClient.from_profile("default")

dataset = client.get_dataset("<DATASET_RID>")

builder = (
    IngestBuilder(client, dataset)
    .add_tabular_data(
        "flight_data.csv",
        timestamp_column="source_time",
        timestamp_type="iso_8601",
        units={"altitude": "m", "airspeed": "m/s"},
        tag_columns={"vehicle_id": "veh_id"},
    )
    .add_mcap("recording.mcap", exclude_topics=["/diagnostics"])
    .add_journal_json("system.jsonl")
    .add_video("nose_camera.mp4", "nose_camera", start="2026-09-10T14:38:19Z")
    .add_tags({"test_event": "ft-114"})
)

# Uploads the files, then submits one asynchronous ingest job
job = builder.submit()

print(f"submitted {job.rid} ({job.ingest_type})")
```

The builder targets an existing dataset and cannot create one, so create or look up the dataset
first. A builder is single-use: after a submission attempt, create a new builder for another
batch. A failed request to trigger ingestion may already have reached the server; check for an
existing job before retrying the batch, or you may ingest it twice. See
[Finding jobs after the fact](#finding-jobs-after-the-fact): filter by dataset and a recent
`start_time_after` to look for a submission whose response was lost.

Returning from `submit()` means the job was submitted, not that its files finished ingesting.
Use [Waiting for a job](#waiting-for-a-job) below to track completion.

The formats mirror the `Dataset` methods: `add_tabular_data`, `add_mcap`, `add_journal_json`,
`add_video`, `add_avro_stream`, `add_ardupilot_dataflash`, and `add_containerized` to run a file
extractor as part of the batch. Each takes the same per-format options as its `Dataset` counterpart,
plus a few the per-file methods do not have. `add_tags()` applies tags to everything in the job, on
top of any per-file tags.

Tabular files accept three options here that `Dataset.add_tabular_data()` does not: `units` maps
channel names to unit symbols at ingest rather than in a later pass, `channel_prefix` namespaces
every channel from that file, and `channel_name_overrides` renames columns on the way in, which is
how you reconcile a vendor's column names with the channel names the rest of your fleet uses.

### Upload failures and partial batches

By default, an upload failure stops submission, raises `NominalIngestUploadFailed`, and triggers
no ingest job. Files already uploaded are not evidence that ingestion started.

To submit the items that uploaded successfully, replace the original `submit()` call with:

```python
job = builder.submit(allow_partial=True)
```

This waits for all uploads to settle and logs the failures. An item is dropped if **any** of its
files failed to upload, even if its other files uploaded successfully. If no items remain,
submission raises `NominalIngestUploadFailed`. The returned job covers only the retained items;
inspect the upload logs to see what was omitted.

Once submitted, each output file ingests independently. A job can reach `COMPLETED` even when
some of its files failed; job status alone does not establish that all data is available.

### Expand a run's bounds

To widen existing runs' time bounds upon successful ingest, replace the original submission with:

```python
job = builder.submit(runs_to_expand=["<RUN_RID>"])
```

This only expands bounds; it never contracts them. You can combine this option with
`allow_partial=True`. Choose the options before submitting: these are alternative submissions,
not additional calls on the already-consumed builder.

### One ingest tag for the batch

Every ingested file normally carries its own `nominal_ingest` tag, which is what makes per-file
deletes and group-bys over individual files work. For files uploaded through a job that tag is
per-**job** rather than per-file, so every file in the batch shares one value.

If you rely on per-file values, supply your own tag while building the batch, **before** calling
`submit()`. For example, replace the earlier tabular `add_*` call with:

```python
from uuid import uuid4

builder.add_tabular_data(
    "flight_data.csv",
    timestamp_column="source_time",
    timestamp_type="iso_8601",
    units={"altitude": "m", "airspeed": "m/s"},
    tag_columns={"vehicle_id": "veh_id"},
    tags={"file_uuid": str(uuid4())},
)
```

## Waiting for a job

An ingest finishes in two stages: the job produces its files, then those files ingest.
`as_files_ingested()` covers both.

```python
from datetime import timedelta

from nominal.core.dataset_file import IngestStatus
from nominal.exceptions import NominalError

observed_files = []
try:
    for dataset_file in job.as_files_ingested(timeout=timedelta(minutes=30)):
        observed_files.append(dataset_file)
        status = dataset_file.ingest_status
        if status is IngestStatus.SUCCESS:
            print(f"{dataset_file.name}: SUCCESS")
        elif status is IngestStatus.FAILED:
            print(f"{dataset_file.name} failed: {dataset_file.get_ingest_error()}")
        else:
            print(f"{dataset_file.name}: success not confirmed (last status {status.name})")
except NominalError:
    print(f"Stopped waiting after observing {len(observed_files)} files")
    raise
```

The iterator discovers newly produced files while the job runs, so files that did not exist when
you started waiting are still picked up. A containerized extractor registers its outputs only after its container
exits, so `job.dataset_files()` is empty until then. It returns what exists at the moment you call
it, not what the job will eventually produce.

A yielded file is not necessarily a successful ingest. A file missing from a polling response
can be yielded with its last known status, and deleted or unrecognized states can also be
yielded. The example reports success only for `SUCCESS`.

Job and file outcomes need separate handling:

- **A job can complete with individual files failed.** Check `ingest_status` on each yielded file if
  you need to establish success; the example above reports failed files without hiding them.
- **A failed *job* raises `NominalIngestFailed`.** Iterating rather than wrapping the call in
  `list()` keeps the files that were yielded before the failure.
- **A cancelled job yields whatever did ingest** and logs a warning, on the reasoning that a
  cancellation is the caller getting what they asked for.

Pass `timeout` to give up and raise `NominalIngestTimeout` instead of waiting indefinitely, and
`poll_interval` to change how often it checks.

A timeout stops waiting; it does not cancel ingestion. Call `job.cancel()` to request cancellation
of a pending or running job. Cancelling a job that is already terminal raises a conflict error;
cancellation does not roll back data already ingested.

## Status

`IngestionJob.status` is one of `SUBMITTED`, `QUEUED`, `IN_PROGRESS`, `COMPLETED`, `FAILED`,
`CANCELLED`, or `UNKNOWN`.

The value is a **snapshot** taken when the job was fetched, not live. Polling it in a loop without
calling `job.refresh()` re-reads the same value forever.

Per-file status is separate and more granular: `DatasetFile.ingest_status` is one of `QUEUED`,
`IN_PROGRESS`, `PARSING`, `INGESTING`, `SUCCESS`, `FAILED`, `DELETION_IN_PROGRESS`, `DELETED`, or `UNKNOWN`. Use
`get_ingest_error()` on a failed file for the reason.

## Waiting on a fixed set of files

When you already hold the files and want `concurrent.futures.wait` semantics, use
`wait_for_files_to_ingest()`. It returns a `(done, not_done)` pair and takes `return_when` of
`ALL_COMPLETED` (the default), `FIRST_COMPLETED`, or `FIRST_EXCEPTION`:

```python
from datetime import timedelta

from nominal.core import wait_for_files_to_ingest

# Deliberately wait only for the files currently visible, not the entire job.
files = job.dataset_files()
done, not_done = wait_for_files_to_ingest(files, timeout=timedelta(minutes=5))
print(f"{len(done)} finished; {len(not_done)} still pending")
```

Unlike `job.as_files_ingested()`, this only ever reports per-file status. It never raises for a
job-level failure, because it does not know about the job. `done` includes failed files; inspect
their status. When the timeout expires, pending files are returned in `not_done` instead of
raising `NominalIngestTimeout`.

A running job may produce more files after this snapshot, including when `files` is empty.
Use `job.as_files_ingested()` when you need to wait for the job and its future outputs.

## Finding jobs after the fact

`search_ingestion_jobs()` filters across the workspace by dataset, creator, status, free text, and
start-time window, which is the practical way to answer "what failed overnight":

```python
from datetime import datetime, timedelta, timezone

from nominal.core import NominalClient
from nominal.core.ingestion_job import IngestionJobStatus

client = NominalClient.from_profile("default")

# Find what failed in the last day, across every dataset in the workspace
failures = client.search_ingestion_jobs(
    statuses=[IngestionJobStatus.FAILED],
    start_time_after=datetime.now(timezone.utc) - timedelta(days=1),
)

for job in failures:
    print(f"{job.rid}  {job.ingest_type}  {job.origin_files}")
    print(f"  {job.nominal_url}")

# `status` is a snapshot taken when the job was fetched. Call refresh() to
# advance it rather than re-reading the same value in a loop.
job = client.get_ingestion_job("<JOB_RID>")
print(job.refresh().status)
```

`IngestionJob.ingest_type` tells you what produced it: `TABULAR`, `MCAP`, `DATAFLASH`,
`JOURNAL_JSON`, `CONTAINERIZED`, `VIDEO`, `AVRO_STREAM`, `POINT_CLOUD`, or `MULTI` for a builder
submission. `origin_files` names the inputs, `produced_file_count` how many files came out, and
`nominal_url` links to the job on the [Ingestion page](https://app.gov.nominal.io/ingestion), which lists every
job with its status, file counts, destination, creator, start time and duration.
