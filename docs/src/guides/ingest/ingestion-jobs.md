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

**It uploads much faster.** `submit()` runs every file concurrently: small files take a one-request
route, large files fan multipart parts out across direct-to-storage streams, and every API request
passes through an admission lane with retry and backoff tuned to the backend's throttling. On a
strong network that is roughly 400-500 Mbps for large files, and around 15 Mbps for many tiny ones
where the limit is per-file request rate rather than bandwidth. The alternatives it replaces are
uploading sequentially, which parallelises poorly, and throwing a thread pool at `Dataset.add_*`,
which thrashes and slows down.

The uploader also rides out network weather on its own. Transient failures such as dropped
connections, timeouts and throttling retry per file on a backoff for up to an hour, so a wifi blip
mid-batch pauses the upload rather than killing it. Permanent failures surface immediately.

:::{warning}

`IngestBuilder` is experimental and its API may change between releases. The `add_*` methods on
`Dataset` are the stable path for single files.
:::

### Build and submit

Add files with the `add_*` method for each format, then call `submit()`:

```{literalinclude} /guides/_snippets/code/sdk/python/ingest/ingest_builder.py
:language: python
```

The builder targets an existing dataset and cannot create one, so create or look up the dataset
first. A builder is also single-use: calling `submit()` a second time raises rather than
re-uploading and re-ingesting everything it holds.

The formats mirror the `Dataset` methods: `add_tabular_data`, `add_mcap`, `add_journal_json`,
`add_video`, `add_avro_stream`, `add_ardupilot_dataflash`, and `add_containerized` to run a file
extractor as part of the batch. Each takes the same per-format options as its `Dataset` counterpart,
plus a few the per-file methods do not have. `add_tags()` applies tags to everything in the job, on
top of any per-file tags.

Tabular files accept three options here that `Dataset.add_tabular_data()` does not: `units` maps
channel names to unit symbols at ingest rather than in a later pass, `channel_prefix` namespaces
every channel from that file, and `channel_name_overrides` renames columns on the way in, which is
how you reconcile a vendor's column names with the channel names the rest of your fleet uses.

### What is atomic, and what is not

The two stages behave differently, which is worth being precise about.

**Uploading is atomic by default.** `submit()` is fail-fast: the first file that fails permanently
cancels the batch, raises an exception group naming each failed file, and triggers no ingest at all.
Pass `allow_partial=True` to let every file run to settlement instead, logging and pruning the ones
that failed and triggering one job for whatever uploaded cleanly:

```python
job = builder.submit(allow_partial=True)
```

**Ingestion is per file.** Once the job is triggered, each file ingests independently, so the job
can complete with some of its files failed.

`runs_to_expand` widens the given runs' time bounds to cover the new data, which matters when the
run was created before the upload and would otherwise not show it:

```python
job = builder.submit(runs_to_expand=[flight_run])
```

### One ingest tag for the batch

Every ingested file normally carries its own `nominal_ingest` tag, which is what makes per-file
deletes and group-bys over individual files work. For files uploaded through a job that tag is
per-**job** rather than per-file, so every file in the batch shares one value.

If you rely on per-file values, pass your own tag on each `add_*` call:

```python
builder.add_tabular_data(path, timestamp_column="t", timestamp_type="iso_8601",
                         tags={"file_uuid": str(uuid.uuid4())})
```

## Waiting for a job

An ingest finishes in two stages: the job produces its files, then those files ingest.
`as_files_ingested()` covers both.

```{literalinclude} /guides/_snippets/code/sdk/python/ingest/ingestion_job_wait.py
:language: python
```

For extractors, the file list is re-read on every poll, so files that did not exist when you called
it are still picked up. A containerized extractor registers its outputs only after its container
exits, so `job.dataset_files()` is empty until then. It returns what exists at the moment you call
it, not what the job will eventually produce.

Each of the three ways an ingest can end changes how you write the calling code:

- **A job can complete with individual files failed.** Check `ingest_status` on each yielded file if
  that matters to you, which is why the example above prints it.
- **A failed *job* raises `NominalIngestFailed`.** Iterating rather than wrapping the call in
  `list()` keeps the files that were yielded before the failure.
- **A cancelled job yields whatever did ingest** and logs a warning, on the reasoning that a
  cancellation is the caller getting what they asked for.

Pass `timeout` to give up and raise `NominalIngestTimeout` instead of waiting indefinitely, and
`poll_interval` to change how often it checks.

`job.cancel()` stops a running job.

## Status

`IngestionJob.status` is one of `SUBMITTED`, `QUEUED`, `IN_PROGRESS`, `COMPLETED`, `FAILED`,
`CANCELLED`, or `UNKNOWN`.

The value is a **snapshot** taken when the job was fetched, not live. Polling it in a loop without
calling `job.refresh()` re-reads the same value forever.

Per-file status is separate and more granular: `DatasetFile.ingest_status` is one of `QUEUED`,
`PARSING`, `INGESTING`, `SUCCESS`, `FAILED`, `DELETION_IN_PROGRESS`, `DELETED`, or `UNKNOWN`. Use
`get_ingest_error()` on a failed file for the reason.

## Waiting on a fixed set of files

When you already hold the files and want `concurrent.futures.wait` semantics, use
`wait_for_files_to_ingest()`. It returns a `(done, not_done)` pair and takes `return_when` of
`ALL_COMPLETED` (the default), `FIRST_COMPLETED`, or `FIRST_EXCEPTION`:

```python
from nominal.core import wait_for_files_to_ingest

done, not_done = wait_for_files_to_ingest(job.dataset_files(), timeout=timedelta(minutes=5))
```

Unlike `job.as_files_ingested()`, this only ever reports per-file status. It never raises for a
job-level failure, because it does not know about the job.

## Finding jobs after the fact

`search_ingestion_jobs()` filters across the workspace by dataset, creator, status, free text, and
start-time window, which is the practical way to answer "what failed overnight":

```{literalinclude} /guides/_snippets/code/sdk/python/ingest/ingestion_job_search.py
:language: python
```

`IngestionJob.ingest_type` tells you what produced it: `TABULAR`, `MCAP`, `DATAFLASH`,
`JOURNAL_JSON`, `CONTAINERIZED`, `VIDEO`, `AVRO_STREAM`, `POINT_CLOUD`, or `MULTI` for a builder
submission. `origin_files` names the inputs, `produced_file_count` how many files came out, and
`nominal_url` links to the job on the [Ingestion page](https://app.gov.nominal.io/ingestion), which lists every
job with its status, file counts, destination, creator, start time and duration.
