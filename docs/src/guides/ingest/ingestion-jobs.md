---
myst:
  html_meta:
    description: "Upload many files as one ingest job, wait for its files to ingest and find failed jobs."
---

# Ingest jobs

{.lead}
An ingest job tracks a batch of files as one unit, with one status and one entry on the [Ingestion page](https://app.gov.nominal.io/ingestion).
You get one from {py:class}`~nominal.experimental.ingest.IngestBuilder` or from a [file extractor](/guides/extractors/overview.md).

To upload a single file, use the dataset's own methods, as in [Datasets](/guides/datasets/overview.md).

```{include} /guides/_snippets/connect-the-client.md
```

## Submit a batch

`IngestBuilder` uploads several files in parallel, then starts one job for them.
It's experimental, so the API may change between releases.

```python
from datetime import datetime, timezone

from nominal.experimental.ingest import IngestBuilder

dataset = client.get_dataset("<DATASET_RID>")
job = (
    IngestBuilder(client, dataset)
    .add_tabular_data(
        "flight_data.csv",
        timestamp_column="source_time",
        timestamp_type="iso_8601",
        units={"altitude": "m", "airspeed": "m/s"},
    )
    .add_mcap("recording.mcap", exclude_topics=["/diagnostics"])
    .add_video("nose_camera.mp4", "nose_camera", start=datetime(2026, 9, 10, 14, 38, 19, tzinfo=timezone.utc))
    .add_tags({"test_event": "ft-114"})  # applied to every file
    .submit()
)
```

The `add_*` methods match the [dataset upload methods](/guides/datasets/overview.md#file-formats).
`add_tabular_data` also takes `units`, `channel_prefix` and `channel_name_overrides`, to rename vendor columns as they ingest.

`submit` returns once the job starts, not when its files finish.
A builder submits once. Make a new one for the next batch.

- If an upload fails, `submit` raises `NominalIngestUploadFailed` and starts no job.
  `submit(allow_partial=True)` starts the job with the files that uploaded and logs the rest.
- `submit(runs_to_expand=["<RUN_RID>"])` widens those runs to cover the new data.
- Files in one job share a single `nominal_ingest` tag. To tell them apart later, pass your own `tags` to each `add_*` call.

If `submit` raises after sending its request, the job may still have started.
[Search for it](#find-jobs) before you retry, so you don't ingest the files twice.

(ingest-drive-revisions)=
## Ingest files already in a Drive

Pass a {py:class}`~nominal.experimental.drives.DriveFileRevision` to any `add_*` method
instead of a local path. Nominal reads that exact revision server-side, so the builder does
not download or re-upload its bytes. You can mix Drive revisions and local files in one job,
including named inputs to `add_containerized`.

```python
from nominal.experimental import drives
from nominal.experimental.ingest import IngestBuilder

drive = drives.get_drive(client, "<DRIVE_RID>")
file = drive.get_file("raw/readings.csv")
assert isinstance(file, drives.ManagedDriveFile)
revision = next(
    r for r in file.revisions()
    if r.rid == file.current_revision_rid
)

job = (
    IngestBuilder(client, dataset)
    .add_csv(
        revision,
        timestamp_column="time",
        timestamp_type="epoch_seconds",
    )
    .add_mcap("recording.mcap")
    .submit()
)
```

Choose the revision explicitly: subsequent changes to the Drive file do not change the
registered input. Format inference uses the revision's recorded path. A removal revision
has no content and is rejected when added. Unknown revision states are passed through for
the service to validate. The service also checks access and whether the revision's content
is still retained.

This input accepts managed file revisions; virtual Drive files and bare revision RIDs are
not accepted. Uploading to a Drive alone does not submit an ingest job.

With `allow_partial=True`, a local upload failure drops the item that depends on it;
Drive-only items remain eligible. Server-side ingestion errors are still reported by the
job. For Drive videos, `frame_timestamps` creates and uploads a timestamp manifest while
the video bytes stay in the Drive.

## Wait for the files

{py:meth}`~nominal.core.IngestionJob.as_files_ingested` yields each file as it finishes, including files the job produces while you wait:

```python
from datetime import timedelta

from nominal.core.dataset_file import IngestStatus

for file in job.as_files_ingested(timeout=timedelta(minutes=30)):
    if file.ingest_status is IngestStatus.SUCCESS:
        print(file.name, "ingested")
    else:
        print(file.name, file.ingest_status.name, file.get_ingest_error())
```

- A completed job can still have failed files, so check each file's status.
- A failed job raises `NominalIngestFailed`, after yielding the files that finished.
- A timeout raises `NominalIngestTimeout` but doesn't stop the job. {py:meth}`~nominal.core.IngestionJob.cancel` does, without removing data that already ingested.

To wait on files you already have, use {py:func}`~nominal.core.wait_for_files_to_ingest`.
It returns `(done, not_done)` and doesn't see files the job adds later.

## Find jobs

{py:meth}`~nominal.core.NominalClient.search_ingestion_jobs` filters by dataset, creator, status, text and start time:

```python
from datetime import datetime, timedelta, timezone

from nominal.core import IngestionJobStatus

failed = client.search_ingestion_jobs(
    statuses=[IngestionJobStatus.FAILED],
    start_time_after=datetime.now(timezone.utc) - timedelta(days=1),
)
for job in failed:
    print(job.ingest_type, job.origin_files, job.nominal_url)
```

A job's `status` doesn't change until you call `job.refresh()`.
