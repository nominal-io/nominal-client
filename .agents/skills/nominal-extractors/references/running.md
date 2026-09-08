# Running extractors and debugging ingest jobs

## Triggering an ingest

```python
dataset = client.get_dataset("ri.catalog.gov-staging.dataset.abc123")

job = dataset.add_containerized(
    extractor,                                # ContainerizedExtractor or its rid
    sources={"RAW_FILE": "captures/flight_042.bin"},
    arguments={"THRESHOLD": "0.8"},
    tags={"vehicle": "N042", "campaign": "sept-flight-test"},
)
```

- **`sources`** — maps each input's *environment variable* (as registered on the active
  image) to a local file path. The SDK uploads each file, then triggers the ingest. Keys
  must match the active image's registered inputs; missing a `required` input raises
  before anything is uploaded. The extractor must have an active image.
- **`arguments`** — parameter values, keyed by the parameter's environment variable.
  Values are strings (that's how they arrive in the container).
- **`tags`** — applied to all data ingested from this run; also exposed to the container
  as `ctx.additional_tags`. Use tags to partition recurring uploads within one dataset
  (per run, per vehicle) instead of creating a dataset per file.
- **`timestamp_column=` / `timestamp_type=`** (together) — override the image's default
  timestamp metadata for this ingest, applied uniformly to all output files. Per-output
  metadata declared in a manifest still wins over this override.

Containerized ingest is asynchronous and can emit many files, so the call returns an
`IngestionJob` immediately.

## Tracking the job

Waiting for a containerized ingest has **two stages**, and conflating them is the most
common mistake here. First the container has to finish producing files; only then do those
files exist to be waited on, and they ingest asynchronously in turn.

```python
import time

from nominal.core import IngestionJobStatus, wait_for_files_to_ingest

RUNNING = (IngestionJobStatus.SUBMITTED, IngestionJobStatus.QUEUED, IngestionJobStatus.IN_PROGRESS)

# 1. the container run
deadline = time.monotonic() + 3600
while job.refresh().status in RUNNING:
    if time.monotonic() > deadline:
        raise TimeoutError(f"extraction still {job.status.name} — see {job.nominal_url}")
    time.sleep(2)
if job.status is not IngestionJobStatus.COMPLETED:
    raise RuntimeError(f"extraction {job.status.name} — see {job.nominal_url}")

# 2. the files it produced
done, still_ingesting = wait_for_files_to_ingest(job.dataset_files())  # timeout=, return_when= available
```

Why stage 1 can't be skipped: `job.dataset_files()` returns the files that exist *at the
moment of the call*, and `job.as_files_ingested()` calls it exactly once. Run either right
after `add_containerized`, before the container has produced anything, and it sees an empty
list — so `list(job.as_files_ingested())` returns `[]` immediately, having waited for
nothing. It looks like a successful wait over zero outputs. Once the job is `COMPLETED` the
file list is complete, and `as_files_ingested()` is then a fine substitute for stage 2.

Why the loop tests the *running* set rather than a terminal set: `IngestionJobStatus` has a
seventh member, `UNKNOWN`, which this client maps any status a newer server introduces into.
Waiting *while* the status is known-running exits on anything unrecognized, and the
`COMPLETED` check then turns it into a loud failure. Waiting *until* the status is one of a
hard-coded terminal set does the opposite — an unrecognized status is never terminal, so a
client one release behind its server spins forever, which is the same class of bug as the
snapshot above. The deadline covers the remaining case, a server that adds a non-terminal
status; pick a bound that suits your extractor's real runtime.

The rest of the handle:

```python
job.refresh()       # re-reads from the server; status is a snapshot without it
job.status          # SUBMITTED → QUEUED → IN_PROGRESS → COMPLETED | FAILED | CANCELLED
job.dataset_files() # the DatasetFiles produced as of this call
job.produced_file_count
job.cancel()        # stop a job that is still running
job.nominal_url     # link to the job's page in the Nominal app
```

## Debugging a failed job

Work from the outside in:

1. **Open `job.nominal_url`** — the job page in the Nominal app shows status, produced
   files, and the container's captured stdout/stderr. The extractor runtime logs its
   startup line (mode, function name, input count), contract warnings, and the final
   traceback there.
2. **Read the runtime's advisory warnings** near the top of the log:
   - "required parameter ... has no value set" — the ingest request omitted an argument
     the image registered as required.
   - "input ... is not present at ..." — a registered input wasn't mounted (usually an
     optional input the request didn't supply).
   - "output directory contains file(s) not passed to ..." (at the end) — the code wrote
     files it never declared; they were not ingested.
3. **Common failure signatures**:
   - `@manifest_extractor disagrees with the image's registered output format ...` — the
     code and the registration disagree; re-register or switch decorators.
   - `exec format error` / container exits instantly with no Python traceback — the image
     was built for the wrong architecture; rebuild with `--platform linux/amd64`.
   - `ModuleNotFoundError` — a dependency missing from the image; it must be installed in
     the Dockerfile, not assumed from your dev machine.
   - Job `COMPLETED` but no data visible — reason about it from what `COMPLETED` actually
     proves: the container exited 0, which means the runtime finalized *at least one*
     declared output. A run that declared nothing at all cannot land here, because
     `_finalize` raises and the job shows `FAILED`. So the live possibilities are: the code
     declared some file but not the one carrying the data (the stray-file warning names it);
     the declared file is genuinely empty because a parse failure was swallowed by a broad
     `except`; the timestamp column or unit is wrong, so the data landed at a time nobody is
     looking at; tags or a channel prefix are filtering it out of view; or the file is still
     ingesting — job status and each file's own `ingest_status` are separate, so check
     `job.dataset_files()` before concluding data is missing. If the entrypoint is
     hand-rolled rather than using this runtime, none of the declaration guarantees hold and
     that is the first thing to check.
   - Ingestion fails before the container ran, complaining about timestamp metadata — the
     image was registered without a default (older registration path) and the request
     supplied no override.
4. **Reproduce locally** — the same code path runs with
   `my_extractor.run(env={...}, exit=False)` (see `authoring.md`, Local testing), or under
   Docker with the input mounted. Local reproduction is almost always faster than
   iterating through platform ingests.

## Fitting into recurring workflows

Once the extractor is registered, ingest triggering is the only per-file step. Typical
patterns:

- **Scripted batch**: loop `add_containerized` over every file and collect the jobs *before*
  waiting on any of them — they run server-side and in parallel, so waiting inside the loop
  serializes work that didn't need to be. Then apply the two-stage wait per job: poll each to
  a terminal status, then `wait_for_files_to_ingest` over the files they produced. Waiting on
  a batch is where skipping stage 1 hurts most, since an empty snapshot per job makes the
  whole batch look instantly finished.
- **Operator self-serve**: users upload raw files through the Nominal web app and pick the
  extractor; no SDK involved. This is the main reason to prefer an extractor over local
  conversion.
- **Version upgrades**: register the new image tag and `set_active_image` — in-flight jobs
  finish on the image they started with; new ingests use the new image.
