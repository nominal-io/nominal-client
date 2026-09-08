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

```python
# Block until every produced file finishes ingesting:
files = list(job.as_files_ingested())

# Or poll manually:
job.refresh()
job.status          # SUBMITTED → QUEUED → IN_PROGRESS → COMPLETED | FAILED | CANCELLED
job.dataset_files() # the DatasetFiles produced so far
job.cancel()
job.nominal_url     # link to the job's page in the Nominal app
```

For timeout control over the blocking wait, use
`nominal.core.wait_for_files_to_ingest(job.dataset_files(), ...)`.

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

- **Scripted batch**: loop `add_containerized` over files, collect jobs, then wait on all
  (`[list(j.as_files_ingested()) for j in jobs]`) — jobs run server-side, so trigger them
  all before waiting.
- **Operator self-serve**: users upload raw files through the Nominal web app and pick the
  extractor; no SDK involved. This is the main reason to prefer an extractor over local
  conversion.
- **Version upgrades**: register the new image tag and `set_active_image` — in-flight jobs
  finish on the image they started with; new ingests use the new image.
