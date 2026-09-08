---
name: nominal-extractors
description: Build, test, register, and run Nominal containerized extractors — custom Docker images the Nominal platform runs during ingest to parse proprietary or unsupported file formats into datasets. Use this whenever the user mentions containerized extractors, custom extractors, nominal.experimental.extractor, @single_file_extractor / @manifest_extractor, register_image, add_containerized, extractor manifests, or asks how to ingest a file format Nominal doesn't natively support (binary telemetry, vendor logger output, packed/proprietary formats), how to convert files server-side during ingest, or how to debug a containerized ingest job.
---

# Nominal Containerized Extractors

A containerized extractor is a Docker image that Nominal runs server-side during ingest.
The platform mounts the uploaded raw file(s) into the container, runs your code, and
ingests whatever your code writes to the output directory into the target dataset. Once
registered, anyone in the workspace can upload raw files in that format — from the SDK or
the Nominal web app — without having the parsing code on their machine.

## Where extractors fit in Nominal's data model

Nominal organizes time-series data into **datasets**. A dataset is built up from ingested
**files**: each ingest parses a file's columns/records into **channels** (series), using
timestamp metadata to place samples in time, with optional **tags** partitioning data
within the dataset (e.g. per test-run or per vehicle).

The SDK's `Dataset` class has native ingest methods for formats the platform parses
itself: `add_tabular_data` (CSV/Parquet), `add_avro_stream`, `add_journal_json` (logs),
`add_mcap`, `add_video`, `add_mcap_video`, `add_ardupilot_dataflash`.

A containerized extractor extends that same ingest pipeline with your code. Use one when:

- the raw format is proprietary or unsupported (binary telemetry, packed structs, vendor
  logger output, custom CSV dialects needing preprocessing);
- the format recurs — many files, many uploaders — so parsing logic should live in one
  versioned, centrally-run place rather than on each engineer's laptop;
- non-developers (operators, technicians) upload files through the web app and the
  conversion must happen automatically.

For a one-off conversion where you already have a parsing script, skip the extractor:
convert locally and call `dataset.add_tabular_data(...)` directly.

Two SDK surfaces are involved, and it's important not to mix them up:

| Surface | Runs where | Import |
|---|---|---|
| Authoring runtime (the extractor's own code) | Inside the container, during ingest | `nominal.experimental.extractor` |
| Management & triggering (create/register/activate/run) | On your machine | `nominal.core` via `NominalClient` |

One more distinction: a **ContainerizedExtractor** carries identity (name, description);
the **ContainerImage**s registered against it carry the execution contract (inputs,
parameters, output format, default timestamp metadata). Exactly one image is active at a
time, so you can register a new image version and switch over atomically.

## Lifecycle

1. **Decide the shape** — which values are inputs vs parameters, how timestamps are
   encoded, what the tags are — `references/modeling.md`
2. **Author** the extractor function — `references/authoring.md`
3. **Test locally** with `my_extractor.run(env={...}, exit=False)` — no Docker or platform
   access needed — `references/authoring.md` (Local testing section)
4. **Build** the image for `linux/amd64` and `docker save` it to a tarball —
   `references/registration.md`
5. **Register and activate** the image against an extractor — `references/registration.md`
6. **Trigger ingests** with `Dataset.add_containerized` and track the `IngestionJob` —
   `references/running.md`

Read the reference file for whichever stage you're working on before writing code — each
stage has contract details (exact env variables, timestamp resolution rules, format
restrictions) that are easy to get subtly wrong from memory.

Step 1 is the one people skip, and it's the one that's expensive to undo: inputs and
parameters are fixed at registration, and timestamp and tag choices shape every query
written against the data afterward. If the extractor's shape isn't already settled, read
`modeling.md` before writing code.

## The output contract: write manifest extractors

**New extractors use `@manifest_extractor`, registered with `output_format=MANIFEST`.**
That is the current contract and a strict superset of the alternative: it describes every
output file individually, so one image can emit several files, mix telemetry with logs and
video, and give each file its own timestamps, tag columns, and channel prefix.

`@single_file_extractor` is the original contract, still supported for images already
registered with `PARQUET`, `CSV`, or `AVRO_STREAM`. You need it when maintaining one of
those; don't reach for it for new work, even when the extractor happens to produce a single
table today. It costs nothing to declare one file through the manifest contract, and the
choice is not cheap to reverse — the output format is fixed at registration, so changing it
later means registering and activating a new image.

| | `@manifest_extractor` (use this) | `@single_file_extractor` (existing images) |
|---|---|---|
| Registered `output_format` | `MANIFEST` | `PARQUET`, `CSV`, or `AVRO_STREAM` |
| Outputs | Any number, mixed formats | Exactly one file |
| Declare with | `ctx.add_tabular` / `add_avro_stream` / `add_journal_json` / `add_video` | `ctx.set_output(path)` |
| Per-output tag columns, channel prefixes, timestamps | Yes | No |
| Video outputs | Yes (recent platform versions) | No |

Whichever you use, the decorator and the registered format must agree — `Extractor.run`
fails at startup if they disagree, rather than emitting output the pipeline rejects.

## Minimal end-to-end example

Extractor code (`extract.py`, runs inside the container):

```python
import pandas as pd

from nominal.experimental.extractor import ManifestExtractorContext, manifest_extractor


@manifest_extractor
def convert(ctx: ManifestExtractorContext) -> None:
    df = parse_proprietary(ctx.input("RAW_FILE"))       # your parsing logic
    threshold = float(ctx.get_param("THRESHOLD", "0.5"))  # params are always strings

    out = ctx.output_dir / "telemetry.parquet"
    df[df.quality > threshold].to_parquet(out)
    ctx.add_tabular(out, timestamp_column="time_s", timestamp_type="epoch_seconds")


if __name__ == "__main__":
    convert.run()
```

Dockerfile:

```dockerfile
FROM python:3.12-slim
RUN pip install --no-cache-dir nominal pandas pyarrow
COPY extract.py /app/extract.py
ENTRYPOINT ["python", "/app/extract.py"]
```

Build, save, register, activate, run (on your machine):

```python
from nominal.core import NominalClient
from nominal.core.container_image import FileExtractionInput, FileExtractionParameter, FileOutputFormat

client = NominalClient.from_profile("default")

# docker build --platform linux/amd64 -t my-extractor:0.1.0 .
# docker save my-extractor:0.1.0 -o my-extractor-0.1.0.tar

extractor = client.create_containerized_extractor("My Format Converter")
image = extractor.register_image(
    "my-extractor-0.1.0.tar",
    tag="0.1.0",
    inputs=[
        FileExtractionInput(
            name="Raw file", environment_variable="RAW_FILE",
            file_suffixes=["bin"], required=True,
        )
    ],
    parameters=[
        FileExtractionParameter(name="Quality threshold", environment_variable="THRESHOLD"),
    ],
    output_format=FileOutputFormat.MANIFEST,
    default_timestamp_column="time_s",
    default_timestamp_type="epoch_seconds",
)
extractor.set_active_image(image)

dataset = client.get_dataset("ri.catalog....")
job = dataset.add_containerized(
    extractor,
    sources={"RAW_FILE": "flight_042.bin"},   # keyed by environment variable
    arguments={"THRESHOLD": "0.8"},           # values are strings
)
files = list(job.as_files_ingested())          # blocks until ingested
```

## Rules that trip people up

- **Outputs must be written under `ctx.output_dir` and declared.** The pipeline ingests
  only declared files; undeclared files earn a warning and are silently dropped.
- **`manifest.json` belongs to the runtime.** Never write it yourself — declare outputs
  through the `add_*` methods and the runtime writes it when your function returns.
- **Parameters arrive as strings.** Coerce them yourself (`int(ctx.param("PARTS"))`).
- **The decorator must match the registered output format.** A mismatch fails at container
  startup with a clear error (better than emitting output the pipeline rejects).
- **Build for `linux/amd64`.** Nominal runs images on amd64; on Apple Silicon always pass
  `--platform linux/amd64` to `docker build`.
- **Image tags are immutable.** Re-registering an existing tag raises
  `NominalAlreadyExistsError` — bump the tag instead.
- **Registration does not activate.** A new image runs only after
  `extractor.set_active_image(image)`.
- **Timestamp metadata resolves in order:** per-output manifest metadata → the ingest
  request's `timestamp_column`/`timestamp_type` override → the image's registered default.
  Registration requires a default; per-output metadata supports only numeric types
  (epoch/relative, seconds→nanoseconds) — richer formats (ISO 8601, custom) must come from
  the job-level metadata.
- **Failures should fail loudly.** Any exception escaping your function exits non-zero and
  fails the ingest job — that's the designed behavior. Don't swallow errors into empty
  outputs.
- **`required=True` means far less on a parameter than on an input.** A missing required
  input raises in `add_containerized` before anything uploads; a missing required parameter
  is only warned about at container start, then fails mid-run when `ctx.param()` reads it.
- **Never register `Relative` as the image's default timestamp type.** The `start` would be
  baked in once and applied to every future ingest. Register an absolute default and
  declare `Relative` per output in the manifest.

## Reference files

- `references/modeling.md` — the decisions that outlive the code: inputs vs parameters,
  absolute vs relative time, and what to tag (plus tagging pitfalls). Read before writing
  code for a new extractor, or when reviewing one whose shape isn't settled.
- `references/authoring.md` — the full in-container runtime: context API, per-format
  declaration methods, system metadata, error semantics, and local testing patterns.
  Read before writing or reviewing extractor code.
- `references/registration.md` — Dockerfile conventions, building/saving the image,
  `register_image` arguments, image lifecycle (statuses, activation, deletion, search).
  Read before registering or upgrading an image.
- `references/running.md` — triggering ingests with `add_containerized`, tracking
  `IngestionJob`s, and debugging failed jobs. Read when running or troubleshooting.
