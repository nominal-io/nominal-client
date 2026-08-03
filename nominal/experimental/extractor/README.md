# Containerized Extractors

> EXPERIMENTAL / UNSTABLE — the in-container contract is set by the ingest pipeline and may change
> without notice.

Nominal ingests CSV, Parquet, MCAP, and a handful of other formats natively. A *containerized
extractor* covers everything else: you write a Docker image that reads your proprietary format and
writes something Nominal understands, register it, and from then on ingest is a normal
`dataset.add_containerized(...)` call. Nominal runs your container, mounts the input files, and
ingests whatever your code declares.

This package is the **in-container runtime** — the part that runs inside your image. Registering the
image and triggering ingests happen from `nominal.core` and are covered below, because you cannot
usefully do one without the other.

## The contract

Nominal drives your container entirely through the environment:

| | |
|---|---|
| Input files | mounted under `/input`, and each input's path is also in the environment variable declared for it |
| Output directory | named by `$OUTPUT_DIR` |
| Parameters | environment variables; **always strings**, coerce them yourself |

Your job is to write files into `$OUTPUT_DIR` and say what they are. The two decorators correspond to
the two output contracts the pipeline supports, and the one you pick must match the `output_format`
you register the image with — `Extractor.run` fails at startup if they disagree.

## Single-file extractors

For images registered with `PARQUET`, `CSV`, or `AVRO_STREAM`. The pipeline ingests exactly one output
file, parsed according to the registered format.

```python
from nominal.experimental.extractor import SingleFileExtractorContext, single_file_extractor

@single_file_extractor
def convert(ctx: SingleFileExtractorContext) -> None:
    table = read_my_format(ctx.input())          # the sole mounted input
    out = ctx.output_dir / "converted.parquet"
    write_parquet(table, out)
    ctx.set_output(out)                          # declare it

if __name__ == "__main__":
    convert.run()                                # the container entrypoint
```

## Manifest extractors

For images registered with `MANIFEST`. Declare as many files as you like — one method per output
format, each exposing only the options that format actually uses. `manifest.json` is written for you.

| Method | For | Options |
|---|---|---|
| `add_tabular` | `.csv` / `.parquet` (and `.gz`) | `tag_columns`, `channel_prefix`, `timestamp_column`/`timestamp_type` |
| `add_avro_stream` | `.avro` / `.avro.gz` | `channel_prefix` |
| `add_journal_json` | `.jsonl` / `.jsonl.gz`, ingested as logs | `timestamp_column`/`timestamp_type` |
| `add_video` | any supported video container | `channel` (required), `start` or `frame_timestamps` |

The gaps are deliberate. Avro records carry their own channel, timestamp, value, and tags, so there is
nothing to map. Log samples carry no tags and all land on one channel, so tag columns and a channel
prefix would be silently dropped. Each method also checks the file extension its format requires, so
a mismatch fails at the call rather than server-side after upload.

```python
from nominal.experimental.extractor import ManifestExtractorContext, manifest_extractor

@manifest_extractor
def split(ctx: ManifestExtractorContext) -> None:
    recording = read_my_format(ctx.input("RECORDING"))

    for i, chunk in enumerate(chunks_of(recording.telemetry, int(ctx.get_param("PARTS", "2")))):
        part = ctx.output_dir / f"part_{i}.parquet"
        write_parquet(chunk, part)
        ctx.add_tabular(part, tag_columns={"vehicle": "veh_id"})

    events = ctx.output_dir / "events.jsonl"
    write_jsonl(recording.events, events)          # each line needs a MESSAGE field
    ctx.add_journal_json(events, timestamp_column="ts", timestamp_type="epoch_microseconds")

    footage = ctx.output_dir / "front.mp4"
    recording.camera.write_h264(footage)
    ctx.add_video(footage, channel="camera/front", start=recording.started_at)

if __name__ == "__main__":
    split.run()
```

A manifest extractor may emit telemetry, videos, or only videos.

### Videos

`add_video` needs a `channel` — the video becomes a channel on the dataset, alongside the telemetry —
and exactly one of two ways to establish absolute time:

```python
# the video's own presentation timestamps, offset from an absolute start
ctx.add_video(footage, channel="camera/front", start="2026-07-31T12:00:00Z")

# ...optionally scaled, when the media plays at a different rate than the camera recorded at
ctx.add_video(footage, channel="camera/front", start=started_at, true_frame_rate=59.94)

# or one absolute nanosecond timestamp per frame, when you have precise per-frame metadata
ctx.add_video(footage, channel="camera/front", frame_timestamps=[1_753_000_000_000_000_000, ...])
```

`frame_timestamps` is the one declaration that *writes*: the runtime serializes the sidecar file the
platform expects (`front.mp4` gets `front.mp4.timestamps.json`) so you never reproduce that format by
hand.

Timestamps accept a `datetime`, an ISO 8601 string, or integer nanoseconds since the epoch.

> Video outputs require a recent version of the Nominal platform. An older ingest pipeline ignores
> them, and rejects a manifest whose only outputs are videos. The container is given an identical
> environment either way, so the runtime cannot detect which one is running it.

### Per-output timestamps

`add_tabular` and `add_journal_json` take `timestamp_column` / `timestamp_type` together to override
the job-level timestamp metadata for that file, so outputs of different shapes can carry different
timestamp fields:

```python
ctx.add_tabular(part, timestamp_column="ts", timestamp_type="epoch_microseconds")
ctx.add_tabular(run, timestamp_column="elapsed", timestamp_type=ts.Relative("milliseconds", start=t0))
```

Only numeric types work here — absolute epochs (`ts.Epoch`) or offsets from a start (`ts.Relative`).
Outputs needing ISO 8601 or custom string formats omit the pair and inherit the job-level metadata,
which supports the full range.

## Inputs and parameters

```python
ctx.input()                      # the sole mounted input; raises if there isn't exactly one
ctx.input("RECORDING")           # by registered name or environment variable
ctx.inputs                       # every mounted input, in the order Nominal serializes them

ctx.param("MODE")                # required; raises if unset
ctx.get_param("PARTS", "2")      # optional, with a default
int(ctx.get_param("PARTS", "2")) # values are always strings — coerce yourself
```

Newer pipelines also inject job context, all optional and `None`/empty on a local run:

```python
ctx.ingest_job_rid
ctx.dataset_rid
ctx.additional_tags            # tags the ingest request applies to all data from this run
ctx.job_timestamp_metadata     # what an output falls back to when it declares none of its own
```

## Errors

Rejections come back as `ExtractorError` when they are about the extractor's own contract — a
reserved file name, an output outside `$OUTPUT_DIR`, no outputs declared — and as the ordinary
argument errors the rest of the client raises (`ValueError` subclasses) when the arguments themselves
are malformed, including a file extension the declared format cannot read.

An undeclared file left in `$OUTPUT_DIR` is a warning, not a failure: the pipeline reads only what the
manifest names, so it will not be ingested, but a scratch file does not fail the run.

`run()` turns any failure into a non-zero exit so the ingest job fails cleanly. Pass `exit=False` to
re-raise instead, which is how you drive it in tests.

## Building the image

Your image needs the `nominal` package and an entrypoint that calls `.run()`. Format I/O — pyarrow,
ffmpeg, whatever your format needs — is your own dependency.

```dockerfile
FROM python:3.12-slim

RUN pip install --no-cache-dir nominal pyarrow
COPY extractor.py /app/extractor.py
WORKDIR /app

ENTRYPOINT ["python", "extractor.py"]
```

Testing it locally is just calling `run` with an environment, no Docker required:

```python
ctx = split.run(
    env={"OUTPUT_DIR": str(out_dir), "NOMINAL_EXTRACTOR_INPUT_DIR": str(in_dir)},
    exit=False,
)
print(ctx.build_manifest())
```

Then save a tarball for upload:

```bash
docker build -t my-extractor:v1 .
docker save my-extractor:v1 -o my-extractor-v1.tar
```

## Registering it

Three steps, all from `nominal.core`: create the extractor, register an image against it, activate
that image.

```python
from nominal.core import NominalClient
from nominal.core.container_image import (
    FileExtractionInput,
    FileExtractionParameter,
    FileOutputFormat,
)

client = NominalClient.from_profile("staging")

extractor = client.create_containerized_extractor(
    "flight-recorder",
    description="Splits .flight recordings into telemetry and camera channels",
)

image = extractor.register_image(
    "my-extractor-v1.tar",
    tag="v1",
    output_format=FileOutputFormat.MANIFEST,   # must match the decorator you used
    inputs=[
        FileExtractionInput(
            name="Recording",
            environment_variable="RECORDING",
            file_suffixes=["flight"],
            required=True,
        ),
    ],
    parameters=[
        FileExtractionParameter(name="Parts", environment_variable="PARTS"),
    ],
    default_timestamp_column="ts",
    default_timestamp_type="epoch_nanoseconds",
)

extractor.set_active_image(image)
```

Notes worth knowing before you hit them:

- **`output_format` must match your decorator.** `MANIFEST` for `@manifest_extractor`, one of
  `PARQUET` / `CSV` / `AVRO_STREAM` for `@single_file_extractor`. A mismatch fails at container
  startup rather than producing output the pipeline rejects.
- **Tags are immutable.** Re-registering an existing tag raises `NominalAlreadyExistsError`; bump the
  tag instead.
- **`default_timestamp_column` / `default_timestamp_type` are required** even if every ingest
  overrides them. They are the fallback encoding for anything ingested through this extractor.
- **Registering does not activate.** `set_active_image` decides which image runs.
- **`environment_variable` is how your code finds things.** It must match what `ctx.input(...)` and
  `ctx.param(...)` ask for.

## Ingesting with it

```python
dataset = client.create_dataset("Flight 42")

job = dataset.add_containerized(
    extractor,
    sources={"RECORDING": "flight-42.flight"},   # keyed by the input's environment variable
    arguments={"PARTS": "4"},                    # parameter values, as strings
    tags={"vehicle": "n1234"},
)

for file in job.as_files_ingested():             # blocks until each output finishes
    print(file.rid, file.ingest_status)
```

A containerized extraction is asynchronous and may produce many files, so it returns an
`IngestionJob` rather than a single file. Use `job.status` to poll, `job.dataset_files()` for what it
produced, and `job.cancel()` to stop it.

The keys of `sources` are the input environment variables you registered, and they must match the
active image's inputs exactly. `timestamp_column` / `timestamp_type` on this call override the image's
default for this ingest.

## Getting the container's logs

The extractor's stdout and stderr land in a log dataset in your workspace, capped at 1 MiB per job. The
runtime's own log lines go through the `nominal.experimental.extractor` logger; `run()` configures
logging when it is the entrypoint, so your own `logging` calls show up there too.
