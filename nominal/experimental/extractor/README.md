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

## Where this fits

Nominal stores time-series data in **datasets**, built from ingested **files**. Each ingest parses a
file's columns or records into **channels**, places samples in time using timestamp metadata, and
optionally applies **tags** that partition the data within the dataset (per test-run, per vehicle).
`Dataset` has a method per natively-parsed format — `add_tabular_data`, `add_avro_stream`,
`add_journal_json`, `add_mcap`, `add_video`, `add_mcap_video`, `add_ardupilot_dataflash`, and more;
see `Dataset` for the current set. A containerized extractor runs your code in that same pipeline and
produces the same channels, tags, and files.

Use one when:

- the format is proprietary or unsupported, and only your code reads it;
- it recurs, so the parsing logic should live in one versioned place instead of on each engineer's
  laptop;
- non-developers upload the files through the Nominal app, so the conversion has to happen without
  them running anything.

For a one-off conversion with a parser you already have, convert locally and call
`dataset.add_tabular_data(...)` instead. An extractor pays for its setup through repeat use.

## The contract

Nominal drives your container entirely through the environment:

| | |
|---|---|
| Input files | mounted under `/input`, and each input's path is also in the environment variable declared for it |
| Output directory | named by `$OUTPUT_DIR` |
| Parameters | environment variables; **always strings**, coerce them yourself |

Your job is to write files into `$OUTPUT_DIR` and say what they are. There are two output contracts,
each with its own decorator, and the one you pick must match the `output_format` you register the
image with — `Extractor.run` fails at startup if they disagree.

**Write new extractors as manifest extractors.** The manifest contract is the current one and a
strict superset: it describes each output file individually, so one image can emit several files, mix
telemetry with logs and video, and set per-file timestamps, tags, and channel prefixes. Single-file
extractors are the original contract, kept for images already registered that way. Changing an
image's output format later requires registering a new image.

## Manifest extractors

For images registered with `MANIFEST` — the contract to use. Declare as many files as you like, one
method per output format, each taking only the options that format uses.
`manifest.json` is written for you.

| Method | For | Options |
|---|---|---|
| `add_tabular` | `.csv` / `.parquet` (and `.gz`) | `tag_columns`, `channel_prefix`, `timestamp_column`/`timestamp_type` |
| `add_avro_stream` | `.avro` / `.avro.gz` | `channel_prefix`, `timestamp_type` |
| `add_journal_json` | `.jsonl` / `.jsonl.gz`, ingested as logs | `timestamp_column`/`timestamp_type` |
| `add_video` | any supported video container | `channel` (required), `start` or `frame_timestamps` |

The gaps are deliberate. Avro records carry their own channel, values, and tags, so there is nothing
to map — but their timestamps are bare numbers, so `add_avro_stream` still takes a `timestamp_type`
saying how to read them. Log samples carry no tags and all land on one channel, so tag columns and a
channel prefix would be silently dropped. Each method also checks the file extension its format
requires, so a mismatch fails at the call rather than server-side after upload.

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

`add_tabular` and `add_journal_json` take `timestamp_column` / `timestamp_type` together;
`add_avro_stream` takes the type alone, since the schema fixes which field holds the timestamps.

Declaring them here is the most specific end of a precedence chain that runs from one output file up
to the extractor's registered defaults. Use it when a single run emits files with different timestamp
fields or units. [Timestamps](#timestamps) lays out the whole chain and which type to emit.

## Single-file extractors

The original contract, for images registered with `PARQUET`, `CSV`, or `AVRO_STREAM`: the pipeline
ingests exactly one output file, parsed according to the registered format. Use it for images already
registered that way; write new extractors as manifest extractors.

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

A second `set_output` call raises, since the registered format describes one file. Everything below —
inputs, parameters, errors, building, registering, ingesting — applies to both contracts.

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

### When to use inputs vs parameters

Parameters are tuning knobs. Inputs are the data to extract from.

- **Input** — a file the extraction reads: the capture itself, plus any sidecar it needs (a
  calibration table, a channel map, a vendor schema).
- **Parameter** — a scalar that changes how the extraction runs: a threshold, a mode, a sample-rate
  divisor.

A value that never varies between ingests goes in the image, not in either mechanism.

Put structured configuration in an input, not a parameter. Parameter values are strings with no
schema, so a mapping, a list, or a nested document has to be encoded and parsed by hand.

**To accept a variable number of files, or a directory layout, register a `.zip` or `.tar` input.**
Each input is a single file, so archives are how you pass many files or preserve folder structure:
declare `file_suffixes=["zip"]` and unpack in the extractor. Use this for loggers that emit a session
directory rather than one capture, or for a run whose file count is not known in advance.

`required=True` is stronger on an input than on a parameter:

- a missing required **input** raises in `add_containerized`, before anything uploads;
- a missing required **parameter** is not checked client-side. The runtime warns at container start,
  then the run fails mid-job when `ctx.param()` reads it.

For a parameter the code cannot run without, read it at the top of your function or give it a default
with `ctx.get_param(name, default)`.

An input's `file_suffixes` also drive discovery: `search_containerized_extractors(file_extension=...)`
matches on them, so they determine which extractors a given file is offered for. The suffixes are
descriptive, not enforced locally — a `.txt` sent to an input registered `["json"]` still uploads.

## Timestamps

Nominal places every sample on an absolute timeline. Your output supplies a numeric or string
timestamp per sample; timestamp metadata says how to read it.

### Where metadata comes from, and what wins

Three levels can specify it. Per output file, the pipeline resolves them in this order:

| Precedence | Level | Set with | Scope |
|---|---|---|---|
| 1 (highest) | Per output | `ctx.add_tabular(..., timestamp_column=, timestamp_type=)` | one file in one run |
| 2 | Ingest request | `dataset.add_containerized(..., timestamp_column=, timestamp_type=)` | every output of one run |
| 3 (lowest) | Image default | `register_image(..., default_timestamp_column=, default_timestamp_type=)` | every run of that image |

Levels 2 and 3 are resolved **before the container runs**, and ingestion fails if both are absent.
That is why registration requires a default: it guarantees the job always has metadata to fall back
to. Level 1 is applied afterward, from the manifest your code writes, and overrides the resolved
job-level value for that file only.

Use each level for what it describes:

- **the image default** — the normal shape of this extractor's output. Set it at registration to the
  column and type your code emits on a typical run.
- **the ingest request** — facts about *this upload* that the image cannot know. Above all, this is
  where a relative t0 belongs: `timestamp_type=ts.Relative("milliseconds", start=t0)`.
- **per output** — when one run emits files with different timestamp fields or units. A manifest
  extractor writing telemetry in microseconds and events in seconds needs this; nothing else does.

Per-output metadata takes numeric types only — `ts.Epoch` or `ts.Relative`, in units of seconds
through nanoseconds. An output needing ISO 8601 or a custom string format must omit the per-output
pair and inherit the job-level value, which accepts the full range. For an avro output, inheriting is
correct only when the job-level type is numeric too: avro timestamps are integers, and a string
format cannot read them.

```python
# per output, in the manifest your extractor writes
ctx.add_tabular(part, timestamp_column="ts", timestamp_type="epoch_microseconds")
ctx.add_tabular(run, timestamp_column="elapsed", timestamp_type=ts.Relative("milliseconds", start=t0))

# avro takes the type alone: the schema fixes which field holds the timestamps
ctx.add_avro_stream(records, timestamp_type="epoch_microseconds")
```

### Prefer relative timestamps

Emit elapsed time from the start of the recording and supply t0 at ingest. Reach for absolute
(`ts.Epoch`, `ts.Iso8601`, `ts.Custom`) only when you can guarantee every timestamp is already
correct in absolute time.

Relative wins on two counts.

**It is recoverable.** With relative timestamps, absolute time is `(elapsed in the file) + (t0 in
metadata)`, so a wrong t0 is a metadata error: delete the file and re-ingest the same bytes with a
corrected `start`. With absolute timestamps, the times *are* the data, so the only fix is to download
the file, rewrite every value, and upload it again.

**It is a smaller problem to get right.** Relative asks one question per sample — how far into the
recording is this — which the logger measures directly off its own clock. Absolute asks every sample
to be correct in wall-clock time, which depends on GPS lock, NTP sync, and clock drift, any of which
can leave you with jumps, rounding, or an offset nobody notices until someone compares two sources.

So: if the source records only elapsed time, emit relative. If the source records absolute time but
you cannot vouch for it — unsynced machines, intermittent GPS, timestamps rounded to the second,
occasional jumps — convert to elapsed-from-first-sample and emit relative with a best-estimate t0.
Choosing absolute is a claim that no timestamp will ever need altering.

Do **not** register `ts.Relative` as an image's `default_timestamp_type`. A default is set once and
applies to every future ingest, so a fixed `start` gives every file the same t0. Register an absolute
default for the fallback and pass `Relative` per ingest, where each file carries its own start.

### Granularity

Use the unit the data has. Declaring `epoch_milliseconds` for microsecond data does not lose
precision, it misplaces every sample by a factor of 1000.

## Choosing tags

Tags separate otherwise-identical channels. A channel is identified by name, so two stands both
producing `chamber_pressure` blend into one series unless a tag distinguishes them. Blended data is
harder to detect and harder to undo than a failed ingest.

Tags reach the data three ways, in increasing specificity:

- **the ingest request** — `add_containerized(..., tags={"vehicle": "n1234"})`, applied to everything
  the run produces and readable in-container as `ctx.additional_tags`. Use it for facts about *this
  upload* that the file does not carry.
- **a tag column** — `ctx.add_tabular(path, tag_columns={"motor": "motor_id"})`, read per row, when
  the distinguishing fact varies *within* a file;
- **avro files** and **journal json**, which carry tags within the files themselves.

A good tag identifies a *source*, is stable for the life of the data, and has few distinct values:
vehicle, stand, motor serial, run identifier. A bad tag is a measurement (that is a channel), a value
derived from a timestamp (that is the timeline), or free text that varies by upload — `Stand A`,
`stand-a`, and `standA` become three unrelated series. Nothing normalizes tag values, so normalize
them in the extractor rather than relying on each caller.

`channel_prefix` and tags solve different problems. A prefix renames channels
(`engine/chamber_pressure`); a tag leaves the name alone and adds a dimension you can filter and group
by. Prefer a tag when comparing the same measurement across sources.

## Errors

Rejections come back as `ExtractorError` when they are about the extractor's own contract — a
reserved file name, an output outside `$OUTPUT_DIR`, no outputs declared, a timestamp type the
manifest cannot express — and as the ordinary argument errors the rest of the client raises
(`ValueError` subclasses) when the arguments themselves are malformed, including a file extension the
declared format cannot read, or half of a `timestamp_column` / `timestamp_type` pair.

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

`env` *replaces* the environment, it does not merge into it, so it must carry everything your code
reads: `OUTPUT_DIR`, the inputs, every parameter. Nothing falls back to the ambient value — omitting
`OUTPUT_DIR` fails with `ExtractorError`, raised to the caller under `exit=False` as above, or printed
as a traceback before a non-zero exit under `run()`'s default `exit=True`.

Then save a tarball for upload:

```bash
docker build --platform linux/amd64 -t my-extractor:v1 .
docker save my-extractor:v1 -o my-extractor-v1.tar
```

Nominal runs extractor images on amd64, so `--platform linux/amd64` is required. Registration does
not check it: an arm64 build (the default on Apple Silicon) registers and activates, then fails at
ingest with an exec format error.

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

- **`output_format` must match your decorator, and it defaults to `PARQUET`.** `MANIFEST` for
  `@manifest_extractor`, one of `PARQUET` / `CSV` / `AVRO_STREAM` for `@single_file_extractor`. A
  manifest extractor must pass it explicitly: omit it and you register the single-file contract, after
  which every run fails at container startup.
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
