---
myst:
  html_meta:
    description: "Write, register, run, and manage containerized file extractors with the Python client"
---

# File extractors in Python

{.lead}
Write, register, run, and manage containerized file extractors with the Python client

A file extractor is a Docker image Nominal runs during ingest. It mounts your uploaded files, runs
your code, and ingests whatever your code writes, which lets you support bespoke binary formats and
proprietary logs without leaving the normal upload flow. It also covers consistent post-processing
of formats Nominal already parses, such as rewriting the timestamp column of otherwise valid
Parquet.

An extractor earns its setup through repeat use: when the format recurs, when the parsing logic
should live in one versioned place instead of on each engineer's laptop, or when non-developers
upload the files through the Nominal app and the conversion has to happen without them running
anything. For a one-off conversion with a parser you already have, convert locally and call
`Dataset.add_tabular_data()` instead.

This page covers the whole lifecycle from Python: writing the container, registering and activating
its image, running an ingest, and managing extractors over time. For the language-agnostic
container contract, the output formats, and the Web UI and REST paths, see
[Ingest custom formats](https://docs.nominal.io/core/documentation/platform/data/containerized-extractors).

An extractor carries only identity, a name and description. Everything about *how* it executes,
including its inputs, parameters, output format, and default timestamp metadata, lives on its
registered images. A new extractor has no active image; activate one before ingesting.
Subsequent ingests use that active image.

:::{warning}

The authoring helpers in `nominal.experimental.extractor` are experimental. The container contract
they implement is stable, but the Python API may change between releases.
:::

## Write the extractor

The contract is environment-driven: input paths and parameters arrive as environment variables, and
output goes to the directory named by `$OUTPUT_DIR`. You can implement it in any language. In
Python, `nominal.experimental.extractor` implements the plumbing, so your code reads inputs and
declares outputs rather than parsing the environment and writing JSON.

### Which contract to write against

The output format you choose when registering the image fixes which of two contracts the pipeline
applies, and there is one decorator per contract.

| Registered output format | Decorator | Your container writes |
| --- | --- | --- |
| `MANIFEST` | `@manifest_extractor` | Any number of files, each declared individually |
| `PARQUET`, `CSV`, `AVRO_STREAM` | `@single_file_extractor` | Exactly one file, in that format |

**Write new extractors against the manifest contract.** It is a strict superset of the single-file
one: one image can emit several files, mix telemetry with logs and video, and set per-file
timestamps, tag columns, and channel prefixes. The single-file contract is the original one, kept
for images already registered against it.

Those four are the only registerable formats. The others on `FileOutputFormat` cannot be ingested,
so an image registered with one produces an extractor whose every ingest fails.

The decorator and the registered format must agree. If they do not (a `@single_file_extractor`
running under an image registered as `MANIFEST`, or the reverse), `run()` fails at startup with an
explicit error rather than producing an ingest that silently drops output.

:::{warning}

`register_image()` defaults `output_format` to `PARQUET`. The declarations below supply
`MANIFEST` through `extract.registration_kwargs()`. For manual registration, pass
`output_format=FileOutputFormat.MANIFEST` explicitly so registration matches the decorator.
:::

### The manifest contract

Save this template as `extractor_manifest.py`. Supply your format-specific parser as
`parse_bespoke_binary` in `bespoke_parser.py`; it is not part of the SDK. In this example,
it returns pandas dataframes `telemetry` (with epoch-microsecond `timestamp` and `veh_id`
columns) and `logs` (with `MESSAGE` and epoch-microsecond `__REALTIME_TIMESTAMP` columns),
plus `write_video(path)` and an absolute `started_at` timestamp for the video.

Declare files and settings with `@input` and `@parameter` beneath the outer
`@manifest_extractor`. The runner resolves them before calling your function. The
prefix defaults to `telemetry/` when omitted. The same declarations supply registration
metadata later in this guide.

Write your output files anywhere under `ctx.output_dir`, then declare each one with the method for
its format. `manifest.json` is written for you when your function returns.

```python
from pathlib import Path

from bespoke_parser import parse_bespoke_binary
from nominal.experimental.extractor import (
    ManifestExtractorContext,
    input,
    manifest_extractor,
    parameter,
)


@manifest_extractor(
    default_timestamp_column="timestamp",
    default_timestamp_type="epoch_microseconds",
)
@input("source", envvar="INPUT_FILE", name="Input file", file_suffixes=["bin"])
@parameter("prefix", envvar="CHANNEL_PREFIX", name="Channel prefix", default="telemetry/")
def extract(ctx: ManifestExtractorContext, *, source: Path, prefix: str) -> None:
    frames = parse_bespoke_binary(source)

    telemetry = ctx.output_dir / "flight_data.parquet"
    frames.telemetry.to_parquet(telemetry, index=False)
    ctx.add_tabular(
        telemetry,
        timestamp_column="timestamp",
        timestamp_type="epoch_microseconds",
        tag_columns={"vehicle_id": "veh_id"},
        channel_prefix=prefix,
    )

    logs = ctx.output_dir / "system_logs.jsonl"
    frames.logs.to_json(logs, orient="records", lines=True)
    ctx.add_journal_json(
        logs,
        timestamp_column="__REALTIME_TIMESTAMP",
        timestamp_type="epoch_microseconds",
    )

    # Video from the same run lands on a channel of the same dataset
    footage = ctx.output_dir / "nose_camera.mp4"
    frames.write_video(footage)
    ctx.add_video(footage, channel="nose_camera", start=frames.started_at)

    # manifest.json is written for you when this function returns


if __name__ == "__main__":
    extract.run()
```

The declaration methods each take only the options their format uses:

| Method | For | Notable options |
| --- | --- | --- |
| `ctx.add_tabular(path, ...)` | Parquet, CSV, and other tabular output | `timestamp_column`, `timestamp_type`, `tag_columns`, `channel_prefix` |
| `ctx.add_journal_json(path, ...)` | `journald` JSON Lines logs | `timestamp_column`, `timestamp_type` |
| `ctx.add_avro_stream(path, ...)` | [Avro stream files](/guides/datasets/avro-streams.md) | `channel_prefix`, `timestamp_type` |
| `ctx.add_video(path, channel=...)` | Video, as a channel on the dataset | `start`, `frame_timestamps`, `true_frame_rate`, `scale_factor` |

Omit `timestamp_column` and `timestamp_type` on a file to inherit the ingest's metadata, and set
them when one run emits files with different timestamp fields or units. See
[Timestamps](#timestamps) for what wins. `ctx.job_timestamp_metadata` exposes what was resolved, if
your code needs to branch on it.

### The single-file contract

Write one file and declare it with `set_output()`. The pipeline parses it according to the output
format the image was registered with. This alternative assumes a CSV with an
epoch-microsecond `timestamp` column. Save it separately as `extractor_single_file.py`;
the build and registration examples below continue to use the manifest template.

```python
from pathlib import Path

import pandas as pd
from nominal.core.container_image import FileOutputFormat
from nominal.experimental.extractor import (
    SingleFileExtractorContext,
    input,
    single_file_extractor,
)


@single_file_extractor(
    output_format=FileOutputFormat.PARQUET,
    default_timestamp_column="timestamp",
    default_timestamp_type="epoch_microseconds",
)
@input("source", envvar="INPUT_FILE", name="Input file", file_suffixes=["csv"])
def extract(ctx: SingleFileExtractorContext, *, source: Path) -> None:
    df = pd.read_csv(source)
    cleaned = df.dropna().reset_index(drop=True)

    output = ctx.output_dir / "processed_data.parquet"
    cleaned.to_parquet(output, index=False)

    # Declare the one file the pipeline should ingest. It is parsed according
    # to the output format the image was registered with.
    ctx.set_output(output)


if __name__ == "__main__":
    extract.run()
```

### Reading inputs and parameters

Use `@input` and `@parameter` for new extractors. `envvar=` names the runtime and
registration variable; `name=` is a display name. Converters and default values stay
in the Python runtime, while registration records names and requiredness.

Both contexts also retain legacy accessors; using them logs a migration warning and
does not contribute metadata to `registration_kwargs()`:

- **`ctx.input()`** returns the mounted path of the image's single declared input. When the image
  declares more than one, pass the input's environment variable name: `ctx.input("CAMERA_FILE")`.
  `ctx.inputs` is a list of mounted paths.
- **`ctx.param(name)`** returns a declared parameter's value and raises if it is missing.
  **`ctx.get_param(name, default)`** returns `None` or your default instead. Parameter values are
  always strings, so convert them yourself.
- **`ctx.additional_tags`** are the tags supplied for this ingest, and **`ctx.dataset_rid`** and
  **`ctx.ingest_job_rid`** identify what the run is writing into, which is useful in log output.

Which of the two a value belongs in: an **input** is a file the extraction reads, whether the
capture itself or a sidecar it needs such as a calibration table or channel map. A **parameter** is
a scalar that changes how the extraction runs, such as a threshold or a mode. A value that never
varies between ingests belongs in the image rather than either mechanism, and structured
configuration belongs in an input, since parameter values are unschema'd strings you would have to
encode and parse by hand.

Each input is a single file, so **register a `.zip` or `.tar` input to accept a variable number of
files or a directory layout**, and unpack it in the extractor. This is the approach for loggers that
emit a session directory rather than one capture.

`required=True` is enforced differently on each. A missing required input raises in
`add_containerized()` before anything uploads. A missing required parameter is not checked
client-side. With `@parameter`, a missing value without a default fails during binding,
before the parser callback runs. Legacy `ctx.param()` lookups fail when the value is read.

An input's `file_suffixes` also drive discovery, since
`search_containerized_extractors(file_extension=...)` matches on them. They are descriptive rather
than enforced locally, so a `.txt` sent to an input registered for `json` still uploads.

### Running it in the container

`run()` is the container entrypoint. It reads the environment, calls your function, finalizes the
output declaration, and on failure prints a traceback and exits non-zero so the ingest job fails:

```python
if __name__ == "__main__":
    extract.run()
```

In tests, call `extract.run(env={...}, exit=False)` to supply a fake environment and get the
exception re-raised instead of the process exiting.

Write logs to `stdout` and `stderr` with any logging library. Nominal collects them into a workspace
logs dataset for each ingest, capped at 1 MiB per job, and that dataset is the first place to look
when a run fails. `run()` configures logging when it is the entrypoint, so your own `logging` calls
land there alongside the runtime's.

## Build the image

Your image needs the `nominal` package, whatever reads your format, and an entrypoint that calls
`run()`. Install from the project's own dependency manifest rather than hand-listing packages in the
Dockerfile, so the image gets the versions the parser was tested against. For the
manifest template, include `nominal`, `pandas`, `pyarrow`, and your parser's dependencies
in `pyproject.toml` and generate its `uv.lock` before building:

```dockerfile
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.9.7 /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY extractor_manifest.py bespoke_parser.py ./
ENTRYPOINT ["/app/.venv/bin/python", "/app/extractor_manifest.py"]
```

Three runtime facts constrain the image, and none of them depend on how you build it:

- **The container runs as a fixed non-root user**, not whatever `USER` the image declares, with
  capabilities dropped. Nothing may require root at runtime.
- **`$HOME` is a writable empty mount** that shadows whatever the build left at that path. A
  dependency installed into the home directory, by `pip install --user` or into a `~/.venv`, is gone
  when the parser runs. It surfaces as `ModuleNotFoundError` for a package the image demonstrably
  contains. Install outside the home directory, as `/app/.venv` above does.
- **`$OUTPUT_DIR` is writable** by that user and needs no ownership or permission setup.

### Verify the architecture before registering

Nominal runs extractor images on `linux/amd64`, and `arm64` is the default on Apple Silicon. Read
the built image's architecture rather than trusting the flag: an earlier native build can still own
the tag, and a Dockerfile pinning `FROM --platform=...` overrides the command line.

```bash
docker build --platform linux/amd64 -t bespoke-file-parser:0.3.0 .

docker image inspect bespoke-file-parser:0.3.0 --format 'os={{.Os}} arch={{.Architecture}}'
# want: os=linux arch=amd64

docker save bespoke-file-parser:0.3.0 -o bespoke-file-parser-0.3.0.tar
```

:::{warning}

Registration does not check the archive's architecture. An `arm64` image registers, reports `READY`,
and activates, then fails at ingest with an exec format error. Rebuild on any other architecture,
with `--no-cache` if the build already reported success.
:::

Save a single-platform image. Nominal receives an archive rather than a registry it can negotiate a
platform with, so do not assume it picks `amd64` out of a multi-platform archive.

### Testing it without Docker

`run()` takes an environment, so you can exercise the extractor as a normal function:

```python
from pathlib import Path

from extractor_manifest import extract

source = Path("path/to/data.bin").resolve()  # An existing file your parser accepts.
out_dir = Path("local-output").resolve()
out_dir.mkdir(exist_ok=True)
ctx = extract.run(
    env={"OUTPUT_DIR": str(out_dir), "INPUT_FILE": str(source)},
    exit=False,
)
print(ctx.build_manifest())
```

`env` **replaces** the environment rather than merging into it, so it has to carry everything your
code requires: `OUTPUT_DIR`, every required input and parameter. Optional parameters use
their declared defaults. Nothing falls back to an ambient value, and omitting `OUTPUT_DIR`
raises `ExtractorError`. The input file and output directory must exist. Decorator timestamp
defaults describe image registration; they do not populate local job metadata. This template
declares timestamps on each output, so the local run does not need job-level metadata.

## Register and activate an image

Create the extractor once, then give it images over time. `register_image()` uploads the
`docker save` tarball and registers it; `set_active_image()` waits for the push to finish before
activating. Shipping a new version means registering a new image and activating it, and the switch
is atomic.

```python
from extractor_manifest import extract
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

# An extractor carries only identity. How it runs lives on its images.
extractor = client.create_containerized_extractor(
    name="bespoke-file-parser",
    description="Parses bespoke binary logs into telemetry, logs, and video",
)

# Uploads the `docker save` tarball and registers it as an image in one call.
# Build for linux/amd64: registration does not check the architecture, so an
# arm64 tarball registers and activates, then fails at ingest.
image = extractor.register_image(
    "bespoke-file-parser-0.3.0.tar",
    tag="0.3.0",
    **extract.registration_kwargs(),
)

# Waits for the image to finish pushing (PENDING -> READY), then activates it.
# Activation is atomic, so this is also how you ship a new version.
extractor.set_active_image(image)

print(f"Extractor {extractor.rid} now runs image {image.rid}")
```

Importing `extract` does not run extraction because of the `__main__` guard. Run this
registration script in an environment containing the extractor module, parser, and their
dependencies. `registration_kwargs()` derives the inputs, parameters, output format, and
timestamp defaults from the decorators. Environment variable names must be distinct
across inputs and parameters; callers use them as `sources` and `arguments` keys.

Registering attaches an image; it does not activate it. `set_active_image()` waits for the image to
report `READY` first, and `poll_until_ready=False` raises instead of waiting. Future ingests use the
new image, while in-flight jobs finish on the one they started with, so a rollback is reactivating
the prior image and does not repair data already ingested.

Registration tags are immutable versions under the stable extractor, so prefer an existing semver or
CI release identifier over a movable alias like `latest`. Re-registering an existing tag raises
`NominalAlreadyExistsError`; inspect the existing image with
`extractor.search_container_images(tag=...)` rather than retrying under a new suffix.

### From the CLI

The same flow is available from the command line, which is usually what you want in CI. The image's
execution contract lives in a checked-in JSON config. As an alternative to the Python
registration above, save this matching contract as `extractor-config.json`:

```json
{
  "inputs": [
    {
      "name": "Input file",
      "environment_variable": "INPUT_FILE",
      "file_suffixes": ["bin"],
      "required": true
    }
  ],
  "parameters": [
    {
      "name": "Channel prefix",
      "environment_variable": "CHANNEL_PREFIX",
      "required": false
    }
  ],
  "output_format": "manifest",
  "default_timestamp_column": "timestamp",
  "default_timestamp_type": "epoch_microseconds"
}
```

Keep this config aligned with the Python declarations if you use the CLI path.
`create` prints the extractor object; copy its RID into `EXTRACTOR_RID`. If you already
created the extractor through Python, reuse its RID and skip `create`.
`register-image` prints only the new image RID to stdout, so that output can be captured:

```bash
nom container extractor create --profile default -n bespoke-file-parser -d "Parses bespoke binary logs"
EXTRACTOR_RID="<EXTRACTOR_RID>"

IMAGE_RID=$(nom container extractor register-image --profile default -r "$EXTRACTOR_RID" \
  -f bespoke-file-parser-0.3.0.tar -t "$(git rev-parse --short HEAD)" -c extractor-config.json)

nom container extractor set-active-image --profile default -r "$EXTRACTOR_RID" -i "$IMAGE_RID"
```

`nom container extractor validate-config extractor-config.json` runs the same validation that
`register-image` performs, without contacting Nominal or needing credentials, so it can lint the
checked-in config in CI.

## Run an ingest

`Dataset.add_containerized()` runs the extractor's active image against a dataset:

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

extractor = client.get_containerized_extractor("<EXTRACTOR_RID>")
dataset = client.get_dataset("<DATASET_RID>")

job = dataset.add_containerized(
    extractor,
    sources={"INPUT_FILE": "path/to/data.bin"},
    arguments={"CHANNEL_PREFIX": "telemetry/"},
    tags={"vehicle_id": "sn-001"},
)

for dataset_file in job.as_files_ingested():
    print(f"ingested {dataset_file.name}")
```

The `sources` keys must match the environment variables of the active image's registered inputs
exactly. `arguments` supplies the image's declared parameters. `tags` supplies additional ingest
tags for outputs that support them; journal log outputs do not carry tags.

Extraction is asynchronous and can emit several files, so this returns an
[ingestion job](/guides/ingest/ingestion-jobs.md) rather than a single file. That page
covers waiting for it, checking per-file status, and cancelling.

Passing `timestamp_column` and `timestamp_type` overrides the image's registered default for this
ingest. Outputs without explicit timestamp metadata inherit that value; per-output
metadata takes precedence. Both override arguments must be given together.

## Timestamps

Nominal places every sample on an absolute timeline. Your output supplies a timestamp per sample,
and timestamp metadata says how to read it. Three levels can specify that metadata, resolved per
output file in this order:

| Precedence | Level | Set with | Scope |
|---|---|---|---|
| 1 (highest) | Per output | `ctx.add_tabular(..., timestamp_column=, timestamp_type=)` | one file in one run |
| 2 | Ingest request | `dataset.add_containerized(..., timestamp_column=, timestamp_type=)` | every output of one run |
| 3 (lowest) | Image default | `register_image(..., default_timestamp_column=, default_timestamp_type=)` | every run of that image |

Levels 2 and 3 resolve **before your container runs**, and ingestion fails if both are absent. That
is why registration requires a default: it guarantees the job always has something to fall back to.
Level 1 is applied afterwards from the manifest your code writes, and overrides the job-level value
for that file only.

Use each level for what it describes:

- **The image default** is the normal shape of this extractor's output. Set it at registration to
  the column and type your code emits on a typical run.
- **The ingest request** is for facts about *this upload* that the image cannot know. Above all,
  this is where a relative t0 belongs: `timestamp_type=ts.Relative("milliseconds", start=t0)`.
- **Per output** is for a run whose files disagree, such as telemetry in microseconds and events in
  seconds. Nothing else needs it.

Per-output metadata accepts numeric types only, `ts.Epoch` or `ts.Relative`. An output needing ISO
8601 or a custom string format has to omit the per-output pair and inherit the job-level value,
which accepts the full range.

## Manage extractors and images

```python
from nominal.core.container_image import ContainerImageStatus

# Find extractors whose active image accepts a given file suffix (no leading dot)
extractors = client.search_containerized_extractors(file_extension="bin")

# List an extractor's images, or filter to the ones that failed to push
images = extractor.search_container_images(status=ContainerImageStatus.FAILED)

# Rename, re-describe, or activate a different image
extractor.update(description="Parses v2 bespoke binary logs")

# Hide from the default upload flow and search, reversibly
extractor.archive()
extractor.unarchive()
```

The CLI mirrors these as `nom container extractor search|get|update|archive|unarchive` and
`nom container image search|get|delete`. An image cannot be deleted while an extractor still has it
active.
