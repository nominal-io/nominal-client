---
myst:
  html_meta:
    description: "Write, build, register and run containerized file extractors for formats Nominal doesn't read."
---

# File extractors

{.lead}
A file extractor is a Docker image that Nominal runs during ingest to convert files it can't read, such as a bespoke binary log.
It reads the uploaded files and writes outputs in formats Nominal ingests.

The authoring helpers in `nominal.experimental.extractor` are experimental, so their API may change between releases. The container contract they implement is stable.

Use an extractor when the format recurs, when the conversion should live in one versioned place, or when people upload these files in the Nominal app.
For a one-off conversion, convert locally and [upload the result](/guides/datasets/overview.md).
For the container contract in other languages, and for setting up extractors in the UI, see [Ingest custom formats](https://docs.nominal.io/core/documentation/platform/data/containerized-extractors).

```{include} /guides/_snippets/connect-the-client.md
```

## Write the extractor

The container gets its inputs and parameters as environment variables, and writes outputs to `$OUTPUT_DIR`.
In Python, decorators read the environment for you.

`@manifest_extractor` lets one run write any number of files: tables, logs, Avro streams and video.
Declare each file with a `ctx.add_*` method, and the runtime writes `manifest.json` when your function returns.
Save this as `extractor.py`, with your own `parse_bespoke_binary` in `bespoke_parser.py`:

```python
from pathlib import Path

from bespoke_parser import parse_bespoke_binary
from nominal.experimental.extractor import ManifestExtractorContext, input, manifest_extractor, parameter


@manifest_extractor(default_timestamp_column="timestamp", default_timestamp_type="epoch_microseconds")
@input("source", envvar="INPUT_FILE", name="Input file", file_suffixes=["bin"])
@parameter("prefix", envvar="CHANNEL_PREFIX", name="Channel prefix", default="telemetry/")
def extract(ctx: ManifestExtractorContext, *, source: Path, prefix: str) -> None:
    frames = parse_bespoke_binary(source)  # your parser: pandas frames and a video writer

    telemetry = ctx.output_dir / "flight_data.parquet"
    frames.telemetry.to_parquet(telemetry, index=False)
    ctx.add_tabular(telemetry, tag_columns={"vehicle_id": "veh_id"}, channel_prefix=prefix)

    logs = ctx.output_dir / "system_logs.jsonl"
    frames.logs.to_json(logs, orient="records", lines=True)
    ctx.add_journal_json(logs, timestamp_column="__REALTIME_TIMESTAMP", timestamp_type="epoch_microseconds")

    video = ctx.output_dir / "nose_camera.mp4"
    frames.write_video(video)
    ctx.add_video(video, channel="nose_camera", start=frames.started_at)


if __name__ == "__main__":
    extract.run()
```

| Method | Output | Options |
| --- | --- | --- |
| `ctx.add_tabular` | Parquet or CSV | `timestamp_column`, `timestamp_type`, `tag_columns`, `channel_prefix` |
| `ctx.add_journal_json` | [Journal JSON](/guides/datasets/journal-json.md) logs | `timestamp_column`, `timestamp_type` |
| `ctx.add_avro_stream` | [Avro streams](/guides/datasets/avro-streams.md) | `channel_prefix`, `timestamp_type` |
| `ctx.add_video` | A video channel | `start`, `frame_timestamps`, `true_frame_rate`, `scale_factor` |

`@single_file_extractor` is the older contract: one Parquet, CSV or Avro stream file, declared with `ctx.set_output(path)`.
Use it only for images already registered that way.

### Inputs and parameters

- `@input` declares a file the extractor reads, such as the capture or a calibration table. For a variable number of files, take a `.zip` or `.tar` input and unpack it.
- `@parameter` declares a string setting, such as a threshold. Convert it yourself. Put structured settings in an input instead.
- Leaving out `default=` makes either one required. A missing required input fails before anything uploads.
- `envvar` is the name callers use in `sources` and `arguments`. It must be unique across inputs and parameters.
- `file_suffixes` describe the input for search. They aren't enforced.

The older `ctx.input()`, `ctx.param()` and `ctx.get_param()` accessors still work, but log a warning and add nothing to registration.

### Test it locally

`run` takes an environment, so you can call the extractor without Docker:

```python
from pathlib import Path

from extractor import extract

Path("out").mkdir(exist_ok=True)
ctx = extract.run(env={"OUTPUT_DIR": str(Path("out").resolve()), "INPUT_FILE": "/path/to/data.bin"}, exit=False)
print(ctx.build_manifest())
```

`env` replaces the whole environment, so include `OUTPUT_DIR` and every required input and parameter.
With `exit=False`, errors raise instead of exiting the process.

Logs written to `stdout` and `stderr` go to a logs dataset for each ingest, up to 1 MiB per job.
Check it first when an ingest fails.

## Build the image

Install the extractor's dependencies from its lockfile, outside the home directory:

```dockerfile
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.9.7 /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY extractor.py bespoke_parser.py ./
ENTRYPOINT ["/app/.venv/bin/python", "/app/extractor.py"]
```

The container runs as a fixed non-root user with no extra capabilities.
`$HOME` is an empty writable folder that hides anything the build put there, so a package installed with `pip install --user` is missing at runtime.

Nominal runs images on `linux/amd64`. Build for it and check the result:

```bash
docker build --platform linux/amd64 -t bespoke-file-parser:0.3.0 .
docker image inspect bespoke-file-parser:0.3.0 --format '{{.Os}}/{{.Architecture}}'  # linux/amd64
docker save bespoke-file-parser:0.3.0 -o bespoke-file-parser-0.3.0.tar
```

:::{warning}
Registration doesn't check the architecture. An `arm64` image registers and activates, then fails every ingest with an exec format error.
Save a single-platform image.
:::

## Register and activate an image

An extractor has a name and description. Its images hold everything else: inputs, parameters, output format and timestamp defaults.
Create the extractor once, then register and activate each new version:

```python
from extractor import extract

extractor = client.create_containerized_extractor(name="bespoke-file-parser", description="Bespoke binary logs")
image = extractor.register_image("bespoke-file-parser-0.3.0.tar", tag="0.3.0", **extract.registration_kwargs())
extractor.set_active_image(image)  # waits until the image is ready
```

`registration_kwargs` reads the inputs, parameters, output format and timestamp defaults from the decorators.
Without it, `register_image` defaults to the `PARQUET` output format, which doesn't match a manifest extractor.

Activation switches new ingests to the image at once. Running jobs finish on their old image.
To roll back, activate the previous image. That doesn't change data already ingested.
Tags can't be reused, so use release versions rather than `latest`.

### From the CLI

In CI, register with `nom` and a JSON config instead:

```json
{
  "inputs": [{"name": "Input file", "environment_variable": "INPUT_FILE", "file_suffixes": ["bin"], "required": true}],
  "parameters": [{"name": "Channel prefix", "environment_variable": "CHANNEL_PREFIX", "required": false}],
  "output_format": "manifest",
  "default_timestamp_column": "timestamp",
  "default_timestamp_type": "epoch_microseconds"
}
```

```bash
nom container extractor create --profile default -n bespoke-file-parser -d "Bespoke binary logs"
IMAGE_RID=$(nom container extractor register-image --profile default -r "<EXTRACTOR_RID>" \
  -f bespoke-file-parser-0.3.0.tar -t "0.3.0" -c extractor-config.json)
nom container extractor set-active-image --profile default -r "<EXTRACTOR_RID>" -i "$IMAGE_RID"
```

`nom container extractor validate-config extractor-config.json` checks a config offline.
Keep it in step with the Python decorators.

## Run an ingest

{py:meth}`~nominal.core.Dataset.add_containerized` runs the active image on your files and returns an [ingest job](/guides/ingest/ingestion-jobs.md):

```python
extractor = client.get_containerized_extractor("<EXTRACTOR_RID>")
job = client.get_dataset("<DATASET_RID>").add_containerized(
    extractor,
    sources={"INPUT_FILE": "data.bin"},  # keys are the inputs' envvar names
    arguments={"CHANNEL_PREFIX": "telemetry/"},
    tags={"vehicle_id": "sn-001"},  # not applied to log outputs
)
for file in job.as_files_ingested():
    print(file.name, file.ingest_status.name)
```

## Timestamps

Each output's timestamp column and type come from the first of these that sets them:

1. The `ctx.add_*` call for that file.
1. `timestamp_column` and `timestamp_type` passed to `add_containerized`, for this ingest.
1. The image's defaults from registration.

Set the image default to what the extractor usually writes.
Pass values to `add_containerized` for facts about one upload, such as a relative start time: `timestamp_type=ts.Relative("milliseconds", start=t0)`.
Set them per file only when files in one run differ.
Per-file values take numeric types only. For ISO 8601 or custom formats, set them on the ingest or the image.

## Manage extractors

```python
from nominal.core.container_image import ContainerImageStatus

client.search_containerized_extractors(file_extension="bin")  # by input suffix, no dot
extractor.search_container_images(status=ContainerImageStatus.FAILED)
extractor.update(description="Bespoke binary logs, v2")
extractor.archive()  # hides it from uploads and search; unarchive() restores it
```

The CLI has the same operations under `nom container extractor` and `nom container image`.
You can't delete an image while it's active.
