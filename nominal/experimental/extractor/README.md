# Containerized Extractors

> EXPERIMENTAL / UNSTABLE — the in-container contract is set by the ingest pipeline and may change
> without notice.

Nominal ingests CSV, Parquet, MCAP, and a handful of other formats natively. A *containerized
extractor* covers everything else: you write a Docker image that reads your proprietary format and
writes something Nominal understands, register it, and from then on ingest is a normal
`dataset.add_containerized(...)` call. Nominal runs your container, mounts the input files, and
ingests whatever your code declares.

This package connects your parser to that container contract. Declare the files and settings your
function needs, receive them as Python arguments, and use `ctx` to describe the outputs. Those
same declarations supply the metadata needed to register the image.

The walkthrough starts with a local example, adds settings and validation, then covers outputs,
image registration, and publishing. The reference tables explain the options as you encounter them.

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

## Why the decorators look like Click

A command-line tool and an extractor face the same setup problem: external inputs need to become
usable Python values before the actual work starts. Click expresses that setup with an
[argument](https://click.palletsprojects.com/en/stable/arguments/) for a positional input and an
[option](https://click.palletsprojects.com/en/stable/options/) for a named setting:

```python
from pathlib import Path
import click

@click.command()
@click.argument("source", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--prefix", default="copied", help="Output file name without the extension.")
def copy_csv(source: Path, prefix: str) -> None:
    Path(f"{prefix}.csv").write_bytes(source.read_bytes())

if __name__ == "__main__":
    copy_csv()
```

Running `python cli.py source.csv --prefix flight` gives the callback a `Path` and the string
`"flight"`. Click handles lookup and defaults before invoking it. The function can concentrate on
copying the data.

An extractor receives mounted files and environment variables instead of command-line tokens.
`@input` and `@parameter` describe that boundary in the same way. The framework handles resolution,
conversion, and validation; your callback handles the data. Click is the inspiration for this API,
not a dependency needed to run an extractor.

## Start with a declarative extractor

This CSV example needs only the standard library and the SDK. Save it as `copy_csv.py`:

```python
from pathlib import Path
from nominal.experimental import extractor as ex

@ex.manifest_extractor(
    default_timestamp_column="time",
    default_timestamp_type="epoch_seconds",
)
@ex.input("source", file_suffixes=["csv"])
@ex.parameter("prefix", default="copied", description="Output file name without the extension")
def copy_csv(ctx: ex.ManifestExtractorContext, *, source: Path, prefix: str) -> None:
    output = ctx.output_dir / f"{prefix}.csv"
    output.write_bytes(source.read_bytes())
    ctx.add_tabular(output, timestamp_column="time", timestamp_type="epoch_seconds")

if __name__ == "__main__":
    copy_csv.run()
```

Try it with a small input:

```bash
mkdir -p /tmp/csv-output
printf 'time,value\n1,42\n' > /tmp/source.csv
OUTPUT_DIR=/tmp/csv-output SOURCE=/tmp/source.csv python copy_csv.py
```
The callback receives `source` as a `Path` and `prefix` as a string. Omitting `PREFIX` uses
`"copied"`; setting `PREFIX=flight` writes `flight.csv`. Both names default to uppercase environment
variables, so no lookups are needed inside the callback.

`ctx.output_dir` chooses where to write, and `ctx.add_tabular` tells Nominal how to read the result.
The runtime writes `manifest.json` alongside your output. The timestamp options on `add_tabular`
describe this output; the outer decorator's timestamp options supply image-registration defaults.
Importing `copy_csv` lets registration code call `copy_csv.registration_kwargs()` without running
extraction.

### Choose the import style that fits your code

The examples use `from nominal.experimental import extractor as ex` to keep declarations compact.
The same objects also live in focused public modules:

| Module | Public API |
|---|---|
| `nominal.experimental.extractor.decorators` | `input`, `parameter`, `error`, `manifest_extractor`, `single_file_extractor` |
| `nominal.experimental.extractor.context` | `ExtractorContext`, `ManifestExtractorContext`, `SingleFileExtractorContext` |
| `nominal.experimental.extractor.types` | `Choice`, `IntRange`, `FloatRange`, `BadParameter` |
| `nominal.experimental.extractor.runner` | `Extractor`, the decorated entrypoint with execution and metadata-export methods |

For explicit imports, use:

```python
from nominal.experimental.extractor.context import ManifestExtractorContext
from nominal.experimental.extractor.decorators import input, manifest_extractor, parameter
from nominal.experimental.extractor.types import IntRange
```

Package-level imports continue to work. `ExtractorError` and `TimestampMetadata` are also available
from the package; their definitions belong to `nominal.core`.

## Inputs and parameters

Now suppose the parser needs optional calibration, a partition count, and an on/off setting.
Add a declaration for each value instead of growing a setup block inside the callback. The
example below shows the signature; replace its body with your parsing and output code.

Keep the extractor decorator outermost. Each `@input("recording")` or `@parameter("parts")`
names a function argument and defaults to the uppercase environment variable (`RECORDING`,
`PARTS`). Override it with `envvar="SOURCE"`; `name=` and `description=` supply registration
metadata. Input `file_suffixes=` supplies the registration file filters.

```python
from pathlib import Path
from nominal.experimental import extractor as ex

@ex.manifest_extractor
@ex.input("recording", file_suffixes=["flight"])
@ex.input("calibration", default=None)
@ex.parameter("parts", type=int, default=2)
@ex.parameter("enabled", type=bool, default=True)
def split(
    ctx: ex.ManifestExtractorContext,
    *,
    recording: Path,
    calibration: Path | None,
    parts: int,
    enabled: bool,
) -> None:
    ...  # Process the resolved arguments and declare outputs using ctx.
```

Arguments are required unless a decorator supplies a default. Optional inputs use `default=None`
and arrive as `Path | None`; supplied input paths must exist as files. Parameter defaults are
already-converted Python values and may be `None`. Keep defaults in decorators rather than the
function signature. Supplied strings use `type=`. When omitted, a `str`, `int`, `float`, or `bool` default selects
its converter; otherwise conversion defaults to `str`. An explicit converter always wins.
An empty string is a supplied value, not a request for the default. Booleans accept `true/false`, `yes/no`, `on/off`, and `1/0`,
case-insensitively. Custom converters may raise `ValueError` or `TypeError` to reject a value.
All declared arguments resolve before user code runs. Binding failures raise `ExtractorError`
and participate in the same error mappings as extraction failures.

When registered `_NOMINAL_*` metadata is present, it is authoritative. Local runs without it
resolve directly from the supplied environment. Parameter types and defaults are enforced by
this runtime; the registration API currently stores neither as structured fields.

### Declaration reference

| Option | Applies to | Meaning |
|---|---|---|
| First positional argument | Both decorators | Exact Python function argument to populate |
| `envvar=` | Both | Container environment variable; defaults to the uppercase argument name |
| `name=` | Both | Registration display name; defaults to the argument name |
| `description=` | Both | Optional registration description |
| `file_suffixes=` | Inputs | Registration suffix filters such as `["mcap", "csv"]`; empty accepts any suffix |
| `default=None` | Inputs | Allow an omitted input and inject `None`; other input defaults are unsupported |
| `type=` | Parameters | Convert a supplied string; infer from a basic default when omitted, otherwise use `str` |
| `default=` | Parameters | Inject this Python value when absent; omitting it makes the parameter required |

The first callback argument receives the context. Declare the remaining arguments explicitly;
keyword-only arguments are recommended. Decorated arguments cannot be positional-only, have
function-signature defaults, or be supplied only through `**kwargs`. Every required callback
argument after the context needs a declaration. Type annotations help readers and type checkers;
they do not select a converter. Use `type=int` for a required integer, or `default=2` to infer it.

Keep the outer extractor decorator above all argument decorators. Registration lists preserve the
top-to-bottom order of declarations. Duplicate argument names or environment variables fail during decoration; display names must be
unique within each category (inputs or parameters). Environment variable names
must be valid identifiers; `OUTPUT_DIR`, `NOMINAL_EXTRACTOR_INPUT_DIR`, and
names beginning with `_NOMINAL_` are reserved.

At runtime, declared arguments match registered metadata by **environment variable**, not display
name. An omitted optional input becomes `None`; a supplied path that is not a file fails. With
parameter metadata present, a parameter absent from that metadata is a contract error even if its
decorator specifies a default. Without metadata, local runs use the declared environment variables.

### Conversion and validation

Use the built-in converters to express common constraints directly:

```python
@ex.manifest_extractor
@ex.input("source", file_suffixes=["csv"])
@ex.parameter("mode", type=ex.Choice(["fast", "precise"]), default="fast")
@ex.parameter("parts", type=ex.IntRange(min=1, max=64), default=2)
@ex.parameter("gain", type=ex.FloatRange(min=0.0), default=1.0)
@ex.parameter("enabled", default=True)
def process(
    ctx: ex.ManifestExtractorContext,
    *,
    source: Path,
    mode: str,
    parts: int,
    gain: float,
    enabled: bool,
) -> None:
    ...  # Apply these settings while parsing source and declaring outputs.
```

| Converter | Accepted supplied values |
|---|---|
| `str` | Any string, including an empty string |
| `int` | Strings accepted by Python's integer converter |
| `float` | Strings accepted by Python's float converter, excluding NaN and infinity |
| `bool` | `true/false`, `yes/no`, `on/off`, `1/0`, case-insensitively |
| `Choice([...])` | An exact, case-sensitive match from a nonempty collection of unique strings |
| `IntRange(min=None, max=None)` | An integer within the inclusive bounds; either bound may be omitted |
| `FloatRange(min=None, max=None)` | A finite number within the inclusive bounds; rejects NaN and infinity |
| Custom callable | A string passed to your callable, returning the callback's Python value |

Defaults are already-converted values: use `default=2`, not `default="2"`, with `type=int`.
Basic built-in converters and the three constraints above validate non-`None` defaults when the
parameter is declared. A `float` default must be finite; integer values are also accepted for float
parameters. `default=None` makes a parameter optional without inferring a converter; use, for example,
`type=int, default=None` and annotate its callback argument as `int | None`. Arbitrary custom
converters are never called on defaults; the author must supply a valid Python value.

For application-specific validation, raise `BadParameter` with a message suitable for the user:

```python
def positive_odd(value: str) -> int:
    result = int(value)
    if result <= 0 or result % 2 == 0:
        raise ex.BadParameter("must be a positive odd integer")
    return result

# Use @ex.parameter("parts", type=positive_odd, default=3) above the callback.
```

`BadParameter` becomes `ExtractorError` naming the argument and environment variable and including
your message. Do not put raw supplied values or secrets in that message. Ordinary `ValueError` and
`TypeError` also become `ExtractorError`, but their messages and supplied values are omitted from
the diagnostic. Other converter exceptions propagate unchanged through normal extractor error handling; their
messages can appear in tracebacks and mapped termination reports. Wrap third-party failures in
`ValueError` or `TypeError` to sanitize them, or `BadParameter` with a message safe to display.

### Job metadata

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

Requiredness is derived from the decorator default and included in registration:

- a missing required **input** raises in `add_containerized`, before anything uploads;
- a missing required **parameter** is checked by the extractor runtime during binding, before your
  function executes. Use `@parameter("name")` for a required value or provide a decorator default.

An input's `file_suffixes` also drive discovery: `search_containerized_extractors(file_extension=...)`
matches on them, so they determine which extractors a given file is offered for. The suffixes are
descriptive, not enforced locally — a `.txt` sent to an input registered `["json"]` still uploads.

## The contract

Nominal drives your container entirely through the environment:

| | |
|---|---|
| Input files | mounted under `/input`, and each input's path is also in the environment variable declared for it |
| Output directory | named by `$OUTPUT_DIR` |
| Parameters | environment variables; `@parameter` converts supplied strings before your function runs |

Your job is to write files into `$OUTPUT_DIR` and say what they are. There are two output contracts,
each with its own decorator, and the one you pick must match the `output_format` you register the
image with — `Extractor.run` fails at startup if they disagree.

**Write new extractors as manifest extractors.** The manifest contract is the current one and a
strict superset: it describes each output file individually, so one image can emit several files, mix
telemetry with logs and video, and set per-file timestamps, tags, and channel prefixes. Single-file
extractors are the original contract, kept for images already registered that way. Changing an
image's output format later requires registering a new image.

## Manifest extractors

Once inputs are resolved, the parser can produce more than one kind of result. A flight recording,
for example, might contain telemetry, events, and camera footage. Keep one callback and declare
each result through `ctx`; the manifest tells Nominal how to ingest each file.

Register these images with `MANIFEST`. Use one method per output format, each taking only the
options that format uses. The runner writes `manifest.json` after your callback returns.

| Method | For | Options |
|---|---|---|
| `add_tabular` | `.csv` / `.parquet` (and `.gz`) | `tag_columns`, `channel_prefix`, `units`, `timestamp_column`/`timestamp_type` |
| `add_avro_stream` | `.avro` / `.avro.gz` | `channel_prefix`, `units`, `timestamp_type` |
| `add_journal_json` | `.jsonl` / `.jsonl.gz`, ingested as logs | `timestamp_column`/`timestamp_type` |
| `add_video` | any supported video container | `channel` (required), `start` or `frame_timestamps` |

The gaps are deliberate. Avro records carry their own channel, values, and tags, so no tag columns
are needed — but their timestamps are bare numbers, so `add_avro_stream` still takes a `timestamp_type`
saying how to read them. Log samples carry no tags and all land on one channel, so tag columns and a
channel prefix would be silently dropped. Each method also checks the file extension its format
requires, so a mismatch fails at the call rather than server-side after upload.

```python
from pathlib import Path
from nominal.experimental import extractor as ex

@ex.manifest_extractor(
    default_timestamp_column="ts",
    default_timestamp_type="epoch_nanoseconds",
)
@ex.input("recording", name="Recording", file_suffixes=["flight"])
@ex.parameter("parts", name="Parts", type=int, default=2)
def split(ctx: ex.ManifestExtractorContext, *, recording: Path, parts: int) -> None:
    decoded = read_my_format(recording)

    for i, chunk in enumerate(chunks_of(decoded.telemetry, parts)):
        part = ctx.output_dir / f"part_{i}.parquet"
        write_parquet(chunk, part)
        ctx.add_tabular(part, tag_columns={"vehicle": "veh_id"}, units={"pressure": "Pa"})

    events = ctx.output_dir / "events.jsonl"
    write_jsonl(decoded.events, events)          # each line needs a MESSAGE field
    ctx.add_journal_json(events, timestamp_column="ts", timestamp_type="epoch_microseconds")

    footage = ctx.output_dir / "front.mp4"
    decoded.camera.write_h264(footage)
    ctx.add_video(footage, channel="camera/front", start=decoded.started_at)

if __name__ == "__main__":
    split.run()
```

Save this entrypoint as `extractor.py` for the build and registration examples below.
`read_my_format`, `chunks_of`, `write_parquet`, and `write_jsonl` are application-provided parsing
and writing helpers. A manifest extractor may emit telemetry, videos, or only videos.

`units` maps channel names to unit symbols on each tabular or Avro output. The runtime copies the
mapping into that output's manifest entry and preserves symbols as supplied. Omitting it, passing
`None`, or passing an empty mapping writes an empty units map.

Declare units where you declare the output, since different files from the same extractor can
have different channels and units. For example:

```python
ctx.add_tabular(table, units={"pressure": "Pa", "temperature": "K"})
ctx.add_avro_stream(records, units={"voltage": "V"})
```

These maps travel in the output manifest; they are not input parameters or image-registration
metadata. Both the package-level context import and
`nominal.experimental.extractor.context.ManifestExtractorContext` expose the same `units=` API.

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
from pathlib import Path
from nominal.core.container_image import FileOutputFormat
from nominal.experimental import extractor as ex

@ex.single_file_extractor(
    output_format=FileOutputFormat.PARQUET,
    default_timestamp_column="ts",
    default_timestamp_type="epoch_nanoseconds",
)
@ex.input("recording", file_suffixes=["flight"])
def convert(ctx: ex.SingleFileExtractorContext, *, recording: Path) -> None:
    table = read_my_format(recording)
    out = ctx.output_dir / "converted.parquet"
    write_parquet(table, out)
    ctx.set_output(out)                          # declare it

if __name__ == "__main__":
    convert.run()                                # the container entrypoint
```

A second `set_output` call raises, since the registered format describes one file. Everything below —
inputs, parameters, errors, building, registering, ingesting — applies to both contracts.

## Timestamps

Nominal places every sample on an absolute timeline. Your output supplies a numeric or string
timestamp per sample; timestamp metadata says how to read it.

### Where metadata comes from, and what wins

Three levels can specify it. Per output file, the pipeline resolves them in this order:

| Precedence | Level | Set with | Scope |
|---|---|---|---|
| 1 (highest) | Per output | `ctx.add_tabular(..., timestamp_column=, timestamp_type=)` | one file in one run |
| 2 | Ingest request | `dataset.add_containerized(..., timestamp_column=, timestamp_type=)` | every output of one run |
| 3 (lowest) | Image default | `@manifest_extractor(default_timestamp_column=, default_timestamp_type=)` exported by `.registration_kwargs()` | every run of that image |

Levels 2 and 3 are resolved **before the container runs**, and ingestion fails if both are absent.
That is why registration requires a default: it guarantees the job always has metadata to fall back
to. Level 1 is applied afterward, from the manifest your code writes, and overrides the resolved
job-level value for that file only.

Use each level for what it describes:

- **the image default** — the normal shape of this extractor's output. Declare its column and type
  on the extractor decorator, then register using `.registration_kwargs()`. Single-file extractors
  accept the same timestamp options.
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

### Structured failures

Declare each expected failure with `@error` below the outer extractor decorator, alongside
`@input` and `@parameter`. It works with both manifest and single-file extractors. The framework
catches matching exceptions from startup, argument binding, extraction, or output finalization,
writes structured JSON to the termination log and stderr, and exits with the mapped status:

```python
from pathlib import Path
from nominal.experimental import extractor as ex

class MalformedRecordingError(ValueError):
    pass

@ex.manifest_extractor(
    default_timestamp_column="ts",
    default_timestamp_type="epoch_nanoseconds",
)
@ex.error(
    MalformedRecordingError,
    code="MALFORMED_INPUT",
    exit_code=64,
    message="The recording could not be decoded.",
)
@ex.error(
    ex.ExtractorError,
    code="EXTRACTOR_CONTRACT",
    exit_code=65,
    message="The extractor contract could not be satisfied.",
)
@ex.input("recording", file_suffixes=["flight"])
@ex.parameter("parts", type=int, default=2)
def convert(ctx: ex.ManifestExtractorContext, *, recording: Path, parts: int) -> None:
    ...  # Parse recording; raise MalformedRecordingError for invalid content; declare outputs.

if __name__ == "__main__":
    convert.run()
```

The payload contains `code`, `message` (from `str(exception)`), and `retryable` (defaults to
`False`; set `retryable=True` on `@error` to override). Stack declarations for different
`Exception` subclasses; duplicate declarations for the same class are rejected. The outer
extractor decorator must stay on top, but the relative order of `@error`, `@input`, and
`@parameter` does not affect error matching. Use specific exception classes for expected failures
so programming errors are not mislabeled. Subclasses match;
the nearest registered class in Python's method resolution order wins. Mapping `ExtractorError`
intentionally includes startup configuration, argument binding, and output-contract failures.
Use a separate application exception such as `MalformedRecordingError` for bad input contents,
so a deployment fault does not receive a data-error code.

The default destination is `/dev/termination-log`. For local tests or a custom container mount,
pass an explicit trusted path to `run(termination_log_path=...)`. Environment variables, including
`TERMINATION_LOG_PATH`, cannot change the destination. Writing the file is best-effort: if it
fails, the framework still emits JSON to stderr and exits with the mapped code.

The JSON payload is limited to 4,096 UTF-8 bytes to fit Kubernetes'
[per-container termination message limit](https://kubernetes.io/docs/tasks/debug/debug-application/determine-reason-pod-failure/). Long messages are shortened on a Unicode boundary; the code and retryable flag
are preserved. An error code that cannot fit even with an empty message is rejected when declaring
`@error`. Stderr contains the structured JSON followed by the full traceback, retaining the
original exception message for debugging. Exit codes
must be integers from 1 through 255. Unmapped failures keep the traceback and exit status 1;
`exit=False` re-raises without writing a termination message. Direct calls to the decorated
function likewise propagate exceptions; reporting belongs to `run()`.

`message=` on `@error` supplies static catalog fallback text. It is optional for runtime-only use,
and does **not** replace `str(exception)` in runtime reports. Catalog export requires it for each
mapping, because the platform needs a useful message when no termination message is available.
Generate `exit_code_mappings` with `catalog_manifest()` as described below; direct
`registration_kwargs()` and the image-registration API do not publish error policies.

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

### Testing before building

Call `run` with an explicit environment and `exit=False` to exercise binding, your extraction logic,
and output finalization together. This needs no Docker or Nominal connection. Create the input
fixture and output directory first; use a real recording in your extractor's format:


```python
from pathlib import Path
from extractor import split

in_dir = Path("test-data")
out_dir = Path("test-output")
out_dir.mkdir(parents=True, exist_ok=True)
ctx = split.run(
    env={"OUTPUT_DIR": str(out_dir), "RECORDING": str(in_dir / "flight-42.flight")},
    exit=False,
)
print(ctx.build_manifest())
```

`env` *replaces* the environment, it does not merge into it, so it must carry everything your code
reads: `OUTPUT_DIR`, the inputs, and required parameters. Omitted optional parameters use their
decorator defaults. Nothing falls back to the ambient value — omitting
`OUTPUT_DIR` fails with `ExtractorError`, raised to the caller under `exit=False` as above, or printed
as a traceback before a non-zero exit under `run()`'s default `exit=True`.

In tests, assert output contents and `ctx.build_manifest()` for manifest extractors; for
single-file extractors, assert the declared file's contents. Also cover missing required values,
invalid conversions, optional inputs, and your parser's expected failures. `run()` returns the
context, not your callback's return value. The callback should declare outputs and return `None`.
A direct `split(ctx)` call binds arguments but does not finalize outputs or report mapped failures;
prefer `run(env=..., exit=False)` for tests of the complete runtime.

### Build and export

Then save a tarball for upload:

```bash
docker build --platform linux/amd64 -t my-extractor:v1 .
docker save my-extractor:v1 -o my-extractor-v1.tar
```

Nominal runs extractor images on amd64, so `--platform linux/amd64` is required. Registration does
not check it: an arm64 build (the default on Apple Silicon) registers and activates, then fails at
ingest with an exec format error.

## Registering it

The decorators already describe what your extractor accepts. Reuse that contract when registering
the image instead of maintaining a second list of inputs and parameters. If you rename an input or
make a parameter optional, the generated registration metadata follows the declaration.

There are three steps, all from `nominal.core`: create the extractor, register an image using
`**split.registration_kwargs()`, then activate that image.

```python
from nominal.core import NominalClient
from extractor import split  # The decorated entrypoint saved as extractor.py.

client = NominalClient.from_profile("staging")

extractor = client.create_containerized_extractor(
    "flight-recorder",
    description="Splits .flight recordings into telemetry and camera channels",
)

image = extractor.register_image(
    "my-extractor-v1.tar",
    tag="v1",
    **split.registration_kwargs(),
)

extractor.set_active_image(image)
```

Notes worth knowing before you hit them:

- **`registration_kwargs()` is pure.** It reads declarations without reading the environment or
  running extraction. It emits the existing SDK input/parameter objects and output/timestamp settings.
- **`output_format` is automatic for manifest extractors.** For single-file metadata generation,
  declare `@single_file_extractor(output_format=FileOutputFormat.CSV, ...)` (or `PARQUET` /
  `AVRO_STREAM`). An explicit format is also checked against the registered format at runtime.
- **Tags are immutable.** Re-registering an existing tag raises `NominalAlreadyExistsError`; bump the
  tag instead.
- **`default_timestamp_column` / `default_timestamp_type` are required** even if every ingest
  overrides them. Declare both on the extractor decorator to generate registration metadata. These
  settings describe image registration defaults; they do not change local output timestamp behavior.
- **Registering does not activate.** `set_active_image` decides which image runs.
- **`envvar` is how your code finds things.** The argument decorators use it for both runtime
  lookup and registration, defaulting to the uppercase argument name.

### What the generated metadata contains

| Declaration | `register_image` argument |
|---|---|
| `@input` | `inputs`: `FileExtractionInput` objects with name, environment variable, description, suffixes, and requiredness |
| `@parameter` | `parameters`: `FileExtractionParameter` objects with name, environment variable, description, and requiredness |
| Outer extractor decorator | `output_format`, `default_timestamp_column`, and `default_timestamp_type` |

Only declarations contribute to this dictionary. Generating it does not inspect the callback body,
run converters, or resolve paths. It returns fresh lists on each call. Both timestamp defaults are
required for generation; omitting them is allowed for runtime-only declarations. Single-file
extractors also require an explicit output format for generation. The helper raises `ValueError`
when these registration settings are incomplete.

`type=` and `default=` remain runtime settings; the current platform registration schema does not
store them. Error mappings configure runtime reporting and are not part of image-registration
kwargs. The helper does not build or upload an image, choose its tag, create an extractor, or
activate it. Keep those deployment decisions in registration code, as above.

## Exporting a catalog manifest

A release pipeline needs the same contract in a different form. Instead of copying declarations
into YAML by hand, add the release identity and let the framework generate the catalog metadata.

Use `.catalog_manifest(...)` when publishing through the first-party extractor catalog's
`extractor.yaml` contract. Use `.registration_kwargs()` for the direct SDK registration shown above.
Both helpers read the same declarations without reading the environment, resolving input paths,
running converters or extraction, or making network requests. Catalog export returns a fresh
JSON/YAML-serializable dictionary; the SDK does not require a YAML library.

Keep release identity and descriptive metadata in one place, shared by your exporter and publisher.
For the runnable `copy_csv.py` from the introduction, create `release.json`:

```json
{
  "id": "copy-csv",
  "version": "1.0.0",
  "display_name": "CSV copy",
  "description": "Copies a CSV recording with epoch-second timestamps."
}
```

Install PyYAML in your development or publishing environment, then save this as
`export_manifest.py` alongside `copy_csv.py` and `release.json`:

```python
import json
from pathlib import Path
import sys

import yaml  # pip install PyYAML; supplied by your publishing environment.
from copy_csv import copy_csv

root = Path(__file__).resolve().parent
release = json.loads((root / "release.json").read_text(encoding="utf-8"))
manifest = copy_csv.catalog_manifest(**release)
target = root / "extractor.yaml"

if sys.argv[1:] == ["--check"]:
    checked_in = yaml.safe_load(target.read_text(encoding="utf-8"))
    if checked_in != manifest:
        raise SystemExit("extractor.yaml differs from the declared contract; regenerate it")
elif not sys.argv[1:]:
    target.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
else:
    raise SystemExit("usage: python export_manifest.py [--check]")
```

Run `python export_manifest.py` to generate the checked-in file, and
`python export_manifest.py --check` in CI. Comparing parsed YAML avoids failures caused only by
formatting. Keep extraction inside the entrypoint's `if __name__ == "__main__"` guard so importing
it is safe. This is ordinary publishing code, not an SDK command-line or test framework.

### Catalog fields and constraints

| Source | Catalog field |
|---|---|
| Explicit export arguments | `id`, `version`, `display_name`, `description` |
| `@input` | `inputs`: environment variable, suffix filters, and requiredness |
| `@parameter` | `parameters`: environment variable, display name, optional description, and requiredness |
| Extractor decorator | `output_file_format`, `default_timestamp_metadata` |
| `@error` | `exit_code_mappings`: exit status, error code, static message, and retryability |

The catalog schema omits input display names/descriptions and parameter converters/default values.
Those remain in the runtime declarations; direct SDK registration retains input names/descriptions.
Export validates the catalog's narrower contract and raises `ValueError` for unsupported declarations:

- `id` uses lowercase letters/digits separated by hyphens, up to 64 characters. `version` is
  `major.minor.patch` with no leading zeros or prerelease suffix. Display name and description are
  nonblank, limited to 128 and 1,024 characters respectively.
- At least one input is required. Every input needs nonempty suffix filters without leading dots,
  such as `["csv", "csv.gz"]`. Environment variables must start with an uppercase letter and
  otherwise contain uppercase letters, digits, or underscores. Runtime-reserved names remain forbidden.
- Both timestamp defaults are required; the column is nonblank and at most 128 characters.
  Catalog defaults support epoch units and ISO 8601. Relative origins and custom parsing rules
  cannot be represented and are rejected instead of being dropped. Supply them per ingest when needed.
  Single-file extractors also need their explicit output format.
- Parameter descriptions are limited to 512 characters. Each error needs a nonblank static
  `message` of at most 512 characters and a code starting with an uppercase letter, followed by
  uppercase letters, digits, or underscores. Platform codes `IMAGE_PULL_FAILED`, `EXTRACTOR_TIMEOUT`,
  `EXTRACTOR_OOM_KILLED`, `OUTPUT_UPLOAD_FAILED`, `INVALID_OUTPUT`, and `UNKNOWN` are reserved.
- Identical fallback mappings sharing an exit status coalesce into one entry. Conflicting mappings
  for the same exit status are rejected, since the platform cannot distinguish their exception classes.

Generation and the CI comparison check **contract drift**. They do not establish Docker image
identity or detect parser/source changes that leave declarations unchanged, and do not replace a
release-version policy. Your publisher still owns Docker builds, S3/image uploads, checksums,
version immutability, registration, and activation. Feed it the same release metadata rather than
maintaining a second version value.

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
`IngestionJob` rather than a single file. `as_files_ingested()` waits out both stages — the container
producing its outputs, then each output ingesting — and re-reads the job's file list as it goes, which
matters for a manifest extractor: none of its outputs are registered until the container exits, so a
list read at trigger time is empty. Use `job.refresh()` then `job.status` to poll the job itself
(`status` is a snapshot; refreshing is what advances it), `job.dataset_files()` for a point-in-time
list of what it has produced so far, and `job.cancel()` to stop it.

The keys of `sources` are the input environment variables you registered, and they must match the
active image's inputs exactly. `timestamp_column` / `timestamp_type` on this call override the image's
default for this ingest.

## Getting the container's logs

The extractor's stdout and stderr land in a log dataset in your workspace, capped at 1 MiB per job. The
runtime's own log lines go through the `nominal.experimental.extractor` logger; `run()` configures
logging when it is the entrypoint, so your own `logging` calls show up there too.

To trace the framework, configure logging before calling `run()`:

```python
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger("nominal.experimental.extractor").setLevel(logging.DEBUG)
convert.run()
```

DEBUG events describe context lookup sources, argument binding, supplied-versus-default
parameter selection, callback invocation, output finalization, registration/catalog export,
and error mapping and termination-log writes. Binding logs include argument and environment
variable names, but never parameter values or defaults. Existing output logs include file names;
exception reporting still includes the exception message and traceback. Your logging handlers
must also allow DEBUG records if you configure handler-level filters.

## Migrating context-based extractors

Editors and type checkers can flag legacy lookups through their `@deprecated` annotations.
The annotations do not emit additional runtime warnings.

Existing `ctx.input()`, `ctx.inputs`, `ctx.param()`, and `ctx.get_param()` calls still work and emit
one logging warning per run. Migrate lookups into declarations and callback arguments:

| Existing lookup | Declaration | Callback argument |
|---|---|---|
| `ctx.input("RECORDING")` | `@ex.input("recording", envvar="RECORDING")` | `recording: Path` |
| `ctx.param("MODE")` | `@ex.parameter("mode", envvar="MODE")` | `mode: str` |
| `int(ctx.get_param("PARTS", "2"))` | `@ex.parameter("parts", envvar="PARTS", type=int, default=2)` | `parts: int` |
| `ctx.get_param("LABEL")` | `@ex.parameter("label", envvar="LABEL", default=None)` | `label: str \| None` |

For unnamed `ctx.input()` or `ctx.inputs` discovery, declare each registered input explicitly using
its actual environment variable. Lookups by display name must likewise map to the registered
**environment variable**. Preserve existing names, descriptions, suffixes, and requiredness when
moving manually maintained metadata into decorators.

Keep `ctx` as the first argument and leave output declarations and job metadata access unchanged.
Add timestamp defaults to the outer decorator (and `output_format` for single-file extractors),
then replace manual registration fields with `**extractor_function.registration_kwargs()`.
Keep `.run()` as the container entrypoint.

Migration can be incremental: mixed functions still run, but generated registration includes
**only decorated arguments**; no declarations yields empty input and parameter lists. Until every
required dependency is declared, retain complete manually supplied registration metadata. Bare
single-file decorators still run without an explicit format, but cannot generate registration
metadata until that format and timestamp defaults are supplied.
