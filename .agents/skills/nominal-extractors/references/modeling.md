# Modeling decisions

The mechanics of writing an extractor are in `authoring.md`; this file covers the choices
that decide whether the resulting data is pleasant or painful to work with. These are the
decisions that are cheap now and expensive later: inputs and parameters are fixed at
registration, and timestamps and tags shape how every downstream query has to be written.

## Inputs vs parameters

Both arrive through the environment, and the distinction is about *kind*, not size:

- **Input** — a file the extractor reads. Nominal mounts it and puts its path in the
  declared environment variable. The ingest request supplies it as
  `sources={"ENV_VAR": path}`.
- **Parameter** — a scalar that tunes behavior, delivered as an environment-variable
  string. The ingest request supplies it as `arguments={"ENV_VAR": "value"}`.

Decide with two questions. *Is it content, or is it a knob?* A calibration table, a channel
map, a vendor schema sidecar — content, so make it an input even though it feels like
configuration. A sample-rate divisor, a mode flag, a quality threshold — a knob, so make it
a parameter. *Does it vary per ingest?* If it never varies, it belongs in the image itself,
baked into the code or a `COPY`'d file, not in either mechanism — every parameter you
register is a field somebody has to understand and fill in.

Two properties push structured configuration toward inputs specifically:

- Parameter values are strings with no schema. Anything with structure — a mapping, a list,
  a nested document — has to be encoded and parsed by hand, and a malformed value fails
  inside your parsing code with whatever message you happen to write.
- Inputs carry declared suffixes (`file_suffixes=["json"]`) saying what the input accepts —
  which is what `search_containerized_extractors(file_extension=...)` matches on, so they
  decide which extractors a given file is offered for — and, more importantly, a real
  client-side required check. The suffixes are descriptive, not enforced locally: passing a
  `.txt` to an input registered `["json"]` still uploads.

**`required=True` is much stronger on an input than on a parameter.** A missing required
input raises in `add_containerized` before anything uploads. A missing required parameter is
not checked client-side at all: the runtime logs a warning at container start, and the run
then fails at whatever moment `ctx.param()` reads it — after the upload, after the container
spun up, in the middle of the job. So for a parameter the code cannot proceed without,
either read it early (fail fast, at the top of the function) or give it a real default with
`ctx.get_param(name, default)` and skip the required flag.

Name the environment variables like the contract they are. The `name` field is what a person
sees in the app; the `environment_variable` is what the code reads, and changing it later is
a new registration plus an update to every caller's `sources`/`arguments`.

## Absolute vs relative time

Nominal places every sample on an absolute timeline, so the question is only how your
output encodes the position:

| Type | Encodes | Use when |
|---|---|---|
| `ts.Epoch(unit=...)` / `"epoch_nanoseconds"` etc. | numeric offset from the Unix epoch | the source records absolute time — the default choice |
| `ts.Relative(unit=..., start=...)` | numeric offset from an absolute `start` | the source records only elapsed time, and you can establish t0 |
| `ts.Iso8601` | absolute timestamp strings | the source carries formatted timestamps you don't want to parse |
| `ts.Custom(format=...)` | absolute strings in a `DateTimeFormatter` pattern | the same, in a non-ISO layout |

**Prefer absolute.** If the source has absolute time anywhere — per record, or a header
start plus per-record offsets you can add — convert once inside the extractor and emit
`Epoch`. Absolute output is self-describing: it survives re-registration, it doesn't depend
on metadata staying correctly paired with the file, and it can't be silently wrong.

Reach for `Relative` when the logger genuinely only knows elapsed time. Then t0 has to come
from somewhere, and where it comes from is the real design decision: a header inside the
file (best — the file stays self-contained), the filename, a registered parameter, or
`ctx.additional_tags`. Prefer reading it from the file, because a t0 passed in as a
parameter is a value someone has to get right on every single upload.

**Do not register `Relative` as an image's `default_timestamp_type`.** A default is baked in
once and applies to every future ingest, so a fixed `start` would assign the same t0 to
every file the extractor ever processes — correct for the first one and wrong for all the
rest. Register an absolute default, and declare `Relative` per output in the manifest, where
each file carries its own start. This is one of the concrete reasons manifest extractors are
the right default: single-file mode has nowhere to put per-file timestamp metadata.

On units: pick the one the data actually has, not a rounder one. Declaring
`epoch_milliseconds` for microsecond data doesn't just lose precision, it misplaces every
sample by a factor of a thousand. Per-output metadata in a manifest accepts only numeric
types (seconds through nanoseconds) — an output needing ISO 8601 or a custom format must
omit the per-output pair and inherit the job-level metadata.

## Tags

Tags are the dimensions that separate otherwise-identical channels. A channel is identified
by its name, so two test stands both producing `chamber_pressure` collide into one series
unless something distinguishes them — and that something is a tag. Get this wrong and the
data isn't lost, it's blended, which is harder to notice and harder to undo than a failed
ingest.

Three places tags can come from, in increasing order of specificity:

1. **The ingest request** — `dataset.add_containerized(..., tags={"vehicle": "n1234"})`.
   Applied uniformly to everything the run produces, and visible in-container as
   `ctx.additional_tags`. This is the right home for facts about *this upload* that the file
   itself doesn't know: which vehicle, which campaign, which operator.
2. **A tag column** — `ctx.add_tabular(path, tag_columns={"motor": "motor_id"})`. Reads the
   value per row from a column, so one file can carry data from several sources. Use this
   when the distinguishing fact varies *within* the file.
3. **Avro records**, which carry their own tags inline. This is why `add_avro_stream` takes
   no tag columns — there would be nothing to map.

Log outputs (`add_journal_json`) and videos (`add_video`) take neither: log samples carry no
tags and land on a single channel, and a video is identified by its `channel`.

**A tag column is consumed, not ingested.** Naming a column in `tag_columns` applies its
values as tags to that file's rows; the column does not also become a channel. So each
column is either a measurement you can plot or a dimension you can filter by, never both —
and if you need the same value in both roles, write it to the output twice under two names.
That makes tagging an authoring-time decision, not a registration-time one: you are
choosing, per column, which of the two things it becomes.

What makes a good tag is a fact that *identifies a source*, stays stable for the life of the
data, and has few distinct values: vehicle, stand, motor serial, run identifier, sensor
location. What makes a bad tag is a measurement (that's a channel), a timestamp or anything
derived from one (that's the timeline), a value that changes constantly (it fragments the
series into noise), or free text that varies by upload — the same concept spelled `Stand A`,
`stand-a`, and `standA` produces three unrelated series that nobody can join.

Two things worth settling before the first real ingest:

- **Fix a vocabulary and enforce it in the extractor.** Tag keys and values are strings
  chosen by whoever wrote the caller, and nothing normalizes them for you. If the extractor
  is the one deciding tag values, normalize there — lowercase, canonical separators,
  validated against a known set — rather than trusting each uploader.
- **Decide tag-versus-channel-prefix deliberately.** `channel_prefix` renames channels
  (`engine/chamber_pressure`); a tag leaves the name alone and adds a dimension. Prefixes
  are convenient when two outputs would otherwise collide by name and you want them visibly
  distinct; tags are better when you want to compare the same measurement across sources,
  because a tag can be filtered and grouped while a prefix has to be matched by string.

Both are painful to change once there is data: tag values are baked into every series
already ingested, so a renamed key or a re-spelled value doesn't migrate — it forks.
