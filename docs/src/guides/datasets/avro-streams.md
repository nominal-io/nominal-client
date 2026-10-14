---
myst:
  html_meta:
    description: "When to use the Avro stream format, how to produce one, and what it can represent"
---

# Avro stream files

{.lead}
When to use the Avro stream format, how to produce one, and what it can represent

An Avro stream file carries data in the same shape as Nominal's
[streaming API](/guides/streaming/overview.md): each record names one channel, its own
timestamps, and the values at those timestamps. A tabular file wants every channel to share one
timestamp column. Avro streams do not, so each channel carries its own clock.

## When to reach for it

**Ragged or sparse data.** When channels sample at different rates or at unrelated moments, a wide
CSV or Parquet file is mostly empty cells, and every row still pays for every column. An Avro stream
carries only the samples that exist.

**Tags that vary per record.** A tabular file maps each tag to a column, so the tag keys are fixed
for the whole file and every row carries every one of them. An Avro record carries its own tag map,
so different records can use different tag keys entirely, and a tag that applies to one channel
costs nothing on the others.

**A backup when streaming drops.** The file shape matches the streaming API, so a system that
normally streams can write the same records to disk and upload them later with no reshaping.

### Avro streams or Parquet

Parquet is the better default for the shape it fits: columnar, so it compresses well and Nominal
can read a subset of channels without touching the rest, and the format most tooling already
produces.

Reach for an Avro stream when the data does not fit a rectangle:

| | Parquet | Avro stream |
| --- | --- | --- |
| Timestamps | one column shared by every channel in the file | per record, so channels need no common clock |
| Sparse channels | a cell per channel per row, empty or not | only the samples that exist |
| Tags | a column per tag, fixed for the file | a map on each record, free to differ |
| Layout | columnar, compresses and prunes well | record-oriented |

## Producing one

### Writing a file

`nominal-streaming` writes the format directly, using the same `enqueue` calls you would use to
stream. Streaming to a file needs no API credentials, and no schema of your own:

```{literalinclude} /guides/_snippets/code/sdk/python/datasets/avro_stream_write.py
:language: python
```

The result is snappy-compressed Avro. To read one back, any Avro reader works:

```python
from fastavro import reader

with open("flight_backup.avro", "rb") as f:
    for record in reader(f):
        record["channel"], record["timestamps"], record["values"], record["tags"]
```

### As a streaming fallback

Point a live stream at a local file and any batch that could not be sent lands there, so a dropped
link leaves a file to upload afterwards rather than a hole in the data:

```python
with (
    NominalDatasetStream(api_key, opts)
    .with_core_consumer("<DATASET_RID>")
    .with_file_fallback(pathlib.Path("flight_backup.avro")) as stream
):
    stream.enqueue("altitude", timestamp, 1250.5)
```

The `nominal` client exposes the same mechanism on its own write stream:

```python
with dataset.get_write_stream(
    implementation="rust",
    file_fallback="flight_backup.avro",
) as stream:
    stream.enqueue("altitude", timestamp, 1250.5)
```

:::{warning}

On `get_write_stream()`, `file_fallback` only takes effect with the Rust stream
(`implementation="rust"`, the default when `nominal-streaming` is installed), and the path must end
in `.avro`. With `implementation="python"` it has no effect beyond a logged warning, and a dropped
batch is lost.
:::

### Writing the schema by hand

Only needed outside Python, or with a tool that wants the schema declared. Records must match this
exactly, and **the order of the arms in the `values` union is part of what the backend validates**:

```json
{
  "type": "record",
  "name": "AvroStream",
  "namespace": "io.nominal.ingest",
  "fields": [
    {"name": "channel", "type": "string"},
    {"name": "timestamps", "type": {"type": "array", "items": "long"}},
    {"name": "values", "type": {"type": "array", "items": [
      "double",
      "long",
      "string",
      {"type": "record", "name": "DoubleArray",
       "fields": [{"name": "items", "type": {"type": "array", "items": "double"}}]},
      {"type": "record", "name": "StringArray",
       "fields": [{"name": "items", "type": {"type": "array", "items": "string"}}]},
      {"type": "record", "name": "JsonStruct",
       "fields": [{"name": "json", "type": "string"}]}
    ]}},
    {"name": "tags", "type": {"type": "map", "values": "string"}, "default": {}}
  ]
}
```

`timestamps` and `values` are parallel arrays, and `tags` is optional per record. A file whose
schema does not match fails ingestion with an invalid-schema error rather than ingesting partially.

The earlier version of this schema, whose `values` union held only `double` and `string`, is still
accepted.

## What the format can and cannot represent

**One arm per channel, for the whole file.** Every value for a given channel and tag set has to use
the same union arm. Mixing arms for one channel is a hard ingest error.

**The arm decides the channel's data type.** Emitting `long` for a channel that already exists as a
double-valued channel creates a second series rather than appending to the first. This is the same
hazard as inferred types, so [pre-register the channel](/guides/datasets/channels.md)
and pick the arm that matches.

**Several common types have no arm.** Booleans, datetimes, dates, durations, times, decimals and
categoricals are not in the union, so convert before writing: a boolean to `double` for a metric or
`string` for a label, a timestamp to integer nanoseconds, a decimal to `double` while accepting the
precision loss.

**Timestamps are integer nanoseconds.** The field is an array of `long`, so there is no string or
fractional form, and a value beyond the 64-bit range fails ingestion with a
timestamps-out-of-range error.

**Arrays are always float arrays.** There is no integer-array arm, so integer arrays are upcast.

### NaN and infinity

`NaN`, `Infinity` and `-Infinity` are ordinary `double` values here. They write and read back
intact, and `NaN` is the conventional way to encode a missing sample on a float channel, since the
`double` arm has no null.

The exception is inside a `JsonStruct`. JSON has no literal for either, so a struct containing
`NaN` or an infinity cannot be serialized, and a writer that tries will raise rather than emit an
unparseable file. Replace them with `null` before serializing the struct.

## Uploading

```python
dataset.add_avro_stream(
    "flight_backup.avro",
    timestamp_type="epoch_nanoseconds",
    tags={"vehicle_id": "sn-001"},
)
```

`timestamp_type` says how to read the numbers in the `timestamps` field, and must be a **numeric**
type: an epoch such as `epoch_nanoseconds` (the default), or a
[`ts.Relative`](/guides/datasets/overview.md#relative-timestamps) offset. ISO 8601 and
custom string formats have no numeric representation and raise a `ValueError`.

Tags passed here fill in keys a record does not already set; tags in the records themselves win.
Note the asymmetry with tabular ingest, which takes `tag_columns` to map a tag onto a column.
Avro records carry their tags directly, so there is nothing to map.

Avro streams are available wherever other formats are: `Asset.add_avro_stream()` and
`Run.add_avro_stream()` write to a data scope or ref name,
[`IngestBuilder.add_avro_stream()`](/guides/ingest/ingestion-jobs.md) includes one in a
batch, and a [file extractor](/guides/extractors/overview.md) can emit one with
`ctx.add_avro_stream()`.
