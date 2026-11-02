---
myst:
  html_meta:
    description: "Write and upload Avro stream files, where each channel has its own timestamps and tags."
---

# Avro streams

{.lead}
An Avro stream file holds records shaped like [streamed data](/guides/streaming/overview.md): each record names one {term}`channel`, with its own timestamps, values and {term}`tags <tag>`.
Upload one with {py:meth}`~nominal.core.Dataset.add_avro_stream`.

```{include} /guides/_snippets/connect-the-client.md
```

## When to use Avro streams

Use Parquet when every channel shares one timestamp column.
Use an Avro stream when they don't:

| | Parquet | Avro stream |
| --- | --- | --- |
| Timestamps | One column for every channel | Per record, so channels can sample at different times |
| Sparse channels | A cell for every channel in every row | Only the samples that exist |
| Tags | One column per tag, the same for the whole file | A map on each record |
| Layout | Columnar, compresses well | One record per channel batch |

Because the records match the streaming format, a program that streams can also write them to disk and upload them later.

## Write a file

`nominal_streaming`, installed with `nominal` on [supported platforms](/guides/install.md#extras), writes Avro streams with the same `enqueue` calls used to stream.
Writing to a file needs no {term}`API key`:

```python
from datetime import datetime, timezone
from pathlib import Path

from nominal_streaming import NominalDatasetStream

with NominalDatasetStream().to_file(Path("flight.avro")) as stream:
    stream.enqueue("altitude", datetime.now(timezone.utc), 1250.5, tags={"vehicle_id": "sn-001"})
    stream.enqueue_batch("airspeed", [1_757_500_000_000_000_000, 1_757_500_100_000_000_000], [42.0, 42.5])
    stream.enqueue_from_dict(1_757_500_200_000_000_000, {"altitude": 1252.0, "fault_code": "E07"})
```

Batch timestamps are epoch nanoseconds.

To keep data when a live stream loses its connection, pass `file_fallback` to {py:meth}`~nominal.core.Dataset.get_write_stream`.
Batches that fail to send are written to that `.avro` file instead:

```python
dataset = client.get_dataset("<DATASET_RID>")
with dataset.get_write_stream(implementation="rust", file_fallback="flight_backup.avro") as stream:
    stream.enqueue("altitude", datetime.now(timezone.utc), 1250.5)
```

`file_fallback` needs the Rust stream. The Python stream logs a warning and drops failed batches.

## Upload a file

```python
dataset = client.create_dataset("Flight telemetry")
file = dataset.add_avro_stream("flight.avro", tags={"vehicle_id": "sn-001"})
file.poll_until_ingestion_completed()
```

`timestamp_type` defaults to `"epoch_nanoseconds"`.
It takes numeric types only: another epoch unit, or {py:class}`ts.Relative <nominal.ts.Relative>`.
`tags` fill in keys that a record doesn't set. Tags in the records take precedence.

{py:class}`~nominal.core.Asset` and {py:class}`~nominal.core.Run` also have `add_avro_stream`, which uploads to a {term}`dataset` by {term}`data scope` name or {term}`ref name`.
To include an Avro stream in a batch, see [Ingest jobs](/guides/ingest/ingestion-jobs.md).

## Value types

Each value is a `double`, `long`, `string`, double array, string array or JSON struct.

- Every value for one channel and tag set must use the same type. Mixing them fails ingestion.
- The type sets the channel's data type. A `long` value for an existing `DOUBLE` channel creates a second series. [Register channels](/guides/datasets/channels.md) to avoid this.
- Booleans, datetimes and decimals have no type. Convert them first, for example a boolean to `double` or `string`.
- Integer arrays become double arrays.
- `NaN` and infinity are valid `double` values, and `NaN` is the usual way to mark a missing sample. JSON structs can't hold them, so replace them with `null` first.

## Schema

Writers in other languages need this schema.
The order of the types in the `values` union must match:

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

`timestamps` and `values` are parallel arrays of the same length.
A file that doesn't match the schema fails ingestion.
The earlier schema, with only `double` and `string` values, is still accepted.
