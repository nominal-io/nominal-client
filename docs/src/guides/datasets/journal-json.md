---
myst:
  html_meta:
    description: "Upload journal JSON log files to a dataset, configure timestamps, and wait for ingestion."
---

# Journal JSON logs in Python

{.lead}
Upload journal-style JSON Lines files into a dataset's log channel.

Use {py:meth}`~nominal.core.Dataset.add_journal_json` for file-based log ingestion.
It returns a `DatasetFile` that you can poll for completion. For application logs or
entries parsed locally, see [Stream logs](/guides/streaming/logs.md), which uses
`get_log_stream()` to send points in the background.

## Connect to Nominal

Install the SDK with `pip install nominal` and
[configure an authentication profile](/guides/authentication.md) before running the examples.

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")
dataset = client.create_dataset(name="Engine test logs")
```

To add logs to an existing dataset, use `client.get_dataset("<DATASET_RID>")` instead of
creating one. A dataset can contain both logs and telemetry; use the same absolute
clock for both so they line up during analysis.

## Upload a journal JSON file

Use this path for a saved journal-style log file, including JSON Lines exports from
journald or files your application writes in the same format. Save the following as
`engine.jsonl`, with one JSON object per line:

```json
{"__REALTIME_TIMESTAMP": "1789051099000000", "MESSAGE": "Sent ACTUATE_MOTOR command", "level": "INFO"}
{"__REALTIME_TIMESTAMP": "1789051100000000", "MESSAGE": "Motor temperature above target", "level": "WARN"}
{"__REALTIME_TIMESTAMP": "1789051101000000", "MESSAGE": "Motor returned to target temperature", "level": "INFO"}
```

These timestamps represent September 10, 2026 at 14:38:19–21 UTC. The format has these
requirements:

- The filename ends in `.jsonl` or `.jsonl.gz`; use one object per line, not a JSON array.
- `MESSAGE` contains the log message.
- `__REALTIME_TIMESTAMP` contains microseconds since the Unix epoch by default.
- Other top-level fields become log arguments, with values converted to strings. Here,
  `level` becomes a log argument; a journald `PRIORITY` field would retain that name.

Lines missing the message or timestamp field are skipped. Check that your file has
both fields before uploading; ingestion completion alone does not prove every input
line became a log entry.

```python
dataset_file = dataset.add_journal_json("engine.jsonl", channel="engine_logs")
dataset_file.poll_until_ingestion_completed()
print("Dataset:", dataset.nominal_url)
```

The upload starts asynchronous ingestion. Keep the returned file handle and wait for
it before inspecting its metadata or time bounds. See
[ingestion jobs](/guides/ingest/ingestion-jobs.md) for completion and error handling.

### Use a different timestamp field

If your file uses another field or epoch unit, supply **both** `timestamp_column` and
`timestamp_type`. For example, this alternative record uses milliseconds:

```json
{"timestamp_ms": 1789051099000, "MESSAGE": "Sent ACTUATE_MOTOR command", "level": "INFO"}
```

Save it as `engine-milliseconds.jsonl`, then upload it instead of the default-format
file above:

```python
dataset_file = dataset.add_journal_json(
    "engine-milliseconds.jsonl",
    channel="engine_logs",
    timestamp_column="timestamp_ms",
    timestamp_type="epoch_milliseconds",
)
dataset_file.poll_until_ingestion_completed()
```

This method accepts absolute epoch timestamps. ISO 8601 strings and relative times
are not supported by its timestamp override; convert them to epoch values or parse
entries into `LogPoint` objects using the [streaming logs guide](/guides/streaming/logs.md).

## View and organize the logs

Open the dataset in Nominal and use a workbook's **Logs** panel to inspect the
`engine_logs` channel alongside telemetry covering the same times.

Logs belong to the dataset. To organize them around a system, attach the dataset to
an [asset](/guides/assets/overview.md). To analyze a test interval, use a
[run](/guides/runs/overview.md) with either a direct dataset attachment or an associated
asset. These relationships reference the existing data rather than copying the logs.

For querying outside the app, see the [SQL interface](https://docs.nominal.io/developers/sql/overview).
