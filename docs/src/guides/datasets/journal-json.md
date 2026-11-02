---
myst:
  html_meta:
    description: "Upload journal-style JSON Lines log files to a dataset's log channel."
---

# Journal JSON logs

{.lead}
Upload journald exports and other JSON Lines log files with {py:meth}`~nominal.core.Dataset.add_journal_json`.
Each line becomes one entry in a log {term}`channel`.

To send logs from a running program instead, see [Stream logs](/guides/streaming/logs.md).

```{include} /guides/_snippets/connect-the-client.md
```

## Upload a file

The file needs a `.jsonl` or `.jsonl.gz` name and one JSON object per line:

```json
{"__REALTIME_TIMESTAMP": "1789051099000000", "MESSAGE": "Sent ACTUATE_MOTOR command", "level": "INFO"}
{"__REALTIME_TIMESTAMP": "1789051100000000", "MESSAGE": "Motor temperature above target", "level": "WARN"}
```

- `MESSAGE` holds the log message.
- `__REALTIME_TIMESTAMP` holds microseconds since the Unix epoch.
- Every other field, here `level`, becomes a log argument with a string value.
- Lines without a message or timestamp are skipped.

```python
dataset = client.create_dataset("Engine test logs")
file = dataset.add_journal_json("engine.jsonl", channel="engine_logs")  # channel defaults to "logs"
file.poll_until_ingestion_completed()
```

## Use another timestamp field

Pass both `timestamp_column` and `timestamp_type` for a different field or epoch unit:

```python
dataset.add_journal_json(
    "engine-ms.jsonl",
    channel="engine_logs",
    timestamp_column="timestamp_ms",
    timestamp_type="epoch_milliseconds",
)
```

`timestamp_type` takes epoch units only.
For ISO 8601 or relative times, convert them to epoch values first, or parse the entries and [stream them](/guides/streaming/logs.md).

## View the logs

Add the {term}`dataset` to an [asset](/guides/assets.md#add-data-with-data-scopes) or a [run](/guides/runs.md).
In a {term}`workbook` on that asset or run, add a **Logs** panel and select the `engine_logs` channel.
