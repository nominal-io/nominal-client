---
myst:
  html_meta:
    description: "Ingest journal JSON files or stream log entries into a dataset, then view them with asset or run data."
---

# Logs in Python

{.lead}
Upload log files or stream log entries into a dataset's log channel.

Logs belong to a **dataset**, alongside its other channels. An asset can reference that
dataset, and a run can select a time interval using directly attached datasets or
associated assets. Neither an asset nor a run is required to write logs.

Choose the ingestion method that matches your input:

| Input | Method | Completion |
| --- | --- | --- |
| A journal-style JSON Lines file | {py:meth}`~nominal.core.Dataset.add_journal_json` | Returns a `DatasetFile`; poll it for ingestion completion. |
| Log entries produced by your application or parsed from another format | {py:meth}`~nominal.core.Dataset.write_logs` | Sends points synchronously in batches; returns no file handle. |

The examples below show **alternative ways to ingest the same synthetic log entries**.
Choose one, then optionally associate the dataset with an asset or run. Running both
examples writes the sample entries twice.

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
entries into `LogPoint` objects using the next workflow.

## Stream parsed log entries

Use `write_logs` when your application already produces log points or you want to parse
a different text format locally. The source can be a saved file or a live iterable;
streaming here refers to sending points through the API, not a requirement for a live source.

### Parse the text format

Save these synthetic entries as `engine.log` in your working directory:

```text
2026-09-10T14:38:19Z [INFO] Sent ACTUATE_MOTOR command
2026-09-10T14:38:20Z [WARN] Motor temperature above target
2026-09-10T14:38:21Z [INFO] Motor returned to target temperature
```

Each line has an ISO 8601 timestamp, a bracketed level, and a message. The `Z` suffix
means UTC. Adapt the parser to your actual log format; it expects all three fields
on every nonblank line. Missing fields or invalid timestamps raise an error.

```python
from typing import Iterable

from nominal.core import LogPoint


def parse_logs(file_path: str) -> Iterable[LogPoint]:
    with open(file_path, encoding="utf-8") as log_file:
        for line in log_file:
            if not line.strip():
                continue
            timestamp, level, message = line.rstrip("\r\n").split(maxsplit=2)
            yield LogPoint.create(
                timestamp=timestamp,
                message=message,
                args={"level": level.removeprefix("[").removesuffix("]")},
            )
```

{py:class}`~nominal.core.LogPoint` holds an absolute timestamp, a message, and structured
string arguments. This parser stores the severity in `args["level"]` and keeps the
rest of the message intact, including spaces. `LogPoint.create` also accepts a
timezone-aware datetime or integer nanoseconds since the Unix epoch. Pass `args=None`
when an entry has no structured arguments.

### Write the parsed points

```python
dataset.write_logs(parse_logs("engine.log"), channel_name="engine_logs")
print("Dataset:", dataset.nominal_url)
```

The parser yields entries as it reads the file. `write_logs` consumes that iterable
and sends batches of 1,000 entries by default; set `batch_size` to change the number
of entries per API request. The default channel name is `"logs"`; this example uses
`"engine_logs"` to identify the source.

The call blocks until all batches have been sent to the API. It returns `None`, not a
`DatasetFile` or an ingestion job, so there is no file handle to poll. This sends parsed
log entries; it does not upload or preserve the original text file as a dataset file.

Earlier batches may already have been sent if parsing or a later request fails.
The operation is not an all-or-nothing file upload; account for partial progress before
retrying a file.

## View the logs

Open the dataset in Nominal and use a workbook's **Logs** panel to inspect the
`engine_logs` channel. You can view it alongside telemetry that covers the same times.
If you work through an asset or run, attach or associate the dataset as described below,
then select its log channel in the workbook.

For querying outside the app, see the [SQL interface](https://docs.nominal.io/developers/sql/overview).

## Use the dataset with an asset

An asset's named data scope references the dataset. Attaching it does not copy the logs:

```python
asset = client.create_asset(name="Engine test stand")
asset.add_dataset(data_scope_name="engine", dataset=dataset)
```

For an existing asset, retrieve it with `client.get_asset("<ASSET_RID>")`. If the dataset
is already attached, you can keep writing logs to it without attaching it again.
See the [Assets overview](/guides/assets/overview.md) for managing data scopes.

## Use the dataset with a run

A standalone run can reference the dataset directly. This interval covers the sample
entries above; choose your actual test interval for your own recording.

```python
from datetime import datetime, timezone

run = client.create_run(
    name="Engine temperature check",
    start=datetime(2026, 9, 10, 14, 38, 19, tzinfo=timezone.utc),
    end=datetime(2026, 9, 10, 14, 38, 22, tzinfo=timezone.utc),
)
run.add_dataset(ref_name="engine", dataset=dataset)
```

If the dataset belongs to an asset, you can instead create an asset-associated run
for the test interval. That run uses the asset's data scopes; it does not require
another copy of the logs. See the [Runs overview](/guides/runs/overview.md) for the
standalone and asset-associated workflows.
