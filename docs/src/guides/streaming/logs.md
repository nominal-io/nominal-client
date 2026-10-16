---
myst:
  html_meta:
    description: "Stream log entries into a dataset with buffered background uploads and context-managed shutdown."
---

# Stream logs in Python

{.lead}
Send timestamped log messages and structured arguments to a dataset with a log stream.

Use `get_log_stream()` when your application produces log entries or you want to parse
a different text format locally. The source can be a saved file or a live iterable;
streaming here refers to sending points through the API, not a requirement for a live source.


For journal-style JSON Lines files, use the
[Journal JSON guide](/guides/datasets/journal-json.md) to upload the file and poll
for ingestion instead. Both methods write to dataset log channels; neither requires
an asset or run.

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

## Parse the text format

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

## Enqueue the parsed points

```python
with dataset.get_log_stream() as stream:
    for point in parse_logs("engine.log"):
        stream.enqueue(
            channel_name="engine_logs",
            timestamp=point.timestamp,
            value=point.message,
            tags=point.args,
        )
print("Dataset:", dataset.nominal_url)
```

For a log stream, `value` is the message and `tags` becomes the entry's structured log
arguments. This preserves the parsed `level` argument. The channel name is supplied
on each `enqueue` call, so one stream can write to multiple log channels.

The stream buffers entries and sends batches in background threads. By default,
`batch_size` is 50,000 records and `max_wait` is `timedelta(seconds=1)`. Adjust these
arguments to `get_log_stream()` to balance batch size and buffering time.

Leaving the `with` block flushes buffered entries and waits for upload workers to
finish. If you manage the stream without a context manager, call `close()` when done.
Enqueueing is not a delivery acknowledgement: background upload failures are logged,
and closing the stream does not raise those worker errors. Check SDK error logs when
verifying delivery. Neither this stream nor `write_logs` returns a dataset-file handle
to poll, and neither uploads the original text file.

For a synchronous alternative, {py:meth}`~nominal.core.Dataset.write_logs` accepts the
same iterable of `LogPoint` objects:
`dataset.write_logs(parse_logs("engine.log"), channel_name="engine_logs")`.
It sends batches of 1,000 points by default and returns after sending them, with request
errors raised to the caller. Use it instead of the stream example when that behavior
fits your workflow. Both methods can send earlier batches before a later failure;
account for partial progress before retrying.

## View and organize the logs

Open the dataset in Nominal and use a workbook's **Logs** panel to inspect the
`engine_logs` channel alongside telemetry covering the same times.

Logs belong to the dataset. To organize them around a system, attach the dataset to
an [asset](/guides/assets/overview.md). To analyze a test interval, use a
[run](/guides/runs/overview.md) with either a direct dataset attachment or an associated
asset. These relationships reference the existing data rather than copying the logs.

For querying outside the app, see the [SQL interface](https://docs.nominal.io/developers/sql/overview).
