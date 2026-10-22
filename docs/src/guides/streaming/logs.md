---
myst:
  html_meta:
    description: "Send log messages with structured arguments to a dataset's log channel."
---

# Stream logs

{.lead}
A log stream sends timestamped messages with string arguments, such as a severity, to a log channel in a dataset.
Use it for logs your program produces or parses itself.

For journald exports and other JSON Lines files, [upload them](/guides/datasets/journal-json.md) instead.

```{include} /guides/_snippets/connect-the-client.md
```

## Parse log lines

{py:class}`~nominal.core.LogPoint` holds a timestamp, a message and string arguments.
This parser reads lines such as `2026-09-10T14:38:19Z [WARN] Motor temperature above target`:

```python
from collections.abc import Iterator

from nominal.core import LogPoint


def parse_logs(path: str) -> Iterator[LogPoint]:
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                timestamp, level, message = line.split(maxsplit=2)
                yield LogPoint.create(timestamp, message.rstrip(), args={"level": level.strip("[]")})
```

## Send the logs

{py:meth}`~nominal.core.Dataset.get_log_stream` batches entries in the background. Leaving the `with` block sends the rest.

```python
dataset = client.create_dataset("Engine test logs")
with dataset.get_log_stream() as stream:
    for point in parse_logs("engine.log"):
        stream.enqueue("engine_logs", point.timestamp, point.message, tags=point.args)
```

For a log stream, `tags` are the entry's arguments.
One stream can write to several log channels.

The stream logs upload failures rather than raising them, so check the client's logs.
To send synchronously and raise on errors, use {py:meth}`~nominal.core.Dataset.write_logs`:

```python
dataset.write_logs(parse_logs("engine.log"), channel_name="engine_logs")
```

Both can send some batches before one fails, so a retry may duplicate entries.

## View the logs

Add the dataset to an [asset](/guides/assets.md#add-data-with-data-scopes) or a [run](/guides/runs.md).
In a workbook on that asset or run, add a **Logs** panel and select the `engine_logs` channel.
