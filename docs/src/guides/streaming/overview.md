---
myst:
  html_meta:
    description: "Stream values to a dataset as they're produced, tune batching, and filter streamed data by tags."
---

# Stream data

{.lead}
A write stream sends values to a {term}`dataset` as your program produces them, batching them in the background.
Use it for live telemetry. For files, see [Datasets](/guides/datasets/overview.md).

For log messages, see [Stream logs](/guides/streaming/logs.md).

```{include} /guides/_snippets/connect-the-client.md
```

## Stream values

{py:meth}`~nominal.core.Dataset.get_write_stream` returns a stream. Leaving the `with` block sends what's left and closes it.

```python
import time
from datetime import datetime, timezone

from nominal.core import ChannelDataType

dataset = client.create_dataset("Motor test telemetry", prefix_tree_delimiter=".")
dataset.add_channel("motor.temperature", ChannelDataType.DOUBLE, unit="K")
dataset.add_channel("motor.mode", ChannelDataType.STRING)

start = datetime.now(timezone.utc)
with dataset.get_write_stream() as stream:
    for i in range(50):
        now = datetime.now(timezone.utc)
        for motor, baseline in [("left", 295.0), ("right", 297.0)]:
            stream.enqueue("motor.temperature", now, baseline + 0.1 * i, tags={"motor_id": motor})
            stream.enqueue("motor.mode", now, "idle" if i < 10 else "running", tags={"motor_id": motor})
        time.sleep(0.2)
end = datetime.now(timezone.utc)
```

Registering {term}`channels <channel>` first sets their types. See [Channels and data types](/guides/datasets/channels.md).
Timestamps can be timezone-aware datetimes, ISO 8601 strings or epoch nanoseconds.
Use the time each value was measured, not the time you enqueue it.

To write several channels at one timestamp, use `enqueue_from_dict`.
To write many points of one channel, use `enqueue_batch`.
Arrays and structs have `enqueue_float_array`, `enqueue_string_array` and `enqueue_struct`.

## Batching and delivery

The stream sends a batch when it reaches `batch_size` points, 250,000 by default, or after `max_wait`, 0.25 seconds by default.
Smaller values lower latency. Larger values lower request overhead.

`enqueue` doesn't confirm delivery, and there's no file to poll.
The two implementations report failures differently:

- **Rust**, the default where `nominal-streaming` is installed, can save failed batches to an `.avro` file with `file_fallback` for [later upload](/guides/datasets/avro-streams.md).
  It also supports `num_workers` and `track_metrics`.
- **Python** logs failed uploads and doesn't raise them when the stream closes.

Choose one with `implementation="rust"` or `implementation="python"`.
After streaming, check that the channels cover the expected time range.

## Add tags to filter data in a run or asset

Tags tell apart points in the same channel, such as the two motors above.
A {term}`run` or {term}`asset` can select one tag value with `series_tags`:

```python
run = client.create_run("Left motor test", start=start, end=end)
run.add_dataset("motor", dataset, series_tags={"motor_id": "left"})
```

Use tags for a small set of values, such as component, vehicle or test configuration.
Don't tag with values that change every point, such as a counter or a UUID per sample, because each tag combination adds query work.

## View streamed data

Open the dataset's asset or run in a {term}`workbook` and click **Go Live** to follow new points.
For a full walkthrough, see the [upload and stream tutorial](/guides/longstart.md#stream-data-to-the-asset).

```{video} /guides/images/b5/stream-gif.mp4
:loop:
:alt: Streamed data in a Nominal workbook
```

## Migrate from connections

`Connection` and `StreamingConnection` were removed. Stream to a dataset instead:

```python
dataset = client.get_dataset("<DATASET_RID>")
with dataset.get_write_stream() as stream:
    stream.enqueue("temperature", "2026-01-01T00:00:00Z", 42.0)
```

Get a migrated connection's dataset {term}`RID` from Nominal. A connection RID passed to `get_dataset` doesn't resolve.
External datasets read from their provider and don't accept streamed writes.
`nominal.experimental.stream_v2.create_write_stream()` now takes a `Dataset` as its first argument.
