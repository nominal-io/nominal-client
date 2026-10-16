---
myst:
  html_meta:
    description: "Stream numeric and string channels to a dataset, manage buffering, and view tagged data in a run."
---

# Stream data

{.lead}
Stream timestamped telemetry to a dataset and view it in a run.

A dataset receives points through named channels. Use
{py:meth}`~nominal.core.DataSource.get_write_stream` to buffer those points and send
batches in the background. This guide creates a dataset, registers numeric and string
channels, sends a short synthetic recording, and attaches it to a run for inspection.
The Python blocks continue the same workflow.

For structured log messages and arguments, use [Stream logs](/guides/streaming/logs.md).
For uploading a saved streaming recording, see [Avro stream files](/guides/datasets/avro-streams.md).

## Create a dataset and register channels

Install `nominal` and [configure an authentication profile](/guides/authentication.md).
Then create the dataset that will receive the points:

```python
from nominal.core import ChannelDataType, NominalClient

client = NominalClient.from_profile("default")
dataset = client.create_dataset(
    name="Motor test telemetry",
    description="Synthetic temperature and operating-mode samples",
    labels=["example"],
    properties={"environment": "simulation"},
    prefix_tree_delimiter=".",
)

dataset.add_channel("motor.temperature", ChannelDataType.DOUBLE, unit="K")
dataset.add_channel("motor.mode", ChannelDataType.STRING)
print("Dataset:", dataset.nominal_url)
```

Register channels before sending data so their types reflect your intent. A numeric
measurement that can be fractional should use `DOUBLE`; an operating mode should use
`STRING`. The delimiter organizes dotted channel names into a hierarchy.
See [Channels and data types](/guides/datasets/channels.md) for registration details.

To continue an existing recording, retrieve its dataset with
`client.get_dataset("<DATASET_RID>")` instead of creating one. Registration with
`add_channel` leaves an existing channel unchanged; it does not repair a mismatched type.

## Send numeric and string samples

This example runs for roughly ten seconds. It samples temperature every iteration
and operating mode every fifth iteration, for two simulated motors. Both channels
use the same timestamp when sampled together.

```python
from datetime import datetime, timedelta, timezone
from time import sleep

start_time = datetime.now(timezone.utc)
with dataset.get_write_stream(max_wait=timedelta(seconds=1)) as stream:
    for sample_index in range(50):
        timestamp = datetime.now(timezone.utc)
        for motor_id, baseline in (("left", 295.0), ("right", 297.0)):
            tags = {"motor_id": motor_id}
            stream.enqueue(
                channel_name="motor.temperature",
                timestamp=timestamp,
                value=baseline + 0.1 * sample_index,
                tags=tags,
            )
            if sample_index % 5 == 0:
                stream.enqueue(
                    channel_name="motor.mode",
                    timestamp=timestamp,
                    value="idle" if sample_index < 10 else "running",
                    tags=tags,
                )
        sleep(0.2)
end_time = datetime.now(timezone.utc)
```

This sends 100 temperature points and 20 mode points. `sleep` paces the demonstration;
it is not a precision sampling clock. For real measurements, use their capture timestamps
rather than the time you happen to enqueue them. Timestamps can be timezone-aware
datetimes, ISO 8601 strings with an offset, or integer nanoseconds since the Unix epoch.

A string channel represents a value such as `"running"` at a time. Use a
[log stream](/guides/streaming/logs.md) when you need messages with structured arguments
such as severity, filename, or subsystem.

## Buffering, shutdown, and delivery

`enqueue` adds a point to the stream; it is not an acknowledgement that the point is
available to query. Batches are sent based on their size and buffering time:

| Setting | Default | Effect |
| --- | --- | --- |
| `batch_size` | 250,000 points | Limits the number of points grouped into a batch. Larger batches can reduce request overhead. |
| `max_wait` | 0.25 seconds | Controls how long a batch buffers before being flushed. The example uses one second. |

Buffering time is not an end-to-end delivery deadline. Network conditions and upload
work also affect when data becomes available. Tune batching against your desired
latency and throughput rather than increasing batch size automatically.

Use the `with` block to close the stream and drain pending work when the producer
finishes. If you manage the stream manually, call `close()` when done. There is no
`DatasetFile` to poll for streamed points. Inspect upload errors and verify the expected
channels and time range in Nominal before treating a recording as complete.

### Streaming implementations and recovery

By default, the SDK uses the Rust backend from `nominal-streaming` when it is available,
and otherwise uses the Python backend. You can select one explicitly with
`implementation="rust"` or `implementation="python"`.

Failure reporting depends on the backend. In particular, the Python backend logs
background upload failures rather than raising them when the context closes; a normal
exit alone does not establish successful delivery of every point.

The Rust backend supports `file_fallback="failed-batches.avro"` to save failed batches
locally. This is a recovery option, not a backup of every point. Use a writable `.avro`
path and retain any fallback file for later upload using the
[Avro stream guide](/guides/datasets/avro-streams.md). Supplying `file_fallback` requires
the Rust backend; the SDK raises if that backend is unavailable instead of silently
ignoring the option.

Rust also supports `num_workers` for upload concurrency and `track_metrics=True` for
request timing and point-staleness metrics. These are optional tuning tools; consult
{py:meth}`~nominal.core.DataSource.get_write_stream` for their behavior before changing them.

## Add tags to filter data in a run or asset

The sample wrote both motors into the same channels, distinguished by `motor_id`.
Create a run covering the recording and attach only the left motor's series:

```python
run = client.create_run(
    name="Left motor temperature test",
    start=start_time,
    end=end_time,
)
run.add_dataset(
    ref_name="motor",
    dataset=dataset,
    series_tags={"motor_id": "left"},
)
print("Run:", run.nominal_url)
```

The filter selects existing tagged series; it does not alter or delete the right
motor's data. An asset can similarly attach the dataset with `series_tags`; see the
[Assets overview](/guides/assets/overview.md). For asset-associated runs, see the
[Runs overview](/guides/runs/overview.md).

Keep tag combinations limited and meaningful: component, vehicle, environment, or test
configuration are useful examples. Avoid values that change every point, such as a
sequence number, timestamp, random UUID, or continuously changing measurement. Store
measurements in channels. Large numbers of distinct tag combinations increase query
work; consider how many combinations your identifiers will produce over time.

## View and verify the recording

Open the run in Nominal and create a workbook. Add `motor.temperature` to a time-series
chart and inspect `motor.mode` in a value grid over the run's interval. This example
has finished streaming, so select its recorded time range rather than looking only at
live data. The run's `motor` data scope should show the left motor's series.

Check that both channels contain data over the expected interval and that the mode
changes from `idle` to `running`. A visible point confirms some data arrived, not that
every expected sample did. For retrieving samples for further checks, see
[Export data](/guides/datasets/export-data.md).

```{video} /guides/images/b5/stream-gif.mp4
:loop:
:alt: Example of streaming data displayed in a Nominal workbook
```

For a longer walkthrough with continuously generated data and live workbook viewing,
continue to the [Python quickstart](/guides/quickstart.md#stream-data-to-nominal).
The [connection workflow](/guides/streaming/using-connections.md) is deprecated;
use datasets for new streaming integrations.
