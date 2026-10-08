---
myst:
  html_meta:
    description: "Overview and recipes for working with Nominal datasets and streaming"
---

# Stream data

{.lead}
Overview and recipes for working with Nominal datasets and streaming

```{include} /guides/_snippets/install-warning.md
```

This guide covers additional patterns for streaming channel data to Nominal. For an end-to-end walkthrough, see the [Python Quickstart](/guides/quickstart.md#stream-data-to-nominal).

:::{tip}

Streaming is the likeliest place for a channel to be assigned the wrong data type.
[Pre-register the channel](/guides/datasets/channels.md) to pin it first.

If the link drops mid-test, [Avro stream files](/guides/datasets/avro-streams.md) carry
the same record shape as this API, so a local backup uploads afterwards without reshaping.
:::

```{video} /guides/images/b5/stream-gif.mp4
:loop:
:alt: Streaming data to Nominal from a Python client
```

## Create a dataset with additional metadata

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

# Create a dataset with labels, properties, and prefix tree delimiter
dataset = client.create_dataset(
    name="Sensor Data Collection",
    description="Real-time sensor data from multiple devices",
    labels=["sensors", "real-time", "production"],
    properties={
        "environment": "production",
        "data_type": "telemetry",
        "owner": "engineering-team"
    },
    prefix_tree_delimiter="."  # For hierarchical channel organization
)

dataset_rid = dataset.rid
print(f"Dataset RID: {dataset_rid}")
print(f"Dataset name: {dataset.name}")
print(f"Dataset labels: {dataset.labels}")
print(f"Dataset properties: {dataset.properties}")
```

## Stream multi-channel data to the dataset

The stream allows for streaming multiple channels, with different intervals simultaneously.
```python

from datetime import datetime, timedelta
from time import sleep

from nominal.core import NominalClient

client = NominalClient.from_profile("default")
dataset = client.get_dataset("<DATASET_RID>")

with dataset.get_write_stream(max_wait=timedelta(seconds=2)) as stream:
    for i in range(1000):
        now = datetime.now()
        value1 = random.random()
        msg = f"Sending data at {now}: channel1={value1}"

        stream.enqueue(channel_name="my_channel_name", timestamp=now, value=value1)
        if int(now.timestamp()) % 2 == 0:
            value2 = random.random()
            msg += f", channel2={value2}"
            stream.enqueue(channel_name="my_channel_name_2", timestamp=now, value=value2)
        print(msg)
        sleep(0.1)
```

After executing the code above, refresh your workbook in the browser. In the "Channel Search" section, you should now see "my_channel_name_2". Drag and drop it into the "Time Series Chart" and click the play button at the top middle of the page to view the data.

## Write string data

In addition to numeric time series, Nominal supports storing string values in channels. This feature can be especially useful for logging discrete states, operational modes, or textual annotations that accompany numeric telemetry.

To write string data, you follow the same steps as for numeric data; the only difference is that the value you enqueue is a string. For example, you might log a series of system "modes" or "states" over time:
```python
from datetime import datetime
from time import sleep

from nominal.core import NominalClient

client = NominalClient.from_profile("default")
dataset = client.get_dataset("<DATASET_RID>")

with dataset.get_write_stream() as stream:
    # We'll cycle through three mode states repeatedly
    for value in ["mode A", "mode B", "mode C"] * 10:
        now = datetime.now()
        print(f"Sending data point at {now}: {value}")
        stream.enqueue(channel_name="my_string_channel_name", timestamp=now, value=value)
        sleep(3)
```

![Write string to stream](/guides/images/b4/write_stream_string_lfoixj.png)

## Add tags to filter data in a run or asset

### When to use tags

Tags help you filter time series data when multiple sources write to the same dataset. Use tags for **low-cardinality identifiers** like:
- Asset or component IDs (`satellite_id`, `motor_id`)
- Test or simulation IDs (`sim_id`, `test_run_id`)
- Environment or configuration (`environment: sim`, `position: left`)

```{include} /guides/_snippets/tag-anti-patterns-warning.md
```

For more guidance on organizing your data model, see [Organizing your test data](https://docs.nominal.io/core/documentation/guides/organizing-test-data).

When multiple assets write data to the same dataset, you can use tags to filter data within runs or assets. Tags are specified when adding the dataset to a run or asset.

### Filter data with tags in a run

When creating a run, specify the tags to filter the data on:

```python
from datetime import datetime

from nominal.core import NominalClient

client = NominalClient.from_profile("default")
dataset = client.get_dataset("<DATASET_RID>")
now = datetime.now()

run = client.create_run(name="my_run_name", start=now, end=None)
run.add_dataset(
    "satellite",
    dataset,
    series_tags={"satellite_id": "xc01"}
)
```

### Log data with tags

Log data to the dataset using the specified tags:

```python
from datetime import datetime, timedelta

from nominal.core import NominalClient

client = NominalClient.from_profile("default")
dataset = client.get_dataset("<DATASET_RID>")

with dataset.get_write_stream(max_wait=timedelta(seconds=2)) as stream:
    stream.enqueue(
        channel_name="temperature",
        timestamp=datetime.now(),
        value=0.1,
        tags={"satellite_id": "xc01"}
    )
    stream.enqueue(
        channel_name="temperature",
        timestamp=datetime.now(),
        value=0.2,
        tags={"satellite_id": "xc02"}
    )
```

When viewing the data in a workbook for the run, you will see that the temperature channel only has the value 0.1. The value 0.2 will be in a run that has `series_tags={"satellite_id": "xc02"}`.

---

## Stream data using deprecated connections

The connection workflow is deprecated. Nominal recommends using datasets for new implementations.

To learn more how to stream data using connections, see the [Using connections](/guides/streaming/using-connections.md) guide.
