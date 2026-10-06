---
myst:
  html_meta:
    description: "Deprecated connections workflow for streaming data to Nominal's internal time series database."
---

# Use deprecated connections to stream data

{.lead}
Deprecated connections workflow for streaming data to Nominal's internal time series database.

:::{warning}

**Deprecated**: The connection workflow described below is deprecated but still supported for backward compatibility. 
Nominal recommends using [datasets](/guides/streaming/overview.md) for new implementations.
:::

:::{note}

Connections are objects that sit on top of an underlying database connected to Nominal (Influx, Timescale, etc). 
Connections are not the data source itself.

This section only covers connecting and streaming to Nominal's internal time series database.
This is the easiest and fastest way to stream data to Nominal.

To set up streaming to an external database connected to Nominal, please reach out to us in our shared 
Slack/Teams channel.
:::

## Create a connection to Nominal's internal time series database

:::{warning}

You only need to run the code below to create a connection once!

Moreover, you may not even need to create a new connection if you or a
colleague has already set one up. Check the [connections page](https://app.gov.nominal.io/data-sources?sidebar=allConnections)
for existing connections where `Type = "nominal"` and `Status = "Connected"`.
:::

To stream data to a Connection, first create a connection to Nominal's internal time series database. The
`datasource_id` must be unique within your organization, and `connection_name` is a friendly name that will appear on
the Nominal platform under [all connections](https://app.gov.nominal.io/data-sources?sidebar=allConnections).

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")

connection = client.create_streaming_connection(
    datasource_id="my-unique-datasource-id",
    connection_name="my-connection-name",
    datasource_description="my description",
    required_tag_names=["asset_id"],
)
```

:::{note}

You can access this connection later using its `rid`. You can find the `rid` in `connection.rid` of the code above
or look it up on the Nominal platform in the "Connections" section and use it in the `client.get_connection()` function
:::

### Configure required tags
You can configure required tag names for a streaming data source. Whenever users add this connection to a run or asset,
they will be required to specify a tag value for required tag names to down scope the data.

:::{note}

If data from multiple assets will be streamed to this datasource, the tag which is used to identify the asset should be
configured as required, so that the streaming datasource will always be properly down-scoped when added to an asset.
:::

## Stream data to the connection without blocking

The Nominal client provides a convenient way to write data to the connection without blocking your application during
network requests, which can be slow compared to your sample rate.

### Write a single data point

First, write a single data point to verify that it arrives correctly on the Nominal platform.

```python
from datetime import datetime

with connection.get_write_stream() as stream:
    now = datetime.now()
    stream.enqueue(channel_name="my_channel_name", timestamp=now, value=0.0)
```

In this example:
- `channel_name`: You can choose any name for your channel.
- `timestamp`: Can be a Python datetime object, a string in ISO format (e.g., "2024-12-31T15:00:00.001Z"), or a time in
nanoseconds since the Unix epoch.

### View the data point in Nominal

After executing this code snippet, create a run and a workbook to see the data point.

*Creating a run*
```python
run = client.create_run(name="my_run_name", start=now, end=None)
run.add_connection("some_ref", connection)
```

*Creating a workbook*

1. Navigate to the [Run section](https://app.gov.nominal.io/runs) on the Nominal platform.
2. Locate and select your run named "my_run_name".
3. Click on "New Workbook" on the top right-hand side of the page and select "New Empty Workbook".
4. In the left pane under "Channel Search", you should see "my_channel_name".
5. Drag and drop "my_channel_name" into the "Time Series Chart".
6. Click the play button at the top middle of the page to display the data point.

You should see the data point that written.

*Checking from a terminal instead*

You can also use the [SQL interface](https://docs.nominal.io/developers/sql/overview) to query the channel directly. Streamed points land in the connection's dataset and are queryable as soon as they arrive:

```sql
SELECT ts, value
FROM points_double
WHERE dataset_rid = '<dataset-rid>'
  AND channel = 'my_channel_name'
ORDER BY ts DESC
LIMIT 10
```

`ORDER BY ts DESC` puts the newest points first, so re-running the query is a quick way to watch data arrive. 

### Stream continuous data

Now stream more data points to this channel:
```python

from time import sleep

with connection.get_write_stream() as stream:
    for i in range(1000):
        now = datetime.now()
        value = random.random()
        print(f"Sending data point at {now}: {value}")
        stream.enqueue(channel_name="my_channel_name", timestamp=now, value=value)
        sleep(1)
```
After running this code, you can see new data points appearing in your workbook. To view the latest data points more
effectively, select "Last Minute" in the "Time History" dropdown at the top middle of the page.

## Stream multi-channel data to the connection

The stream allows for streaming multiple channels, with different intervals simultaneously.
```python
from datetime import timedelta

with connection.get_write_stream(max_wait=timedelta(seconds=2)) as stream:
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
        sleep(1)
```

After executing the code above, refresh your workbook in the browser. In the "Channel Search" section, you should now see "my_channel_name_2". Drag and drop it into the "Time Series Chart" and click the play button at the top middle of the page to view the data.

## Write string data

In addition to numeric time series, Nominal supports storing string values in channels. This feature can be especially useful for logging discrete states, operational modes, or textual annotations that accompany numeric telemetry.

To write string data, you follow the same steps as for numeric data; the only difference is that the value you enqueue is a string. For example, you might log a series of system "modes" or "states" over time:
```python
from time import sleep
from datetime import datetime

with connection.get_nominal_write_stream() as stream:
    # Cycle through three mode states repeatedly
    for value in ["mode A", "mode B", "mode C"] * 10:
        now = datetime.now()
        print(f"Sending data point at {now}: {value}")
        stream.enqueue(channel_name="my_string_channel_name", timestamp=now, value=value)
        sleep(3)
```

![Write string to stream](/guides/images/b4/write_stream_string_lfoixj.png)

## Add tags to filter data in a run or asset

When multiple assets write data to a connection, you can use tags to filter data within runs or assets. Tags are specified on the connection and referenced when adding the connection to a run or asset.

### Create a connection

Define a connection:

```python
connection = client.create_streaming_connection(
    datasource_id="my-unique-datasource-id",
    connection_name="my-connection-name",
)
```

### Filter data with tags in a run

When creating a run, specify the tags to filter the data on:

```python
run = client.create_run(name="my_run_name", start=now, end=None)
run.add_connection(
    "satellite",
    connection,
    series_tags={"satellite_id": "xc01"}
)
```

### Log data with tags

Log data to the connection using the specified tags:

```python
from datetime import timedelta

with connection.get_write_stream(max_wait=timedelta(seconds=2)) as stream:
    ...
    stream.enqueue(
        channel_name="temperature",
        timestamp=now,
        value=0.1,
        tags={"satellite_id": "xc01"}
    )
    stream.enqueue(
        channel_name="temperature",
        timestamp=now,
        value=0.2,
        tags={"satellite_id": "xc02"}
    )
```

When viewing the data in a workbook for the run, you will see that the temperature channel only has the value 0.1. The
value 0.2 will be in a run that has `series_tags={"satellite_id": "xc02"}`.
