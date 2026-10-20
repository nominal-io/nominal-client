---
myst:
  html_meta:
    description: "Configure and monitor published checklists against live asset data with Python."
---

# Trigger alerts on live data

{.lead}
Configure and monitor published checklists against live asset data with Python.

## Overview

Nominal's streaming checklists allow you to continuously monitor real-time data on an asset and trigger alerts
whenever certain conditions are met. This guide walks you through the process of setting up and running a streaming
checklist in Python using the Nominal client.

To run a streaming checklist, you need the following:
- A Dataset.
- An asset.
- A Run off the asset (needed to create a Checklist in the web GUI as a preview)
- A Checklist.
- (Optional) An integration for sending notifications.

### Prerequisites

Make sure you have the Nominal Python packages installed. You can install it using:
```shell
pip3 install nominal
```

#### Connect to Nominal

```{include} /guides/_snippets/python/auth.md
```

### Create a Nominal Dataset

:::{warning}

You only need to run the code below to create a dataset once!

Moreover, you may not even need to create a new dataset if you or a
colleague has already set one up. Check the [Datasets page](https://app.gov.nominal.io/data-sources?sidebar=allDatasets)
for existing Datasets.
:::

To stream data, first create a Dataset in Nominal:
```python
dataset = client.create_dataset(
    name="my-unique-dataset-name",
)
```

:::{note}

You can access this dataset later using its `rid`. You can find the `rid` in `dataset.rid` of the code above
or look it up on the Nominal platform in the "Dataset" section and use it in the client.get_dataset(<dataset_rid />) function
:::

### Create an asset for Streaming Test Data

Create an asset, associate it with the dataset, and then stream some initial data:
```python
from datetime import datetime, timedelta

values = [1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] * 3
interval = timedelta(seconds=2)
start = datetime.now() - len(values) * interval
timestamps = [start + i * interval for i in range(len(values))]

asset = client.create_asset(
    name = 'streaming_checklist_asset'
)
asset.add_dataset("my_refname", dataset)

with dataset.get_write_stream(batch_size=len(values)) as stream:
    stream.enqueue_batch(channel_name="my_channel_name", timestamps=timestamps, values=values)
```

### Create run to preview in Checklist

Use {py:meth}`~nominal.core.Asset.create_run` to create a temporary run around the data
just streamed to the asset and preview it in a checklist:
```python
temp_run = asset.create_run(
    name = 'temporary_run',
    start = start,
    end = start + timedelta(hours=1),
)
```

### Create the checklist in the Nominal web UI

![Checklist preview](/guides/images/b4/checklist_preview_bwaqex.png)

1. Go to the [Checklist Page](https://app.gov.nominal.io/checklists).
2. Click on `New checklist`.
3. Select the 'temporary_run' you created above.
4. Enter a name for the first check and a priority and click `Add check`
5. Define a check:
    - In the field next to `when`, select `my_channel_name`.
    - in the next field select `>`.
6. you should now see the violations in the chart. Zoom in if necessary.
7. click on `Publish` in the top-right corner.
8. From the dropdown next to the checklist name (`⌄`), copy the rid of the checklist.
9. Paste the rid in the code below.

```python
checklist_rid = "paste_rid_here"
```

### Select an integration for notifications (optional)

Fetch an existing integration from the [Integrations page](https://app.gov.nominal.io/settings/org/integrations),
or use the client's integration creation methods. Slack destinations are authorized in the UI.

```python
integration = client.get_integration("integration-rid")
checklist = client.get_checklist(checklist_rid)
```

### Execute the streaming checklist

Pass integrations or their RIDs directly for default notification settings:

```python
checklist.execute_streaming(
    [asset],
    notifications=[integration, "another-integration-rid"],
    auto_create_events=True,
)
```

The recommended example enables automatic events so failing checks create events and recovery updates them.
The API defaults to `auto_create_events=False`; omit the argument or pass `False` to monitor without creating events.

Wrap a destination to configure tags, a workbook link, execution-error suppression, or a mute deadline:

```python
from nominal.core import StreamingNotificationConfig

checklist.execute_streaming(
    assets=[asset],
    notifications=[
        StreamingNotificationConfig(
            integration,
            tags=["test-stand"],
            mute_execution_errors=True,
        )
    ],
    auto_create_events=True,
)
```

Set `workbook="workbook-rid"` to include a workbook link in failure and recovery notifications.
Set `mute_until` to a timezone-aware `datetime` to temporarily suppress all notifications for a destination.

To execute without notifications, pass `notifications=[]`. Existing calls using
`integration_rids=["integration-rid"]` remain supported but emit a deprecation warning.
Replace that argument with `notifications=["integration-rid"]`. Positional integration lists should
also move to the keyword-only `notifications=` argument. Supply only one of these arguments.

:::{warning}
Executing again **replaces all streaming settings for the specified assets**, including notification destinations.
Omitting notifications disables them. Settings for other assets are unchanged.
:::

Returning from `execute_streaming` means the request was accepted. It does not mean evaluation has started
or confirm that notifications have been delivered.

## Monitoring and managing streaming checklists

Inspect the executing commit and individual check results:

```python
status = checklist.get_streaming_status([asset])[asset.rid]
print(status.state, status.commit_id)
for check in status.checks:
    print(check.check_rid, check.parameter_index, check.state)
```

A running checklist can contain failing, invalid, or skipped checks. Parameterized checks have separate results
identified by their zero-based `parameter_index`.
Live status is available only for assets configured to stream the checklist.
All public state enums use lowercase string values; prefer comparing enum members. Unknown lifecycle or
check-status variants fail in the generated decoder with `ValueError` and require an SDK upgrade.

Read existing settings before changing them. For example, increase the evaluation delay while keeping the other settings:

```python
from datetime import timedelta

configuration = checklist.get_streaming_configurations()[asset.rid]
checklist.execute_streaming(
    [asset],
    notifications=configuration.notifications,
    evaluation_delay=timedelta(seconds=5),
    recovery_delay=configuration.recovery_delay,
    auto_create_events=configuration.auto_create_events,
)
```

Readback contains integration and workbook RIDs, and delays in integer nanoseconds. Both nanoseconds and
`timedelta` are accepted when configuring execution. Mute deadlines use Python datetime's microsecond precision.
Reading and reapplying settings is not an atomic update; coordinate with other users changing the same asset's settings.

List streaming checklist RIDs or stop execution:

```python
client.list_streaming_checklists(asset=asset)
checklist.stop_streaming_for_assets([asset])
# Or stop execution on every asset:
checklist.stop_streaming()
```

### Stream more data to trigger notifications

Now that your streaming checklist is active, send more data to trigger notifications. The following code continuously
sends data points every 2 seconds. When conditions are met, notifications will be sent to the configured integration.

```python
from datetime import datetime, timedelta
from time import sleep

values = [1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
interval = timedelta(seconds=2)

with dataset.get_write_stream() as stream:
    for value in values * 100:
        now = datetime.now()
        print(f"Sending data point at {now}: {value}")
        stream.enqueue(channel_name="my_channel_name", timestamp=now, value=value)
        sleep(2)
```

Example Alert (Slack):
You should now receive real-time notifications through your chosen integration. For Slack, the messages might look like this:
![slack integration](/guides/images/b4/Slack_Live_Alert_Example.png)
