---
myst:
  html_meta:
    description: "Overview and recipes for working with Nominal streaming checklists in"
---

# Trigger alerts on live data

{.lead}
Overview and recipes for working with Nominal streaming checklists in

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

Now create a temporary run around the data just streamed to the asset, to preview in a checklist:
```python
temp_run = client.create_run(
    name = 'temporary_run',
    start = start,
    end = start + timedelta(hours=1),
    assets=[asset],
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

Notification integrations are optional. You can run a streaming checklist without configuring any notification integrations to track checklist status and generate events without sending alerts.

This is useful when you want to:

- Monitor check results through the workbook event panel without triggering external alerts.
- Run checklists in observation mode during early testing before configuring production notification channels.
- Create events from streaming checklists for historical analysis without real-time notifications.

To skip notifications, pass an empty list for `integration_rids` when calling `execute_streaming` (see the code example below).

If you want to receive alerts through Slack, Teams, PagerDuty, or other channels, find available integrations in the [Integrations Page](https://app.gov.nominal.io/settings/org/integrations), copy the RID, and paste it in the code below.

```python
integration_rid = "paste_rid_here"
```

![integrations](/guides/images/b4/integrations_nn0y0c.png)

### Execute the streaming checklist

With the checklist and asset ready, you can now execute the streaming checklist.

With notifications:
```python
checklist = client.get_checklist(checklist_rid)

checklist.execute_streaming(
    assets=[asset],
    integration_rids=[integration_rid],
)
```

Without notifications (status tracking only):
```python
checklist = client.get_checklist(checklist_rid)

checklist.execute_streaming(
    assets=[asset],
    integration_rids=[],
)
```

## Monitoring and managing streaming checklists

- List running streaming checklists:
```python
list(client.list_streaming_checklists())
```

- Stop a running streaming checklist:
```python
checklist.stop_streaming()
```

### Stream more data to trigger notifications

Now that your streaming checklist is active, send more data to trigger notifications. The following code continuously
sends data points every 2 seconds. When conditions are met, notifications will be sent to the configured integration.

```python
from datetime import datetime, timedelta
from time import sleep

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
