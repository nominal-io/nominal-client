---
myst:
  html_meta:
    description: "Run a published checklist against an asset's live data and send alerts when checks fail."
---

# Trigger alerts on live data

{.lead}
A streaming checklist evaluates a published [checklist](/guides/checklists/authoring.md) against an asset's live data.
When a check fails, it can create an event and notify an integration such as Slack.

```{include} /guides/_snippets/connect-the-client.md
```

## Stream sample data to an asset

The checklist runs against an [asset](/guides/assets.md), so stream to a dataset on one.
This example sends a value that's `1` for 4 seconds of every 28:

```python
from datetime import datetime, timedelta, timezone

asset = client.create_asset("Streaming checklist demo")
dataset = asset.get_or_create_dataset("telemetry")

values = [1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] * 3
start = datetime.now(timezone.utc) - len(values) * timedelta(seconds=2)
with dataset.get_write_stream() as stream:
    stream.enqueue_batch("pump_on", [start + i * timedelta(seconds=2) for i in range(len(values))], values)
```

## Create the checklist

Author and publish the checklist [in Python](/guides/checklists/authoring.md), or in the UI:

1. Create a run over the sample data, so the UI can preview the check:

    ```python
    run = asset.create_run("Checklist preview", start=start, end=start + timedelta(hours=1))
    ```

1. Go to [Checklists](https://app.gov.nominal.io/checklists) and click **New checklist**.
1. Select the `Checklist preview` run.
1. Add a check that fails when `pump_on` is `>` `0`. The chart shows where it fails.
1. Click **Publish**, then copy the checklist's RID from the arrow (`⌄`) next to its name.

![Checklist preview](/guides/images/b4/checklist_preview_bwaqex.png)

## Start the checklist

{py:meth}`~nominal.core.Checklist.execute_streaming` starts the checklist on the asset.
Get an integration from the [Integrations page](https://app.gov.nominal.io/settings/org/integrations), or create one with a method such as {py:meth}`~nominal.core.NominalClient.create_webhook_integration`.

```python
checklist = client.get_checklist("<CHECKLIST_RID>")
checklist.execute_streaming(
    [asset],
    notifications=[client.get_integration("<INTEGRATION_RID>")],
    auto_create_events=True,  # create an event when a check fails, and update it on recovery
)
```

Returning means the request was accepted, not that evaluation started or a notification arrived.
Pass `notifications=[]` to evaluate without notifying.

:::{warning}
Calling `execute_streaming` again replaces all streaming settings for those assets, including notifications.
:::

{py:class}`~nominal.core.StreamingNotificationConfig` sets per-destination options:

```python
from nominal.core import StreamingNotificationConfig

notification = StreamingNotificationConfig(
    "<INTEGRATION_RID>",
    tags=["test-stand"],
    workbook="<WORKBOOK_RID>",  # link a workbook in the notification
    mute_execution_errors=True,
)
```

`mute_until` takes a timezone-aware datetime and silences that destination until then.
`integration_rids=[...]` is deprecated. Pass the same RIDs as `notifications=[...]`.

## Trigger alerts

Keep streaming, and each `1` fails the check:

```python
import time

with dataset.get_write_stream() as stream:
    for value in values * 100:
        stream.enqueue("pump_on", datetime.now(timezone.utc), value)
        time.sleep(2)
```

![Slack alert](/guides/images/b4/Slack_Live_Alert_Example.png)

## Monitor and change settings

{py:meth}`~nominal.core.Checklist.get_streaming_status` returns each check's state per asset:

```python
status = checklist.get_streaming_status([asset])[asset.rid]
print(status.state, status.commit_id)
for check in status.checks:
    print(check.check_rid, check.parameter_index, check.state)
```

A running checklist can have failing, invalid or skipped checks.
A parameterized check has a result per parameter, by `parameter_index`.

To change one setting, read the current ones with {py:meth}`~nominal.core.Checklist.get_streaming_configurations` and pass them back:

```python
config = checklist.get_streaming_configurations()[asset.rid]
checklist.execute_streaming(
    [asset],
    notifications=config.notifications,
    evaluation_delay=timedelta(seconds=5),
    recovery_delay=config.recovery_delay,
    auto_create_events=config.auto_create_events,
)
```

This isn't atomic, so coordinate with anyone else changing the asset's settings.

To stop, use {py:meth}`~nominal.core.Checklist.stop_streaming_for_assets`, or {py:meth}`~nominal.core.Checklist.stop_streaming` for every asset.
{py:meth}`~nominal.core.NominalClient.list_streaming_checklists` lists the checklists streaming on an asset.
