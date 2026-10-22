---
myst:
  html_meta:
    description: "Create, find and update events that mark moments and intervals in test data."
---

# Events

{.lead}
An event marks a moment or interval in test data that someone should see, such as an engine start or an out-of-limit reading.
Events can come from people reviewing data, from [checklists](/guides/checklists/authoring.md) or from your scripts.

```{include} /guides/_snippets/connect-the-client.md
```

## Create an event

Create an event on an [asset](/guides/assets.md) with {py:meth}`~nominal.core.Asset.create_event`.
A zero `duration`, the default, marks a single moment.

```python
from datetime import datetime, timedelta, timezone

from nominal.core import EventType

asset = client.get_asset("<ASSET_RID>")
event = asset.create_event(
    "Engine start",
    EventType.INFO,
    start=datetime.now(timezone.utc),
    duration=timedelta(seconds=10),
    properties={"test_phase": "burn"},
    labels=["critical"],
)
```

{py:class}`~nominal.core.EventType` is the event's level: `INFO`, `FLAG`, `ERROR` or `SUCCESS`.
To create an event on several assets, or on none, use {py:meth}`~nominal.core.NominalClient.create_event` with `assets=[...]`.
{py:meth}`~nominal.core.Run.create_event` creates one on a [run](/guides/runs.md).

## Find events

{py:meth}`~nominal.core.NominalClient.search_events` combines its filters with AND:

```python
now = datetime.now(timezone.utc)
events = client.search_events(
    after=now - timedelta(days=1),  # ends after this time
    before=now,  # starts before this time
    assets=[asset],
    labels=["critical"],
)
```

`after` and `before` find events that overlap the window.
The reference lists the other filters, such as `event_type`, `created_by` and `workbook`.

Retrieve events by RID with {py:meth}`~nominal.core.NominalClient.get_event`, which raises if the event doesn't exist.
{py:meth}`~nominal.core.NominalClient.get_events` takes several RIDs and leaves out any that don't resolve.
To copy an event's RID in the UI, click the event in a workbook and select **Copy RID**.

## Update or archive an event

{py:meth}`~nominal.core.Event.update` replaces each field you pass and leaves fields passed as `None` unchanged.
Its `type` argument is required, so pass `type=None` to keep the type:

```python
event.update(labels=[*event.labels, "reviewed"], type=None)
```

{py:meth}`~nominal.core.Event.archive` hides the event:

```python
event.archive()
```
