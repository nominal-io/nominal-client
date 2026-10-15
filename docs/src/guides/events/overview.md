---
myst:
  html_meta:
    description: "Overview and recipes for working with Nominal's Event primitive in Python"
---

# Events in Nominal with Python

{.lead}
Overview and recipes for working with Nominal's Event primitive in Python

```{include} /guides/_snippets/install-warning.md
```

Events are time-based annotations that capture meaningful moments or intervals in test data.
You can create events manually during review or live monitoring. You can also create events automatically, either by applying checklists to runs or assets, or through external scripts that create events through the Nominal API or Python client.

## Connect to Nominal

```{include} /guides/_snippets/python/auth.md
```

## Create an Event

To create an Event, use {py:obj}`create_event() <nominal.core.NominalClient.create_event>`.
Provide a name, type, and start time. Duration defaults to zero for an instantaneous
event, and asset associations are optional. This example creates a ten-second event
on an asset. Replace `<ASSET_RID>` with an asset RID from your workspace; you can also
pass an Asset instance in `assets`.

```python
from datetime import datetime, timedelta, timezone

from nominal.core import EventType, NominalClient

client = NominalClient.from_profile("default")
asset_rid = "<ASSET_RID>"
event = client.create_event(
    name="Engine start",
    type=EventType.INFO,
    start=datetime.now(timezone.utc),
    duration=timedelta(seconds=10),
    assets=[asset_rid],
    properties={"test_phase": "burn"},
    labels=["reviewed", "critical"],
)
```

To retrieve an Asset's RID, visit its detail page and click on the clipboard icon next to "ID" in the right-hand drawer:

![run-metadata](/guides/images/b4/asset_metadata_drawer_srnsdh.png)

```{include} /guides/_snippets/what-is-a-rid.md
```

## Search Events

Use {py:obj}`search_events() <nominal.core.NominalClient.search_events>` to retrieve events that meet specific filter criteria. All filters are ANDed together, meaning an event must match every condition to be returned.

Common use cases include querying for:

- Events with specific labels or metadata
- Events within a time window
- Events tied to one or more assets
- Events created by a specific user

```python
from datetime import datetime, timedelta, timezone

from nominal.core import NominalClient

client = NominalClient.from_profile("default")

asset_rid = "<ASSET_RID>"  # Use the same asset as the creation example.
now = datetime.now(timezone.utc)
results = client.search_events(
    search_text="engine start",
    after=now - timedelta(days=1),
    before=now,
    assets=[asset_rid],
    labels=["reviewed", "critical"],
    properties={"test_phase": "burn"},
)
```
The example combines all these filters. Adjust them to match your data; omit a filter
to avoid restricting results by it. To filter by author, add `created_by="<USER_RID>"`
with a user RID from your workspace.

### Parameters
- `search_text` (optional): Text to search for in the event's metadata.
- `after` / `before` (optional): Find overlapping events: `after` matches events ending strictly after that time; `before` matches events starting strictly before that time. Use timezone-aware datetimes or integer epoch nanoseconds.
- `assets` (optional): Asset RIDs or instances that must all be present on the event.
- `labels` (optional): All listed labels must be present on the event.
- `properties` (optional): Key-value pairs that must all be present in the event’s metadata.
- `created_by` (optional): User RID or instance. Only returns events authored by this user.

### Returned Events

The `search_events()` method returns a list of `Event` objects. Each `Event` object has the following attributes:

- `name`: The name of the event.
- `start`: The start time as integer epoch nanoseconds.
- `duration`: The duration as integer nanoseconds.
- `asset_rids`: The RIDs of the assets associated with the event.
- `labels`: The labels associated with the event.
- `properties`: The properties associated with the event.
- `rid`: The RID of the event.
- `type`: The type of the event.

## Get an Event

To retrieve the event created above, use {py:obj}`get_event() <nominal.core.NominalClient.get_event>`.

```python
event = client.get_event(event.rid)
```

To retrieve an Event's RID, click on the event where it's visible on a workbook, and select "Copy RID" from the dropdown menu:

![copy-event-rid](/guides/images/b4/Screenshot_2025-07-08_at_2.03.19_PM.png)

You can also retrieve multiple events at once by passing a list of RIDs to `get_events()`:

```python
events = client.get_events(["<EVENT_RID_1>", "<EVENT_RID_2>"])
```

Replace the placeholders with event RIDs from your workspace. `get_events()` omits
RIDs that do not resolve, and returns events in backend order. Use `get_event()` when
a single event must exist; it raises if the event cannot be found.
