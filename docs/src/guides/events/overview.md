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

Events are time-based annotations that capture meaningful moments or intervals within a run. 
You can create events manually during review or live monitoring. You can also create events automatically, either by applying checklists to runs or assets, or through external scripts that create events through the Nominal API or Python client.

## Connect to Nominal

```{include} /guides/_snippets/python/auth.md
```

## Create an Event

To create an Event, use {py:obj}`create_event() <nominal.core.NominalClient.create_event>`.
Provide a name, type, start time, and duration, and at least one resource ID ("RID") for an associated asset. You can also add properties and labels:

```{literalinclude} /guides/_snippets/code/sdk/python/events/create_event.py
:language: python
```

To retrieve an Asset's RID, visit its detail page and click on the clipboard icon next to "ID" in the right-hand drawer:

![run-metadata](/guides/images/b4/asset_metadata_drawer_srnsdh.png)

```{include} /guides/_snippets/what-is-a-rid.md
```

If any of the required parameters are missing, such as the asset RID, the event will not be created.

## Search Events

Use {py:obj}`search_events() <nominal.core.NominalClient.search_events>` to retrieve events that meet specific filter criteria. All filters are ANDed together, meaning an event must match every condition to be returned.

Common use cases include querying for:

- Events with specific labels or metadata
- Events within a time window
- Events tied to one or more assets
- Events created by a specific user

```{literalinclude} /guides/_snippets/code/sdk/python/events/search_events.py
:language: python
```
### Parameters
- `search_text` (optional): Substring to match against event metadata.
- `after` / `before` (optional): Filter events based on time. after matches events that end after this time. before matches events that start before this time.
- `assets` (optional): Asset RIDs or instances that must all be present on the event.
- `labels` (optional): All listed labels must be present on the event.
- `properties` (optional): Key-value string pairs that must all be present in the event’s metadata.
- `created_by` (optional): User RID or instance. Only returns events authored by this user.

### Returned Events

The `search_events()` method returns a list of `Event` objects. Each `Event` object has the following attributes:

- `name`: The name of the event.
- `start`: The start time of the event.
- `duration`: The duration of the event.
- `asset_rids`: The RIDs of the assets associated with the event.
- `labels`: The labels associated with the event.
- `properties`: The properties associated with the event.
- `rid`: The RID of the event.
- `type`: The type of the event.

## Get an Event

To retrieve an Event, use {py:obj}`get_event() <nominal.core.NominalClient.get_event>`.

```{literalinclude} /guides/_snippets/code/sdk/python/events/get_event.py
:language: python
```

To retrive an Event's RID, click on the event where it's visible on a workbook, and select "Copy RID" from the dropdown menu:

![copy-event-rid](/guides/images/b4/Screenshot_2025-07-08_at_2.03.19_PM.png)

You can also retrieve multiple events at once by passing a list of RIDs to `get_events()`:

```{literalinclude} /guides/_snippets/code/sdk/python/events/get_events.py
:language: python
```
