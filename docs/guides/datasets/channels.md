---
myst:
  html_meta:
    description: "Pre-register channels to pin their data type, units, and descriptions before data arrives"
---

# Channels and data types

{.lead}
Pre-register channels to pin their data type, units, and descriptions before data arrives

A {abbr}`channel (A named signal for a series of measurements or computed values (example: voltage, pressure, system state).)` gets its data
type from the first data Nominal sees for it. That inference is usually right, and usually
invisible. It fails when the values are ambiguous, and by then the channel already has the wrong
type.

Pre-registering a channel fixes its type up front, before any data arrives.

## When inference gets it wrong

**Whole numbers that are floats.** A channel whose first batch happens to be `0`, `1`, `2` is
inferred as an integer channel. Fractional values arriving later do not fit the type it already has.
This bites hardest on values that are usually round: a setpoint that sits at `0` until the test
starts, a percentage that reads `100` while idle, a rate that is whole until it is not.

**Numbers that are categories.** Fault codes, mode enumerations, part numbers and serial
numbers all look numeric. Inferred as numeric, they get plotted and interpolated as quantities, and
leading zeros are lost.

Both are more likely when streaming than when uploading a file, because a stream's first values are
whatever the system happened to emit at start-up, and a CSV at least shows a whole column at once.
Neither format inspects your intent.

## Pre-registering a channel

`add_channel()` creates the channel's metadata without uploading any data:

```{literalinclude} /guides/_snippets/code/sdk/python/datasets/channel_pre_register.py
:language: python
```

The data types are `DOUBLE`, `INT`, `UINT`, `STRING`, `LOG`, `DOUBLE_ARRAY`, `STRING_ARRAY`,
`STRUCT`, and `VIDEO`.

Pick `DOUBLE` for anything that could ever be fractional, even if today's values are whole. Pick
`STRING` for identifiers and codes you will filter and group by rather than do arithmetic on.

`add_channel()` raises if the channel already exists.

## Registering many channels at once

For a known instrumentation list, `batch_update_or_create_channels()` upserts in bulk. Channels that
already exist are updated rather than duplicated, so this is safe to re-run from a setup script:

```{literalinclude} /guides/_snippets/code/sdk/python/datasets/channel_pre_register_batch.py
:language: python
```

One caveat: a channel name repeated within a single batch fails the whole batch. Deduplicate before
calling.

The result reports which requests were upserted and which the server dropped, so check it rather
than assuming every request landed.

## Units and descriptions

Both can be set at registration, and changed later without touching the data.

`set_channel_units()` takes a mapping of channel name to unit symbol, and clears a unit when given
`None`. Pass `validate_schema=True` to raise on channel names that do not exist, so that a typo in
a mapping drawn from an instrumentation sheet fails instead of passing silently:

```python
dataset.set_channel_units({"altitude": "m", "airspeed": "m/s"}, validate_schema=True)
```

Units must come from Nominal's UCUM catalog unless you pass `allow_display_only_units=True`, which
accepts an arbitrary label that is displayed but not used for conversion.

## Inspecting what exists

`get_channels()` lists the channels on a data source, optionally filtered to specific names, and
`search_channels()` does exact or fuzzy matching and can filter by data type:

```python
for channel in dataset.search_channels(fuzzy_search_text="temp"):
    print(channel.name, channel.data_type, channel.unit)
```

Use these to confirm a type landed the way you intended before streaming production data into it.

## Grouping channels by prefix

`set_channel_prefix_tree()` tells the UI to group a data source's channels into a tree on a
delimiter, which makes a flat namespace of a few thousand channels navigable:

```python
dataset.set_channel_prefix_tree(delimiter=".")
```

Channels named `propulsion.motor.rpm` and `propulsion.motor.temp` then nest under `propulsion` and
`motor`. Several ingest paths accept a `channel_prefix` argument that writes names in this shape.
