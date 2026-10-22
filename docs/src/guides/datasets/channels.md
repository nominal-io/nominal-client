---
myst:
  html_meta:
    description: "Register channels to set their data type, unit and description before data arrives."
---

# Channels and data types

{.lead}
A channel is one named signal in a dataset, such as `altitude` or `fault_code`.
Nominal sets a channel's data type from the first values it receives, unless you register the channel first.

```{include} /guides/_snippets/connect-the-client.md
```

## When to register channels

Register a channel when its first values could give it the wrong type:

- **Whole numbers that will become fractional.** A setpoint that reads `0` at start-up becomes an integer channel, and later values such as `0.5` don't fit it.
- **Codes that look like numbers.** Fault codes, modes and serial numbers become numeric channels, which lose leading zeros and chart as quantities.

Streams are more exposed than files, because their first values are whatever the system sends at start-up.

## Register channels

{py:meth}`~nominal.core.Dataset.add_channel` creates a channel without any data:

```python
from nominal.core import ChannelDataType

dataset = client.create_dataset("Frosty flight")
dataset.add_channel("altitude", ChannelDataType.DOUBLE, unit="m", description="Barometric altitude")
dataset.add_channel("fault_code", ChannelDataType.STRING)
```

If the channel already exists, `add_channel` returns it unchanged, even when the type, unit or description differ.

Use `DOUBLE` for any value that could be fractional, and `STRING` for codes you filter and group by.
{py:class}`~nominal.core.ChannelDataType` lists all types, including `LOG`, `STRUCT`, arrays and `VIDEO`.

To register many channels, such as from an instrumentation list, use {py:meth}`~nominal.core.Dataset.batch_update_or_create_channels`.
It updates channels that exist, so a setup script can run again:

```python
from nominal.core import CreateChannelRequest

dataset.batch_update_or_create_channels(
    [
        CreateChannelRequest("airspeed", ChannelDataType.DOUBLE, unit="m/s"),
        CreateChannelRequest("motor_rpm", ChannelDataType.INT, unit="/min"),
    ]
)
```

A name that appears twice fails the whole batch.
The result lists the requests the server dropped, so check it.

## Set units

{py:meth}`~nominal.core.Dataset.set_channel_units` sets units by channel name, and `None` clears a unit.
It changes the label, not the stored values.

```python
dataset.set_channel_units({"altitude": "m", "airspeed": "m/s"}, validate_schema=True)
```

`validate_schema=True` raises on names that aren't channels in the dataset. By default they're ignored.

Units are [UCUM](https://ucum.org/) symbols, such as `Cel` for degrees Celsius, so workbooks can convert between them.
Pass `allow_display_only_units=True` for labels outside UCUM, such as `encoder_ticks`. Those aren't converted.
To check a symbol, use {py:meth}`~nominal.core.NominalClient.get_unit` or {py:meth}`~nominal.core.NominalClient.get_commensurable_units`.

To change one channel, use {py:meth}`~nominal.core.Channel.update`.

## Find channels

{py:meth}`~nominal.core.Dataset.search_channels` matches names exactly or fuzzily:

```python
for channel in dataset.search_channels(fuzzy_search_text="temp"):
    print(channel.name, channel.data_type, channel.unit)
```

## Group channels by prefix

{py:meth}`~nominal.core.Dataset.set_channel_prefix_tree` groups channels in the UI by a delimiter in their names:

```python
dataset.set_channel_prefix_tree(delimiter=".")
```

`propulsion.motor.rpm` and `propulsion.motor.temp` then appear under `propulsion` → `motor`.
