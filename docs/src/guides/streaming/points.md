---
myst:
  html_meta:
    description: "Write measurements with explicit time or time-frequency coordinates."
---

# Write points with coordinates

{.lead}
Use the Nominal Python Client to write measurements with explicit coordinates.
Use an existing dataset on a deployment that supports the coordinate domain.

```{include} /guides/_snippets/connect-the-client.md
```

## Write a spectrum

{py:meth}`~nominal.core.Dataset.write_points` pairs coordinates with measurements by index.
Use {py:class}`~nominal.core.TimeFrequencyAxes` for frequency bins, repeating the capture
timestamp for each bin.

```python
from nominal.core import TimeFrequencyAxes

dataset = client.get_dataset("<dataset RID>")
# 2026-01-01T12:00:00.000000123Z
t0 = 1767268800000000123
dataset.write_points(
    "Pressure",
    axes=TimeFrequencyAxes(
        timestamps=[t0] * 3,
        frequencies_hz=[10.0, 20.0, 30.0],
    ),
    values=[0.25, 0.5, 0.125],
    unit="Pa",
    tags={"sensor": "inlet"},
)
```

For measurements indexed only by time, use {py:class}`~nominal.core.TimeAxes`.
Timestamps are integral UTC epoch nanoseconds. Coordinate and value columns must have
equal lengths.

The supplied domain must be compatible with the destination channel.
The unit labels the values without converting them. Supplying a unit can update an existing
channel's metadata asynchronously.

## Wait for acceptance

The call returns after the batch is accepted.
Storage completes asynchronously. Each call sends one batch; send larger inputs in
separate calls within the service's 50,000-point limit.

Failures before acceptance raise {py:class}`~nominal.exceptions.NominalError` or a subclass.
The client does not automatically replay failed writes. A lost response can leave
acceptance unknown, so repeating a call can duplicate data.
