---
icon: browser
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  name: python3
  display_name: Python 3 (ipykernel)
  language: python
---

+++ {"editable": true, "deletable": true, "slideshow": {"slide_type": ""}}

# Notebook Display

This notebook demonstrates the rich display available to Nominals core objects when used with Jupyter notebook, VS Code, or Colab. 


Sample data related to a helicopter is generated and used to propagate the model. 

The example needs the [installed Nominal Python client](/guides/install.md) and a `default` [profile](/guides/api-keys-setup.md).

+++ {"editable": true, "deletable": true, "slideshow": {"slide_type": ""}}

## Generate Sample Data

This examples demonstrate the typical lifecycle of  piece of test equipment,
called an [asset](/guides/assets.md). Let's imagine, for example, that we fire up the
rotor of a helicopter to measure its angular velocity.

```{code-cell} ipython3
---
editable: true
deletable: true
slideshow:
  slide_type: ''
---
# Start each execution with the rig registered, no runs or events, and two ingested datasets.
# The client cannot delete, so old resources are archived. The `example` property matches only the
# rig this example created, so the cleanup never touches another RTR-02 asset or its data.
# The page does not show this cell.
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

from nominal.core import EventType, NominalClient

client = NominalClient.from_profile("default")
rig = client.get_or_create_asset_by_properties(
    {"rig_id": "RTR-02", "example": "notebook_display"},
    name="Rotor test rig RTR-02",
    description="Tail rotor gearbox test stand",
    labels=["test-stand"],
)
for old_run in rig.list_runs():
    if not old_run.is_archived:
        old_run.archive()
for old_event in rig.search_events():
    old_event.archive()
old_datasets = rig.list_datasets()
rig.remove_data_scopes(names=[name for name, _ in old_datasets])
for _, old_dataset in old_datasets:
    old_dataset.archive()

# Three minutes at 10 Hz: spin up, hold near 31 rad/s (about 300 rpm), spin down.
t = np.arange(1800) / 10
speed = np.interp(t, [0, 30, 120, 150, 180], [0, 31.4, 31.4, 0, 0])
test_start = pd.Timestamp.now(tz="UTC").floor("s") - pd.Timedelta(minutes=10)
times = test_start + pd.to_timedelta(t, unit="s")
rng = np.random.default_rng(2)
tables = {
    "telemetry": pd.DataFrame(
        {
            "time": times,
            "rotor_speed": speed.round(3),
            # The gearbox warms with each rotor revolution and levels off above 70 °C.
            "gearbox_temp": (
                40 + 35 * (1 - np.exp(-np.cumsum(speed) / 15000))
            ).round(2),
        }
    ),
    "vibration": pd.DataFrame(
        {
            "time": times,
            "accel_rms": (0.02 * speed + rng.normal(0, 0.05, t.size)).round(4),
        }
    ),
}
units = {"rotor_speed": "rad/s", "gearbox_temp": "°C", "accel_rms": "g"}
for scope, table in tables.items():
    data_file = Path(tempfile.mkdtemp()) / f"rtr02_{scope}.csv"
    table.to_csv(data_file, index=False, date_format="%Y-%m-%dT%H:%M:%S.%fZ")
    dataset = client.create_dataset(
        f"RTR-02 {scope}", labels=[scope], properties={"sample_rate_hz": "10"}
    )
    rig.add_dataset(scope, dataset)
    dataset.add_tabular_data(
        data_file, timestamp_column="time", timestamp_type="iso_8601"
    ).poll_until_ingestion_completed()
    for channel in dataset.get_channels():
        channel.update(unit=units[channel.name])

seconds = pd.Timedelta(seconds=1)
for name, begin, end in [
    ("Spin-up", 0, 30),
    ("Hold at 300 rpm", 30, 120),
    ("Spin-down", 120, 150),
]:
    rig.create_run(
        f"RTR-02 {name}",
        start=test_start + begin * seconds,
        end=test_start + end * seconds,
        labels=["example"],
        properties={"test_bay": "Bay 3", "gearbox_sn": "GB-1187"},
        links=[
            {
                "url": "https://dev.nominal.io/quickstart/fundamentals/data-model/",
                "title": "Nominal data model",
            }
        ],
    )
rig.create_event(
    "Gearbox above 70 °C",
    EventType.FLAG,
    start=test_start + 108 * seconds,
    duration=72 * seconds,
    description="Gearbox temperature passed the 70 °C limit late in the hold.",
    labels=["example"],
);  # the cell shows no output
```

## Create the Nominal Client 
A {py:class}`~nominal.core.NominalClient` provides the link to the Nominal Platform.

```{code-cell} ipython3
from nominal.core import NominalClient

client = NominalClient.from_profile("default")
rig = client.search_assets(
    properties={"rig_id": "RTR-02", "example": "notebook_display"}
)[0]
```

## An asset

The rig's {py:class}`~nominal.core.Asset` card lists its datasets, by [data scope](/guides/assets.md#add-data-with-data-scopes),
and its runs, newest first.
Each name links to its page in Nominal, and its copy button copies its RID.
**Open in Nominal** opens the asset's own page:

```{code-cell} ipython3
rig
```

## Configuring the display

{py:obj}`~nominal.core.set_display_options` changes how every card looks,
from that cell on, and {py:obj}`~nominal.core.get_display_options` returns the current settings as {py:class}`~nominal.core.DisplayOptions`:

```{code-cell} ipython3
from nominal.core import get_display_options, set_display_options

get_display_options()
```

Timestamps show to the second by default. Hover over one to see it to the nanosecond.
For example, show timestamps in the test site's time zone, to the millisecond, in a more
readable format:

```{code-cell} ipython3
set_display_options(
    timezone="America/Denver", time_precision="ms", time_format="readable"
)
rig
```

Each card loads its related resources from Nominal, with a few requests.
To display cards faster, or offline, `fetch_details=False` shows only the fields
the resource already holds.
Used in a `with` block, the change lasts only until the block ends:

```{code-cell} ipython3
from IPython.display import display

with set_display_options(fetch_details=False):
    display(rig)
```

`style="text"` shows the plain text form instead of cards:

```{code-cell} ipython3
with set_display_options(style="text"):
    display(rig)
```

## A dataset

A {py:class}`~nominal.core.Dataset` card shows the time span of its data and lists its files and channels:

```{code-cell} ipython3
telemetry = rig.get_dataset("telemetry")
telemetry
```

## A channel

A numeric {py:class}`~nominal.core.Channel` card summarizes its data: the number of samples, the minimum,
maximum and mean, the first and last timestamps, and a chart of its range and mean over time:

```{code-cell} ipython3
telemetry.get_channel("gearbox_temp")
```

## A run

A {py:class}`~nominal.core.Run` card shows its number, time span and duration, and lists its assets, datasets and links:

```{code-cell} ipython3
hold = next(
    run
    for run in rig.list_runs()
    if run.name == "RTR-02 Hold at 300 rpm" and not run.is_archived
)
hold
```

## Files and events

Other resources have cards too, such as a dataset's files ({py:class}`~nominal.core.DatasetFile`)
and an asset's events ({py:class}`~nominal.core.Event`):

```{code-cell} ipython3
next(iter(telemetry.list_files()))
```

```{code-cell} ipython3
rig.search_events()[0]
```
