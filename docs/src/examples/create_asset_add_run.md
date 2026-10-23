---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  language: python
  name: python3
icon: chevron-right
---

# A Typical Experimental Run

In this example, we demonstrate the typical lifecycle of an
experimental [run](/guides/runs.md). Such a run involves a piece of test equipment,
called an [asset](/guides/assets.md). Let's imagine, for example, that we fire up the
rotor of a helicopter to measure its angular velocity.

Below, we show how to register the helicopter as an asset; this is only ever done once.

Then, we measure rotor data, and upload a [dataset](/guides/datasets/overview.md) to the helicopter asset.

The example needs the [installed Nominal Python client](/guides/install.md) and a `default` [profile](/guides/api-keys-setup.md).

## Registering the asset

{py:meth}`~nominal.core.NominalClient.create_asset` registers the helicopter as an {py:class}`~nominal.core.Asset`.
Properties identify the physical aircraft, such as its tail number:

```python
from nominal.core import NominalClient

client = NominalClient.from_profile("default")
helicopter = client.create_asset(
    "Helicopter N123HX",
    description="Light utility helicopter",
    properties={"tail_number": "N123HX"},
)
```

## Adding a dataset to the asset

The helicopter keeps its data in a {py:class}`~nominal.core.Dataset`.
An asset holds each dataset under a name, its [data scope](/guides/assets.md#add-data-with-data-scopes).
Workbook templates and checklists refer to data by this name.
Give the same kind of data the same name on every asset, here `telemetry`, so one template works for every helicopter.
{py:meth}`~nominal.core.NominalClient.create_dataset` creates an empty dataset, and {py:meth}`~nominal.core.Asset.add_dataset` attaches it under that name.
This is also done only once:

```python
helicopter.add_dataset(
    "telemetry",
    client.create_dataset("Helicopter N123HX telemetry")
)
```

Each later test finds the registered helicopter by its properties with {py:meth}`~nominal.core.NominalClient.search_assets`:

```{code-cell} python
:tags: [remove-cell]

# Start each execution with the helicopter registered, no runs and an empty dataset.
# The client cannot delete, so old runs and datasets are archived. The page does not show this cell.
from nominal.core import NominalClient

client = NominalClient.from_profile("default")
helicopter = client.get_or_create_asset_by_properties(
    {"tail_number": "N123HX"},
    name="Helicopter N123HX",
    description="Light utility helicopter",
)
for old_run in helicopter.list_runs():
    old_run.archive()
old_datasets = helicopter.list_datasets()
helicopter.remove_data_scopes(names=[name for name, _ in old_datasets])
for _, old_dataset in old_datasets:
    old_dataset.archive()
helicopter.add_dataset("telemetry", client.create_dataset("Helicopter N123HX telemetry"))
```

```{code-cell} python
from nominal.core import Asset, NominalClient

client = NominalClient.from_profile("default")
helicopter: Asset = client.search_assets(properties={"tail_number": "N123HX"})[0]
print(helicopter.nominal_url)
```

## Preparing the simulated data

Next, we generate an angular velocity curve for a helicopter
rotor: it spins up, holds near 31 rad/s (about 300 rpm), and spins
down at t = 45 s. This synthetic curve is for illustration only; in a
real experiment, the data would come from sensors.

```{code-cell} python
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

t = np.arange(600) / 10  # one minute at 10 Hz, in seconds
speed = 31.4 * (1 - np.exp(-t / 5)) + 0.3 * np.sin(2 * np.pi * t / 4)
speed = np.where(t > 45, speed * np.exp(-(t - 45) / 4), speed).clip(min=0)

test_start = pd.Timestamp.now(tz="UTC").floor("s") - pd.Timedelta(minutes=5)
data = pd.DataFrame({
    "time": test_start + pd.to_timedelta(t, unit="s"),
    "rotor_speed": speed.round(3)
})

data_file = Path(tempfile.mkdtemp()) / "rotor_spin_up.csv"
data.to_csv(data_file, index=False, date_format="%Y-%m-%dT%H:%M:%S.%fZ")
```

## Start the run, upload some data

We now commence our test run. During the run, we "measure" the data simulated above, and upload it to the helicopter.

```{code-cell} python
dataset = helicopter.get_dataset("telemetry")
file = dataset.add_tabular_data(
    data_file,
    timestamp_column="time",
    timestamp_type="iso_8601"
)
file.poll_until_ingestion_completed()  # wait until done
```

The helicopter's `telemetry` dataset collects the data of every test ever recorded on it.
A {py:class}`~nominal.core.Run` selects one test from that data: it is a time slice out of all data on the asset.

After ingestion, the file's `bounds` hold its first and last timestamps, in nanoseconds since the Unix epoch.
{py:meth}`~nominal.core.Asset.create_run` uses them to set the run's start and end:

```{code-cell} python
run = helicopter.create_run(
    "Rotor spin-up",
    start=file.bounds.start,
    end=file.bounds.end,
    labels=["example"],
)
print(run.nominal_url)
```

## Inspect the data

We may now be curious to inspect the data we uploaded. This is perhaps easiest done using a [workbook](/guides/workbooks.md) in the Nominal platform, but we can also plot it using matplotlib.

An analysis usually starts from a run, retrieved by the RID on its page in Nominal.
{py:meth}`~nominal.core.Run.list_assets` returns the run's asset, and {py:meth}`~nominal.core.Asset.get_dataset` returns its `telemetry` dataset.
The dataset holds every test on the helicopter, so read it between the run's start and end to get this test only.
{py:func}`~nominal.thirdparty.pandas.datasource_to_dataframe` downloads that slice of the `rotor_speed` channel into a pandas DataFrame:

```{code-cell} python
from nominal.thirdparty.pandas import datasource_to_dataframe

run = client.get_run(run.rid)  # in practice, the RID from the run's page
helicopter: Asset = run.list_assets()[0]
telemetry = helicopter.get_dataset("telemetry")
df = datasource_to_dataframe(telemetry, channel_exact_match=["rotor_speed"], start=run.start, end=run.end)
ax = df.plot(y="rotor_speed", legend=False, title=run.name)
ax.set_ylabel("Angular velocity (rad/s)");
```
