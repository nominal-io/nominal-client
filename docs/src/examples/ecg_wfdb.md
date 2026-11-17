---
icon: pulse
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# ECG from a heart monitor (WFDB)

Read a PhysioNet WFDB record (an ECG with beat labels) with `nominal.io`, explore it with the `df.nom` accessor,
and plot it with the Nominal plotly style. This example needs no Nominal profile; see
[Upload a DataFrame](upload_dataframe.md) for sending data to Nominal.

It needs the WFDB reader: `pip install nominal[wfdb,plotly]`.

The sample is the first minute of record 119 from the
[MIT-BIH Arrhythmia Database](https://physionet.org/content/mitdb/1.0.0/).

+++

## Load

A WFDB record is a header (`.hea`), the signals (`.dat`) and, often, annotations (`.atr`): beat labels
placed by cardiologists. Each ECG lead becomes a numeric channel, and the annotations become string
channels: `wfdb.beat` for beats and `wfdb.rhythm` for rhythm changes.

Many research records don't store when they were recorded. Then the reader warns and uses the current
time; pass `start=` to choose one, as here. Options like `start` pass through `nominal.io.read`.

A copy of the sample sits next to this notebook in `data/wfdb/`. If you downloaded only the notebook, the first cell
fetches the record from PhysioNet (this needs a network connection).

```{code-cell} ipython3
from pathlib import Path

import wfdb

record = Path("data/wfdb/119")
if not record.with_suffix(".hea").exists():
    record.parent.mkdir(parents=True, exist_ok=True)
    wfdb.dl_files("mitdb", str(record.parent), ["119.hea", "119.dat", "119.atr"])
```

The second cell reads the first minute (21,600 samples at 360 Hz).

```{code-cell} ipython3
import nominal.io
import nominal.pandas  # registers df.nom
import nominal.plotly

nominal.plotly.set_style()
df = nominal.io.read(
    "data/wfdb/119.hea", start="2026-01-01T12:00:00Z", sampto=60 * 360
)
print(df.nom.channels)
df.head()
```

## Explore

Beat labels: `N` is a normal beat, `V` a premature ventricular contraction (PVC).

```{code-cell} ipython3
beats = df.nom.select("wfdb.beat")
beats["value"].value_counts()
```

Rhythm changes: `N` is normal sinus rhythm, `B` ventricular bigeminy (every other beat is a PVC).

```{code-cell} ipython3
df.nom.select("wfdb.rhythm")[["timestamp", "value"]]
```

The heart rate comes from the time between beats.

```{code-cell} ipython3
rr = (
    beats["timestamp"].diff().dt.total_seconds()
)  # seconds between consecutive beats
print(f"mean heart rate: {60 / rr.mean():.0f} bpm")
```

## Plot

The first 10 seconds of lead MLII, with each beat marked by its label.

```{code-cell} ipython3
first = df.nom.first("10s")
wide = first.nom.select("wfdb.MLII").nom.wide()

fig = wide.plot(backend="plotly")
fig.update_yaxes(title="mV")
for label, group in first.nom.select("wfdb.beat").groupby("value"):
    fig.add_scatter(
        x=group["timestamp"],
        y=wide.loc[group["timestamp"], "wfdb.MLII"],
        mode="markers",
        marker_size=9,
        name=f"beat {label}",
    )
fig
```

Zoom into the first PVC, with both leads.

```{code-cell} ipython3
import pandas as pd

pvc = beats.loc[beats["value"] == "V", "timestamp"].iloc[0]
around = df.nom.between(pvc - pd.Timedelta("0.6s"), pvc + pd.Timedelta("0.8s"))

fig = (
    around.nom.select("wfdb.MLII", "wfdb.V1")
    .nom.wide()
    .plot(backend="plotly", facet_row="channel", height=420)
)
fig.update_yaxes(matches=None, title=None)
fig.add_vline(x=pvc, line_dash="dot", line_color="gray")
fig
```

## Get more data

[PhysioNet](https://physionet.org/about/database/) hosts many WFDB databases, such as `mitdb`
(arrhythmia), `nsrdb` (normal sinus rhythm), `afdb` (atrial fibrillation) and `ltafdb` (long-term AF).
Pass `pn_dir` to read a record straight from PhysioNet; `sampto` keeps the download short.

```python
more = nominal.io.read("100.hea", format="wfdb", pn_dir="mitdb", sampto=10 * 360, start="2026-01-01T12:00:00Z")
more.nom.select("wfdb.beat")["value"].value_counts()
```

To work offline, download a whole database and read its records locally:

```python
import wfdb

wfdb.dl_database("nsrdb", "data/nsrdb")
df = nominal.io.read("data/nsrdb/16265.hea")
```
