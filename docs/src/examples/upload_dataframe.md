---
icon: upload
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# Upload a DataFrame to Nominal

Get a pandas frame (here built by hand; `nominal.io.read(path)` gives you one from a file), then send it to Nominal with `df.nom.to_nominal()` and open a workbook on it.
This needs the [installed Nominal Python Client](/guides/install.md) with `nominal[avro]`, and a
[profile](/guides/api-keys-setup.md).

+++

## One shot

```{code-cell} ipython3
:tags: [remove-cell]

# Start each run from a fresh asset: archive the one an earlier run made, so reruns don't pile up
# duplicate data or workbooks. The page does not show this cell.
import nominal.config

for old in nominal.config.get_client().search_assets(
    exact_substring="nominal.io upload example"
):
    if old.name == "nominal.io upload example":
        old.archive()
```

```{code-cell} ipython3
import numpy as np

import nominal.io
import nominal.pandas  # registers df.nom
from nominal.io import ChunkBuilder

# A small demo frame: a sine wave and a state channel, 100 Hz for two seconds.
n = 200
times = 1_760_000_000_000_000_000 + np.arange(n) * 10_000_000
builder = ChunkBuilder()
builder.add(
    times, "demo.sine", np.sin(2 * np.pi * np.arange(n) / 100), {"source": "example"}
)
builder.add(times[:1], "demo.state", ["running"], {"source": "example"})
df = builder.build().to_pandas()

asset, dataset, file = df.nom.to_nominal(
    asset="nominal.io upload example", wait=True
)
print(asset.name, dataset.name, file.ingest_status)
```

With no `dataset` argument, the data goes in a dataset called `nominal-pandas` on the asset, created the first time.
`wait=True` returns once Nominal has ingested the data.

## View it

Create a blank workbook on the asset, then drag channels such as `demo.sine` onto a chart from its channel search.

```{code-cell} ipython3
workbook = asset.create_workbook()
workbook
```

## Choosing the profile

`to_nominal` uses the profile it is told to (`profile="staging"`), else the one the asset or dataset came from, else the
active profile.

```{code-cell} ipython3
import nominal.config

nominal.config.active_profile()  # `use_profile`, then NOMINAL_PROFILE, then "default"
```

Switch for the rest of the session with `nominal.config.use_profile("staging")`, or save a new one from Python with
`nominal.config.make_profile("staging", token=..., base_url=...)`.
