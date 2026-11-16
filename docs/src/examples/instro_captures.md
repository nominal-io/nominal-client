---
icon: database
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# Read instro captures, in any format

`nominal.io` reads instro's `FilePublisher` output in CSV, Avro or JSON Lines into the same long-form frame
(pandas by default; `engine="polars"` for polars), and `df.nom` filters and reshapes the pandas one. This example writes a small capture so it has no data files; it needs no Nominal
profile. Avro needs `pip install nominal[avro]`.

+++

## Make a capture

Readers produce `Chunk`s, columns of numpy arrays that `nominal.io` turns into a pandas or polars frame.
`ChunkBuilder` collects samples of each channel, and `write_avro` saves them in Nominal's `AvroStream` format.

```{code-cell} ipython3
import numpy as np

import nominal.io
import nominal.pandas  # registers df.nom
import nominal.plotly
from nominal.io import ChunkBuilder
from nominal.io.instro import write_avro

nominal.plotly.set_style()

t0 = 1_760_000_000_000_000_000  # 2025-10-09T08:53:20Z, in ns
n = 200
times = t0 + np.arange(n) * 10_000_000  # 100 Hz
phase = np.arange(n) / 100

builder = ChunkBuilder()
builder.add(times, "daq.ai_0", np.sin(2 * np.pi * phase), {"run": "demo"})
builder.add(times, "daq.ai_1", 0.5 * np.cos(2 * np.pi * phase), {"run": "demo"})
chunk = builder.build()
write_avro(chunk, "capture.avro")
chunk.to_pandas().head()
```

## Read it back

`read` chooses the reader from the extension, and `format=` names one explicitly.
`nominal.io.list_formats()` shows everything that is installed.

```{code-cell} ipython3
df = nominal.io.read("capture.avro")
print(len(df), "rows;", df.nom.channels)
[f.name for f in nominal.io.list_formats()]
```

Large files can be streamed in chunks instead of loaded whole; each chunk is a `Chunk` unless you pass an `engine`:

```{code-cell} ipython3
for chunk in nominal.io.read_chunks("capture.avro", chunksize=150, engine="pandas"):
    print(len(chunk), "rows", chunk["timestamp"].min())
```

The same data as a polars frame:

```{code-cell} ipython3
nominal.io.read("capture.avro", engine="polars").head(3)
```

## Explore and plot

```{code-cell} ipython3
window = df.nom.between("2025-10-09 08:53:20.5Z", "1s")
window.nom.wide().plot(backend="plotly")
```

```{code-cell} ipython3
:tags: [remove-cell]

import os

os.remove("capture.avro")
```
