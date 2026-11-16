---
icon: pencil
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# Add a file format to nominal.io

`nominal.io` is a registry of readers. A reader turns one file format into long-form frames, and
`nominal.io.read` finds it by extension. This example registers a reader for a toy format in the notebook; to ship
one in a package, see [the I/O reference](/ref/io/index.md#write-a-reader).

+++

## The format

A log of lines like `1760000000.5 temperature 21.3`: seconds since the Unix epoch, a channel name and a value.

```{code-cell} ipython3
from pathlib import Path

Path("bench.toylog").write_text(
    "# toy log\n"
    "1760000000.0 temperature 21.3\n"
    "1760000000.5 temperature 21.4\n"
    "1760000000.5 state running\n"
    "1760000001.0 temperature 21.6\n"
)
```

## The reader

`read_chunks` yields `Chunk`s built with `ChunkBuilder`, with no pandas or polars in sight; a streaming reader would
yield one per block, and this one yields one. `sniff` is optional: it only matters when several formats share an extension.

```{code-cell} ipython3
from collections.abc import Iterator

from nominal.io import Chunk, ChunkBuilder, Reader, register_reader


@register_reader(
    "toylog", [".toylog"], description="A toy log: <seconds> <channel> <value>"
)
class ToyLogReader(Reader):
    def sniff(self, path):
        return Path(path).read_text().startswith("# toy log")

    def read_chunks(self, path, **options) -> Iterator[Chunk]:
        rows = [
            line.split()
            for line in Path(path).read_text().splitlines()
            if not line.startswith("#")
        ]
        builder = ChunkBuilder()
        builder.add_rows(
            [
                round(float(t) * 1e9) for t, _, _ in rows
            ],  # nanoseconds since the epoch
            [channel for _, channel, _ in rows],
            [
                float(v) if v.replace(".", "", 1).isdigit() else v
                for _, _, v in rows
            ],  # numbers or strings
        )
        yield builder.build()
```

## Use it

```{code-cell} ipython3
import nominal.io
import nominal.pandas  # registers df.nom

df = nominal.io.read("bench.toylog")
df
```

```{code-cell} ipython3
df.nom.select("temperature").nom.wide()
```

## Test it

`check_reader` asserts the contract on a sample file: the schema, time order, and that `sniff` accepts it.

```{code-cell} ipython3
from nominal.io.testing import check_reader

len(check_reader(nominal.io.resolve_format("bench.toylog"), "bench.toylog"))
```

```{code-cell} ipython3
:tags: [remove-cell]

nominal.io.unregister("toylog")
Path("bench.toylog").unlink()
```
