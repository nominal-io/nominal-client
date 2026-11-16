# I/O

Read files of many formats into one long-form frame (pandas, or polars), and add formats of your own.

```python
import nominal.io

df = nominal.io.read("capture.mylog")         # format chosen from the file
for chunk in nominal.io.read_chunks("big.mylog", engine="pandas"):   # for files too big for memory
    ...
df = nominal.io.read("capture.dat", format="mylog")   # name the format when it can't be guessed
df = nominal.io.read("capture.mylog", engine="polars")   # a polars frame instead of pandas (experimental layout)
nominal.io.list_formats()                      # what's installed
```

Every format produces the same data, one row per sample. A reader yields `Chunk` objects (columns of numpy arrays),
and `read` converts them to the frame library you ask for with `engine=` (`"pandas"` by default), so no reader
depends on pandas or polars. The pandas layout is:

| column      | dtype                     | contents                                                  |
| ----------- | ------------------------- | --------------------------------------------------------- |
| `timestamp` | `datetime64[ns, UTC]`     | sample time                                               |
| `channel`   | `object` (str)            | channel name, e.g. `"daq.ai_0"`                           |
| `value`     | `float64` or `object`     | `float64` when every value is numeric, else floats and strings mixed |
| `tags`      | `object` (dict)           | tag dict, `{}` when untagged                              |

Rows are sorted by timestamp, stably. The polars layout has `value` (`Float64`, null for string samples), `value_str`
(`String`) and `tags` (a list of `{key, value}` structs); it is experimental and may change.

## How a file's format is chosen

1. `format="name"`, if given, always wins.
2. Otherwise the formats claiming the longest matching extension compete (`.sigmf-meta` beats `.meta`).
3. If several remain, each one's `sniff` method looks at the file's first bytes or header.
4. If that still leaves more than one, `read` raises an error naming the candidates.

Nothing is guessed silently.

## Write a reader

A format is a `Reader` that yields `Chunk`s, plus a `ReaderSpec` that tells the registry its
name and extensions without importing your code.

```python
from collections.abc import Iterator

from nominal.io import Chunk, ChunkBuilder, Reader


class MyLogReader(Reader):
    def sniff(self, path):
        with open(path, "rb") as f:
            return f.read(6) == b"MYLOG1"          # True, False, or None if you can't tell

    def read_chunks(self, path, **options) -> Iterator[Chunk]:
        import mylog_lib                           # heavy imports go here, not at module level

        builder = ChunkBuilder()
        for block in mylog_lib.blocks(path):
            # many samples of one channel: timestamps in ns since the epoch, values, optional tags
            builder.add(block.times_ns, block.name, block.values, {"unit": block.unit})
        yield builder.build()                      # or build and yield per block to stream a big file
```

`ChunkBuilder.add` takes samples of one channel; `add_rows` takes rows of mixed channels. Values can be numbers,
`None` (missing) or strings. Don't import pandas or polars in a reader.

For a quick one-off in a notebook or script, register it for the session:

```python
from nominal.io import register_reader

@register_reader("mylog", [".mylog"])
class MyLogReader(Reader): ...
```

To ship it in a package, declare the spec as an entry point. Keep the module that holds the spec free of
heavy imports; `factory` is imported only when a file is read.

```python
# mypkg/spec.py
from nominal.io import ReaderSpec

SPEC = ReaderSpec(
    name="mylog",
    extensions=(".mylog",),
    factory="mypkg.reader:MyLogReader",
    description="MyLog recordings",
    install_hint="pip install mypkg",
)
```

```toml
# pyproject.toml
[project.entry-points."nominal.io.readers"]
mylog = "mypkg.spec:SPEC"
```

The entry-point name must equal the spec's `name`. A package that fails to load is skipped with a warning
and doesn't affect other formats.

Test it with `check_reader`, which asserts the contract on a small sample file and returns the data as one `Chunk`:

```python
from nominal.io.testing import check_reader
from mypkg.spec import SPEC

def test_mylog():
    chunk = check_reader(SPEC, "tests/data/sample.mylog")
    assert len(chunk.channel_names) == 3
```

## API Reference

```{eval-rst}
.. include:: ../_summaries/nominal.io.rst
.. include:: ../_summaries/nominal.io.testing.rst
```
