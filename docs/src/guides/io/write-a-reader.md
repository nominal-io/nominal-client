---
myst:
  html_meta:
    description: "Write a reader for a file format, register it for a session or ship it in a package, and test it with check_reader."
---

# Add a file format

{.lead}
A format is a {py:class}`~nominal.io.Reader` that yields chunks of samples, described by a {py:class}`~nominal.io.ReaderSpec`.
Register the reader for one Python session, or ship it in a package that others install.

## Write the reader

A reader implements `read_chunks`, which yields {py:class}`~nominal.io.Chunk` objects built with {py:class}`~nominal.io.ChunkBuilder`.
It can also implement `sniff`, a cheap check of the file's first bytes that separates formats sharing an extension.

```python
from collections.abc import Iterator

from nominal.io import Chunk, ChunkBuilder, Reader


class MyLogReader(Reader):
    def sniff(self, path):
        with open(path, "rb") as f:
            return f.read(6) == b"MYLOG1"  # True, False, or None if it can't tell

    def read_chunks(self, path, **options) -> Iterator[Chunk]:
        import mylog_lib  # import heavy dependencies here, not at module level

        builder = ChunkBuilder()
        for block in mylog_lib.blocks(path):
            # Samples of one channel: timestamps in nanoseconds since the Unix epoch, values and tags
            builder.add(block.times_ns, block.name, block.values, {"unit": block.unit})
        yield builder.build()  # build and yield per block to stream a large file
```

{py:meth}`ChunkBuilder.add <nominal.io.ChunkBuilder.add>` takes samples of one channel; {py:meth}`~nominal.io.ChunkBuilder.add_rows` takes rows of mixed channels.
Values are numbers, `None` for a missing value, or strings.
Don't import pandas or polars in a reader. {py:func}`~nominal.io.read` converts the chunks to the DataFrame library the caller asks for.

## Register it for a session

{py:func}`~nominal.io.register_reader` registers a reader class in a notebook or script:

```python
from nominal.io import register_reader


@register_reader("mylog", [".mylog"])
class MyLogReader(Reader): ...
```

`nominal.io.read("run.mylog")` now uses it.

## Ship it in a package

Describe the format with a {py:class}`~nominal.io.ReaderSpec`.
Keep the module that holds the spec free of heavy imports: `factory` is imported only when a file is read.

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

Declare the spec as an entry point. The entry point's name must equal the spec's `name`.

```toml
# pyproject.toml
[project.entry-points."nominal.io.readers"]
mylog = "mypkg.spec:SPEC"
```

`nominal.io` discovers the entry point when the package is installed. A package that fails to load is skipped with a warning and doesn't affect other formats.
If the reader needs a package that isn't installed, `read` raises {py:class}`~nominal.io.MissingDependencyError` with the spec's `install_hint`.

## Test it

{py:func}`~nominal.io.testing.check_reader` asserts that a reader follows the contract on a small sample file and returns its data as one chunk:

```python
from mypkg.spec import SPEC
from nominal.io.testing import check_reader


def test_mylog():
    chunk = check_reader(SPEC, "tests/data/sample.mylog")
    assert len(chunk.channel_names) == 3
```

For an executable walkthrough, see [Add a file format to nominal.io](/examples/custom_reader.md).
