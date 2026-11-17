---
myst:
  html_meta:
    description: "Read files of many formats into one DataFrame of samples, choose the format, stream large files and pick pandas or polars."
---

# Read files into a DataFrame

{.lead}
`nominal.io` reads files of many formats into one DataFrame with a row per sample.
It needs the Nominal Python Client. Some formats need an extra install, which the error for a missing one names.

## Read a file

{py:func}`~nominal.io.read` chooses the reader from the file's extension and returns a pandas DataFrame:

```python
import nominal.io

df = nominal.io.read("capture.avro")
```

The DataFrame has one row per sample, sorted by time, with `timestamp`, `channel`, `value` and `tags` columns.
The [reference](/ref/io/index.md) describes each column.

Pass `engine="polars"` to get a polars DataFrame instead. Its layout is experimental and may change.

```python
df = nominal.io.read("capture.avro", engine="polars")
```

## See which formats are installed

{py:func}`~nominal.io.list_formats` returns a {py:class}`~nominal.io.ReaderSpec` for each installed format, without importing its reader:

```python
for spec in nominal.io.list_formats():
    print(spec.name, spec.extensions)
```

The [reference](/ref/io/index.md) lists the formats that ship with the client and what each one needs. A package that adds a format shows up here once it is installed.

## Choose the format

`read` picks the format from the extension. When several formats share an extension, it looks at the start of the file to decide.
If it still can't tell, it raises an error that names the candidates. Pass the format's name to skip the guess:

```python
df = nominal.io.read("<path to file>", format="<format name>")
```

The [reference](/ref/io/index.md) gives the exact rules.

## Read a large file in pieces

{py:func}`~nominal.io.read_chunks` yields the file one piece at a time, so it doesn't need to fit in memory.
Each piece is a {py:class}`~nominal.io.Chunk` unless you pass `engine`:

```python
for chunk in nominal.io.read_chunks("capture.avro", engine="pandas", chunksize=50_000):
    ...  # process one DataFrame of up to about 50,000 samples
```

`read_chunks` is a generator, so an unknown format or a missing dependency raises when you start iterating, not when you call it.

## Pass options to a reader

Keyword arguments other than `format` and `engine` go to the format's reader. Use them to read part of a file or set a start time.
The instro readers take `chunksize`, the number of samples per piece. Each reader's options are in its reference entry.

```python
# The WFDB reader takes a start time and a range of samples
df = nominal.io.read("119.hea", start="2026-01-01T12:00:00Z", sampfrom=0, sampto=21_600)
```

To filter, slice and reshape the DataFrame, use the [`df.nom` accessor](/ref/pandas/index.md#the-dfnom-accessor).
To read a format that isn't installed, see [Add a file format](/guides/io/write-a-reader.md).
