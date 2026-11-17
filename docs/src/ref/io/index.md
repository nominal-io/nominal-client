# I/O

{.lead}
Read files of many formats into one long-form DataFrame, and add formats of your own.
To get started, see [Read files into a DataFrame](/guides/io/read-files.md) and [Add a file format](/guides/io/write-a-reader.md).

## Frame layout

A reader yields {py:class}`~nominal.io.Chunk` objects, which are columns of numpy arrays.
{py:func}`~nominal.io.read` converts them to the DataFrame library you ask for with `engine=`, so no reader depends on pandas or polars.
The pandas layout (`engine="pandas"`, the default) has one row per sample:

| Column      | Type                  | Contents                                                          |
| ----------- | --------------------- | ----------------------------------------------------------------- |
| `timestamp` | `datetime64[ns, UTC]` | Sample time.                                                      |
| `channel`   | `object` (str)        | Channel name, such as `"daq.ai_0"`.                               |
| `value`     | `float64` or `object` | `float64` when every value is numeric, otherwise floats and strings mixed. |
| `tags`      | `object` (dict)       | Tags of the sample, `{}` when it has none.                        |

Rows are sorted by timestamp, and rows with the same timestamp keep their order in the file.

The polars layout (`engine="polars"`) is experimental and may change. `timestamp` is `Datetime("ns", "UTC")` and `channel` is `String`.
`value` is `Float64`, null for a string sample, and `value_str` is `String`, null for a numeric sample.
`tags` is a list of `{key, value}` structs.

## How a format is chosen

`read` and `read_chunks` choose the format for a file in this order:

1. The format named by `format=`, if given.
2. The formats that claim the longest matching extension, so `.sigmf-meta` beats `.meta`.
3. If several remain, the format whose `sniff` method accepts the first bytes or header of the file. A format that can't be imported is skipped.
4. If that leaves more than one, `read` raises an error that names the candidates.

## API Reference

```{eval-rst}
.. include:: ../_summaries/nominal.io.rst
.. include:: ../_summaries/nominal.io.testing.rst
```
