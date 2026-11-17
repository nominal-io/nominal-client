# pandas

Work with Nominal data in pandas: the `df.nom` accessor for long-form frames, and helpers to upload
DataFrames and export channel data into DataFrames or Series. Installed with `nominal`.

```python
import nominal.io
import nominal.pandas  # registers df.nom

df = nominal.io.read("capture.avro")
df.nom.select("daq.ai_0", "daq.ai_1").nom.between("2025-10-09 08:53:21Z", "10s").nom.wide().plot()
asset, dataset, file = df.nom.to_nominal(asset="Bench run 12")
```

## The `df.nom` accessor

A long-form frame (see {py:mod}`nominal.io`) has one row per sample, with `timestamp`, `channel`, `value`
and `tags` columns. Importing `nominal.pandas` adds `df.nom`, with methods that filter and chain
({py:meth}`~nominal.pandas.NomAccessor.select`, {py:meth}`~nominal.pandas.NomAccessor.between`,
{py:meth}`~nominal.pandas.NomAccessor.first`, {py:meth}`~nominal.pandas.NomAccessor.last`,
{py:meth}`~nominal.pandas.NomAccessor.with_tags`), and methods that end a chain
({py:meth}`~nominal.pandas.NomAccessor.wide`, {py:meth}`~nominal.pandas.NomAccessor.to_avro`,
{py:meth}`~nominal.pandas.NomAccessor.to_nominal`).

The accessor registers only when `nominal.pandas` is imported, so `import nominal` stays light.
`to_avro` needs `nominal[avro]`.

## Upload and export helpers

Start with {py:func}`~nominal.pandas.datasource_to_dataframe` to export a dataset, or
{py:func}`~nominal.pandas.upload_dataframe_to_dataset` to upload to an existing one.
The reference also includes helpers for creating datasets and retrieving individual channels.

These functions were previously in `nominal.thirdparty.pandas`, which still works but is deprecated.
For batch exports with Polars, see the [export guide](../../guides/datasets/export-data.md#export-in-batches).

## API Reference

```{eval-rst}
.. include:: ../_summaries/nominal.pandas.rst
```
