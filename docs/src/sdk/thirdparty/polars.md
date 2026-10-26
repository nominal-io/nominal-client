# Polars

Export channel data in batches of Polars DataFrames with
{py:class}`~nominal.thirdparty.polars.polars_export_handler.PolarsExportHandler`.
Polars is installed with `nominal`.

The handler supports numeric (`DOUBLE` and `INT`) and `STRING` channels.
Other channel types are skipped with a warning. See the
[export guide](../../guides/datasets/export-data.md#export-in-batches)
for examples of selecting channels, time bounds, and batch output.

## API Reference

```{eval-rst}
.. include:: ../_summaries/nominal.thirdparty.polars.polars_export_handler.rst
```
