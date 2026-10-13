# Polars

Export queried channel values into Polars DataFrames.
{py:meth}`~nominal.thirdparty.polars.polars_export_handler.PolarsExportHandler.export`
takes a time range and optional tag filters, and yields the results in batches.

To download a dataset file to disk instead of querying channel values, use
{py:meth}`~nominal.core.DatasetFile.download`.

```{eval-rst}
.. automodule:: nominal.thirdparty.polars.polars_export_handler
```
