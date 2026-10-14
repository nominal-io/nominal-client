When uploading tabular data, there are a few common formats Nominal supports, as well as some more specialized formats.
These include (but are not limited to):

* `.csv` files (or gzip-compressed CSV files, `.csv.gz`)
* `.parquet` files
* `.bin` ardupilot dataflash files

```{literalinclude} /guides/_snippets/code/data_upload/uploading_tabular_data_1.py
:language: python
```

Use the same pattern for later flights or additional files. Each file can specify its own timestamp column and type:

```{literalinclude} /guides/_snippets/code/data_upload/uploading_tabular_data_2.py
:language: python
```

:::{note}

Want to ingest Ardupilot Dataflash `.bin` files?
When adding data to the dataset, you can use `Dataset.add_ardupilot_dataflash`.
:::
