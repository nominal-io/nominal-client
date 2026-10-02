When uploading tabular data, there are a few common formats Nominal supports, as well as some more specialized formats.
These include (but are not limited to):

* `.csv` files (and tarballs of CSV files `.csv.gz`)
* `.parquet` files
* `.bin` ardupilot dataflash files

```{literalinclude} /guides/_snippets/code/data_upload/uploading_tabular_data_1.py
:language: python
```

Since `get_or_create_dataset` returns the existing dataset if one already exists with that data scope name, you can use the same code for subsequent flight test events.
You can also retrieve the dataset directly by its data scope name:

```{literalinclude} /guides/_snippets/code/data_upload/uploading_tabular_data_2.py
:language: python
```

:::{note}

Want to ingest Ardupilot Dataflash `.bin` files?
When adding data to the dataset, you can use `Dataset.add_ardupilot_dataflash`.
:::
