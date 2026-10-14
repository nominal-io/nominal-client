Journald-style `.jsonl` log files (or gzip-compressed `.jsonl.gz` files) can be uploaded to Nominal analogously to tabular data files.

```{literalinclude} /guides/_snippets/code/data_upload/uploading_log_data_1.py
:language: python
```

For another file, use the same refname to retrieve or create the dataset:

```{literalinclude} /guides/_snippets/code/data_upload/uploading_log_data_2.py
:language: python
```

:::{admonition} How is this different from tabular data?
:class: note

The same `nominal.core.Dataset` class handles uploading text logs *and* tabular data.
You *can* add text logs to a dataset created from `.csv` files, assuming that the dataset does not already have a channel named `logs`.
The text logs and numeric tabular data will show up as separate channels, and can be added/visualized in a workbook as normal.
:::
