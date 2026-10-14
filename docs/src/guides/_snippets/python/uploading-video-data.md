Once video files have been transformed into a format Nominal can ingest, uploading and ingesting the files is straightforward.

Video is stored as a **video channel** on a dataset, alongside your other test data. Get or create the
dataset for this camera, then add files to a named channel on it:

```{literalinclude} /guides/_snippets/code/data_upload/uploading_video_data_1.py
:language: python
```

For later flights or additional files from this flight, use the same dataset refname and channel, with the new file's absolute start time:

```{literalinclude} /guides/_snippets/code/data_upload/uploading_video_data_2.py
:language: python
```

:::{note}

Standalone `Video` objects (`client.create_video()`, `Video.add_file()`, `Asset.add_video()`) are
deprecated in favor of video channels. Using them emits a `LegacyVideoDeprecationWarning`.
:::
