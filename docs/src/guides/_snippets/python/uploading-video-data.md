Once video files have been transformed into a format Nominal can ingest, uploading and ingesting the files is straightforward.

Video is stored as a **video channel** on a dataset, alongside your other test data. Create the
dataset once per video source, then add files to a named channel on it:

```{literalinclude} /guides/_snippets/code/data_upload/uploading_video_data_1.py
:language: python
```

For subsequent flight test events, or even just additional video files
from the original test event, skip creating a new dataset and
instead add files to the existing channel:

```{literalinclude} /guides/_snippets/code/data_upload/uploading_video_data_2.py
:language: python
```

:::{note}

Standalone `Video` objects (`client.create_video()`, `Video.add_file()`, `Asset.add_video()`) are
deprecated in favor of video channels. Using them emits a `LegacyVideoDeprecationWarning`.
:::
