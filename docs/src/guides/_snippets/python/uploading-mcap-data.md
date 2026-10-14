MCAP files are uploaded to Nominal analogously to any other tabular file format, though they differ
slightly in the sense that they may also have video content. Video frames go to a video channel on
the same dataset, so telemetry and video share one time domain.

```{literalinclude} /guides/_snippets/code/data_upload/uploading_mcap_data_1.py
:language: python
```

For subsequent flight test events, skip creating the dataset in favor of searching for the dataset created earlier:

```{literalinclude} /guides/_snippets/code/data_upload/uploading_mcap_data_2.py
:language: python
```
