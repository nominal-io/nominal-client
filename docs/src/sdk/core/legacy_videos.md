# Legacy Videos

:::{warning}
Standalone `Video` and `VideoFile` APIs are deprecated. For new integrations, use
video channels on a {py:class}`~nominal.core.Dataset` and the
{py:class}`~nominal.core.VideoDatasetFile` objects returned by
{py:meth}`~nominal.core.Dataset.add_video` and
{py:meth}`~nominal.core.Dataset.list_video_files`.
:::

For current video ingestion, see the [Video ingest guide](/guides/video/video-ingest.md)
and [Datasets and files](dataset.md).

```python
from nominal.core import Video, VideoFile
```

## Videos

```{eval-rst}
.. automodule:: nominal.core.video
```

## Video files

```{eval-rst}
.. automodule:: nominal.core.video_file
```
