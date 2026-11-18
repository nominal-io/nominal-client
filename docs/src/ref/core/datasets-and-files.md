# Datasets and files

Upload files, monitor ingestion, and work with dataset channels and exports.

## Upload files to storage

Use {py:class}`~nominal.core.MultipartUploader` when another operation needs the storage
location for each local file. One uploader shares its storage connections and payload
buffer across the batch. Each future returns the location when that upload completes.

```python
from pathlib import Path

from nominal.core import MultipartUploader

paths = [Path("manifest.json"), Path("recording.mcap")]
with MultipartUploader.create(client, max_buffer_bytes=128 * 1024**2) as uploader:
    uploads = {path: uploader.enqueue_file(path) for path in paths}
    locations = {path: upload.result() for path, upload in uploads.items()}
```

Choose a byte budget large enough for each individual part. A reader waits for space
before reading its next part. Small files can use a single Nominal request while
multipart uploads send bytes directly to storage. To ingest the files into a dataset, see [ingest jobs](/guides/ingest/ingestion-jobs.md).

## API Reference

```{eval-rst}
.. include:: ../_summaries/nominal.core.rst
   :start-after: .. core-datasets-and-files-start
   :end-before: .. core-datasets-and-files-end
```
