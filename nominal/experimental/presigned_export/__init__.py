"""Experimental: presigned-link data export, staged here ahead of its real landing.

`PolarsExportHandler.export_to_files` exports channel data by asking the export service
for an S3 presigned link per planned request (`generate_export_channel_data_presigned_link`)
and downloading each file straight to disk with parallel ranged GETs, so the export never
sits in memory and the API-proxy size ceiling does not apply.

Both modules are verbatim copies from the in-progress branch `sroustom/sync-channels-phases`
(as of commit 1edbbbb), with one import rewritten so this package is self-contained:

- ``polars_export_handler.py`` supersedes ``nominal/thirdparty/polars/polars_export_handler.py``
- ``multipart_downloader.py`` supersedes ``nominal/core/_utils/multipart_downloader.py``

This package exists so downstream tooling can depend on the presigned export path from a
git branch before that branch merges. Delete the whole package when it does — consumers
switch to the real module paths.
"""

from nominal.experimental.presigned_export.polars_export_handler import PolarsExportHandler

__all__ = ["PolarsExportHandler"]
