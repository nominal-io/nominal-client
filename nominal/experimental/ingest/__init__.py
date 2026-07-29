"""Experimental multi-file ingest: a throughput-tuned uploader (see `MultipartUploader`)."""

from __future__ import annotations

from nominal.experimental.ingest._multipart_uploader import (
    DEFAULT_MAX_STORAGE_WORKERS,
    DEFAULT_SMALL_FILE_ROUTE_MAX_BYTES,
    MAX_SMALL_FILE_ROUTE_BYTES,
    MultipartUploader,
)
from nominal.experimental.ingest._upload_pacing import NOMINAL_MAX_CONCURRENCY

__all__ = [
    "DEFAULT_MAX_STORAGE_WORKERS",
    "DEFAULT_SMALL_FILE_ROUTE_MAX_BYTES",
    "MAX_SMALL_FILE_ROUTE_BYTES",
    "NOMINAL_MAX_CONCURRENCY",
    "MultipartUploader",
]
