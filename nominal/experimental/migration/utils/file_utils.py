from __future__ import annotations

import logging
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import BinaryIO, cast

import requests

from nominal.core import Dataset, DatasetFile, FileType

logger = logging.getLogger(__name__)


def copy_file_to_dataset(
    source_file: DatasetFile,
    destination_dataset: Dataset,
) -> DatasetFile:
    log_extras = {"destination_client_workspace": destination_dataset._clients.workspace_rid}
    logger.debug("Copying dataset file: %s", source_file.name, extra=log_extras)
    source_api_file = source_file._get_latest_api()
    if source_api_file.handle.s3 is not None:
        old_file_uri = source_file._clients.catalog.get_dataset_file_uri(
            source_file._clients.auth_header, source_file.dataset_rid, source_file.id
        ).uri

        response = requests.get(old_file_uri, stream=True)
        response.raise_for_status()

        file_name = source_api_file.handle.s3.key.split("/")[-1]
        file_type = FileType.from_path(file_name)
        file_stem = _resolve_destination_file_stem(file_name)

        if file_type.is_journal():
            new_file = _ingest_from_temp_file(
                response,
                file_name,
                destination_dataset.add_journal_json,
            )
        elif file_type.is_mcap():
            new_file = _ingest_from_temp_file(
                response,
                file_name,
                lambda path: destination_dataset.add_mcap(path, tags=source_file.file_tags),
            )
        elif file_type.is_ardupilot_dataflash():
            new_file = _ingest_from_temp_file(
                response,
                file_name,
                lambda path: destination_dataset.add_ardupilot_dataflash(path, tags=source_file.file_tags),
            )
        elif source_file.timestamp_channel is not None and source_file.timestamp_type is not None:
            new_file = destination_dataset.add_from_io(
                dataset=cast(BinaryIO, response.raw),
                timestamp_column=source_file.timestamp_channel,
                timestamp_type=source_file.timestamp_type,
                file_type=file_type,
                file_name=file_stem,
                tag_columns=source_file.tag_columns,
                tags=source_file.file_tags,
            )
            new_file.poll_until_ingestion_completed()
        else:
            raise ValueError("Unsupported file handle type or missing timestamp information.")
        logger.debug(
            "New file created %s in dataset: %s (rid: %s)",
            new_file.name,
            destination_dataset.name,
            destination_dataset.rid,
        )
        return new_file
    raise ValueError("Unsupported file handle type or missing timestamp information.")


def _ingest_from_temp_file(
    response: requests.Response,
    file_name: str,
    ingest_fn: Callable[[Path], DatasetFile],
) -> DatasetFile:
    """Stream the response body to a temp file and ingest it via the given native ingest function.

    Used for file types (journal, MCAP, ArduPilot Dataflash) that must be re-ingested through their
    dedicated ingest path rather than as raw CSV/parquet, so the destination reprocesses them the
    same way the source did. The original file name is preserved so the upload name is meaningful.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / file_name
        with response:
            with tmp_path.open("wb") as tmp:
                shutil.copyfileobj(response.raw, tmp)
        new_file = ingest_fn(tmp_path)
        new_file.poll_until_ingestion_completed()
        return new_file


def _resolve_destination_file_stem(file_name: str) -> str:
    file_stem = Path(file_name).stem
    _, separator, suffix = file_stem.partition("Z_")
    return suffix if separator else file_stem
