from __future__ import annotations

import logging
import shutil
import tempfile
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
            tmp_path = None
            try:
                with tempfile.NamedTemporaryFile(suffix=file_type.extension, delete=False) as tmp:
                    tmp_path = Path(tmp.name)
                    with response:
                        shutil.copyfileobj(response.raw, tmp)
                new_file = destination_dataset.add_journal_json(tmp_path)
                new_file.poll_until_ingestion_completed()
            finally:
                if tmp_path is not None:
                    tmp_path.unlink(missing_ok=True)
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


def _resolve_destination_file_stem(file_name: str) -> str:
    file_stem = Path(file_name).stem
    _, separator, suffix = file_stem.partition("Z_")
    return suffix if separator else file_stem
