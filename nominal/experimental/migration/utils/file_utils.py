from __future__ import annotations

import logging
from typing import BinaryIO, cast
from urllib.parse import unquote_plus

import requests

from nominal.core import Dataset, DatasetFile, FileType, FileTypes
from nominal.experimental.migration.utils.filename_utils import resolve_destination_filename

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

        # Source keys from data uploaded before the encoding fix are percent-encoded (e.g.
        # "...Z_paren%28reduced%29.csv"). Decode so the destination upload sends the real name
        # rather than re-uploading a literal "%28", which breaks Azure Blob. Use unquote_plus:
        # the old encoder was quote_plus, which maps space -> "+", so a plain unquote would leave
        # "my+file" instead of "my file". Then sanitize any genuinely-unsafe characters (migration
        # replaces rather than raises, so one bad source filename never blocks a bulk migration).
        raw_name = source_api_file.handle.s3.key.split("/")[-1]
        file_name = unquote_plus(raw_name)
        file_type = FileType.from_path(file_name)
        file_name = resolve_destination_filename(file_name)

        with response:
            match file_type:
                case FileTypes.JOURNAL_JSONL | FileTypes.JOURNAL_JSONL_GZ:
                    new_file = destination_dataset.add_journal_json_from_io(
                        cast(BinaryIO, response.raw),
                        file_type=file_type,
                        file_name=file_name,
                    )
                case _ if source_file.timestamp_channel is not None and source_file.timestamp_type is not None:
                    new_file = destination_dataset.add_from_io(
                        dataset=cast(BinaryIO, response.raw),
                        timestamp_column=source_file.timestamp_channel,
                        timestamp_type=source_file.timestamp_type,
                        file_type=file_type,
                        file_name=file_name,
                        tag_columns=source_file.tag_columns,
                        tags=source_file.file_tags,
                    )
                case _:
                    raise ValueError("Unsupported file handle type or missing timestamp information.")
        new_file.poll_until_ingestion_completed()
        logger.debug(
            "New file created %s in dataset: %s (rid: %s)",
            new_file.name,
            destination_dataset.name,
            destination_dataset.rid,
        )
        return new_file
    raise ValueError("Unsupported file handle type or missing timestamp information.")
