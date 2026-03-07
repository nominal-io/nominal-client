from __future__ import annotations

import logging
from pathlib import Path
from typing import BinaryIO, cast

import requests

from nominal.core.filetype import FileTypes
from nominal.core.video import Video
from nominal.core.video_file import VideoFile

logger = logging.getLogger(__name__)


def clone_video_file_to_video_dataset(
    source_video_file: VideoFile,
    destination_video_dataset: Video,
) -> VideoFile | None:
    """Clone a video file into a destination video dataset."""
    return copy_video_file_to_video_dataset(source_video_file, destination_video_dataset)


def copy_video_file_to_video_dataset(
    source_video_file: VideoFile,
    destination_video_dataset: Video,
) -> VideoFile | None:
    log_extras = {"destination_client_workspace": destination_video_dataset._clients.workspace_rid}
    logger.debug("Copying video file: %s", source_video_file.name, extra=log_extras)

    (mcap_video_details, timestamp_options) = source_video_file._get_file_ingest_options()
    old_file_uri = source_video_file._clients.catalog.get_video_file_uri(
        source_video_file._clients.auth_header, source_video_file.rid
    ).uri

    response = requests.get(old_file_uri, stream=True)
    response.raise_for_status()

    file_name = source_video_file.name
    file_stem = Path(file_name).stem
    if timestamp_options is not None:
        new_file = destination_video_dataset.add_from_io(
            video=cast(BinaryIO, response.raw),
            name=file_stem,
            start=timestamp_options.starting_timestamp,
            description=source_video_file.description,
        )
        new_file.update(
            starting_timestamp=timestamp_options.starting_timestamp,
            ending_timestamp=timestamp_options.ending_timestamp,
        )
    elif mcap_video_details is not None:
        new_file = destination_video_dataset.add_mcap_from_io(
            mcap=cast(BinaryIO, response.raw),
            name=file_stem,
            topic=mcap_video_details.mcap_channel_locator_topic,
            description=source_video_file.description,
            file_type=FileTypes.MCAP,
        )
    else:
        raise ValueError(
            "Unsupported video file ingest options for copying video file. "
            "Expected either _mcap_video_details or _timestamp_options to be set."
        )
    logger.debug(
        "New video file created %s in video dataset: %s (rid: %s)",
        new_file.name,
        destination_video_dataset.name,
        destination_video_dataset.rid,
    )
    return new_file
