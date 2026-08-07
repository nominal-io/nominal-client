from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import BinaryIO, cast

import requests

from nominal.core._utils.filenames import sanitize_upload_filename
from nominal.core._video_types import McapVideoDetails, TimestampOptions
from nominal.core.exceptions import NominalIngestTimeout, NominalVideoFileMetadataError
from nominal.core.filetype import FileTypes
from nominal.core.video import Video
from nominal.core.video_file import DEFAULT_INGEST_POLL_TIMEOUT, VideoFile

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VideoFileCopyOutcome:
    """What happened to one video file.

    The two fields are independent on purpose. `file` is set whenever something was created on
    the destination and should be recorded in migration state; `skip_reason` is set whenever the
    copy was less than a clean success. A timed-out ingest sets both: the file exists and must not
    be uploaded a second time, but the run must still report that we never saw it finish.
    """

    file: VideoFile | None
    skip_reason: str | None = None


def copy_video_file_to_video_dataset(
    source_video_file: VideoFile,
    destination_video_dataset: Video,
    poll_timeout: timedelta | None = DEFAULT_INGEST_POLL_TIMEOUT,
) -> VideoFileCopyOutcome:
    log_extras = {"destination_client_workspace": destination_video_dataset._clients.workspace_rid}
    logger.debug("Copying video file: %s", source_video_file.name, extra=log_extras)

    try:
        (mcap_video_details, timestamp_options) = source_video_file._get_file_ingest_options()
    except NominalVideoFileMetadataError as error:
        # The source file is unusable where it already lives, so there is nothing to copy and no
        # amount of retrying will change that. Skip it and let the rest of the asset through.
        logger.warning(
            "Skipping video file %s (rid: %s): %s",
            source_video_file.name,
            source_video_file.rid,
            error,
            extra=log_extras,
        )
        return VideoFileCopyOutcome(file=None, skip_reason=f"unusable at source: {error}")

    old_file_uri = source_video_file._clients.catalog.get_video_file_uri(
        source_video_file._clients.auth_header, source_video_file.rid
    ).uri

    response = requests.get(old_file_uri, stream=True)
    response.raise_for_status()

    new_file, skip_reason = _create_destination_video_file(
        source_video_file,
        destination_video_dataset,
        cast(BinaryIO, response.raw),
        mcap_video_details,
        timestamp_options,
        poll_timeout,
    )
    logger.debug(
        "New video file created %s in video dataset: %s (rid: %s)",
        new_file.name,
        destination_video_dataset.name,
        destination_video_dataset.rid,
    )
    return VideoFileCopyOutcome(file=new_file, skip_reason=skip_reason)


def _create_destination_video_file(
    source_video_file: VideoFile,
    destination_video_dataset: Video,
    raw_video_stream: BinaryIO,
    mcap_video_details: McapVideoDetails | None,
    timestamp_options: TimestampOptions | None,
    poll_timeout: timedelta | None,
) -> tuple[VideoFile, str | None]:
    file_stem = sanitize_upload_filename(_resolve_destination_file_stem(source_video_file.name))
    if timestamp_options is not None:
        new_file = destination_video_dataset.add_from_io(
            video=raw_video_stream,
            name=file_stem,
            start=timestamp_options.starting_timestamp,
            description=source_video_file.description,
        )
        skip_reason = _await_ingestion(new_file, poll_timeout)
        if skip_reason is not None:
            # The timestamps below are metadata on a file the server has not finished ingesting;
            # applying them now would fail on top of an already-degraded copy.
            return new_file, skip_reason
        new_file.update(
            starting_timestamp=timestamp_options.starting_timestamp,
            ending_timestamp=timestamp_options.ending_timestamp,
        )
        return new_file, None

    if mcap_video_details is not None:
        new_file = destination_video_dataset.add_mcap_from_io(
            mcap=raw_video_stream,
            name=file_stem,
            topic=mcap_video_details.mcap_channel_locator_topic,
            description=source_video_file.description,
            file_type=FileTypes.MCAP,
        )
        return new_file, _await_ingestion(new_file, poll_timeout)

    raise ValueError(
        "Unsupported video file ingest options for copying video file. "
        "Expected either _mcap_video_details or _timestamp_options to be set."
    )


def _await_ingestion(new_file: VideoFile, poll_timeout: timedelta | None) -> str | None:
    """Wait for a copied video to finish ingesting; return a skip reason if it never did.

    A timeout is not treated as a failure of the asset: the upload itself succeeded, and the
    destination may well finish on its own. It is reported so nobody mistakes the run for
    complete, and the caller still records the mapping so a rerun does not upload it twice.
    """
    try:
        new_file.poll_until_ingestion_completed(timeout=poll_timeout)
    except NominalIngestTimeout as error:
        logger.warning("Video file (rid: %s) did not finish ingesting: %s", new_file.rid, error)
        return f"upload succeeded but ingest did not complete within {poll_timeout}: {new_file.rid}"
    return None


def _resolve_destination_file_stem(file_name: str) -> str:
    file_stem = Path(file_name).stem
    _, separator, suffix = file_stem.partition("Z_")
    return suffix if separator else file_stem
