from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import BinaryIO, cast

import requests

from nominal.core._utils.filenames import sanitize_upload_filename
from nominal.core._video_types import McapVideoDetails, TimestampOptions
from nominal.core.exceptions import NominalIngestFailed, NominalIngestTimeout, NominalVideoFileMetadataError
from nominal.core.filetype import FileTypes
from nominal.core.video import Video
from nominal.core.video_file import VideoFile
from nominal.experimental.migration.utils.retry_utils import retry_transient

logger = logging.getLogger(__name__)

# Deliberately far longer than any healthy ingest: the deadline exists to catch stalled ingests, not slow ones.
DEFAULT_INGEST_POLL_TIMEOUT = timedelta(hours=2)


@dataclass(frozen=True)
class VideoFileCopyOutcome:
    """`file` is what was created (record it in state); `skip_reason` is set when the copy was
    less than a clean success. A timed-out or failed ingest sets both.
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
        # NominalVideoFileMetadataError is permanent, so it still raises on the first attempt.
        (mcap_video_details, timestamp_options) = retry_transient(
            source_video_file._get_file_ingest_options,
            description=f"ingest options for video file {source_video_file.rid}",
        )
    except NominalVideoFileMetadataError as error:
        # Unusable where it already lives; retrying cannot help. Skip it, not the whole asset.
        logger.warning(
            "Skipping video file %s (rid: %s): %s",
            source_video_file.name,
            source_video_file.rid,
            error,
            extra=log_extras,
        )
        return VideoFileCopyOutcome(file=None, skip_reason=f"unusable at source: {error}")

    # Download + upload retry as one unit: the source download is streamed straight into the
    # upload, so a connection broken in either leg restarts from a fresh presigned URI. A
    # failure during the transfer itself creates no destination record; only a failure in the
    # upload call's final registration step can leave one behind, in which case the retry may
    # strand a duplicate file — never a duplicate mapping.
    new_file = retry_transient(
        lambda: _upload_to_destination(
            source_video_file, destination_video_dataset, mcap_video_details, timestamp_options
        ),
        description=f"copy of video file {source_video_file.rid}",
    )

    # From here the destination file exists, so every path must return it — the caller records
    # the mapping, and an escaping exception would leave a rerun to upload a duplicate.
    try:
        skip_reason = _finish_copy(new_file, timestamp_options, poll_timeout)
    except Exception as error:
        logger.warning("Post-upload steps failed for video file (rid: %s): %s", new_file.rid, error, extra=log_extras)
        skip_reason = (
            f"upload succeeded but its ingest could not be confirmed: {error}; "
            f"a rerun will not retry it — check {new_file.rid} by hand"
        )
    logger.debug(
        "New video file created %s in video dataset: %s (rid: %s)",
        new_file.name,
        destination_video_dataset.name,
        destination_video_dataset.rid,
    )
    return VideoFileCopyOutcome(file=new_file, skip_reason=skip_reason)


def _upload_to_destination(
    source_video_file: VideoFile,
    destination_video_dataset: Video,
    mcap_video_details: McapVideoDetails | None,
    timestamp_options: TimestampOptions | None,
) -> VideoFile:
    old_file_uri = source_video_file._clients.catalog.get_video_file_uri(
        source_video_file._clients.auth_header, source_video_file.rid
    ).uri

    # timeout bounds connect and per-chunk reads — the same stalled-server hang the poll deadline
    # catches. The context manager closes the connection on every exit path, so a failed attempt
    # doesn't strand a partially-drained socket for each retry.
    with requests.get(old_file_uri, stream=True, timeout=60) as response:
        response.raise_for_status()
        raw_video_stream = cast(BinaryIO, response.raw)

        file_stem = sanitize_upload_filename(_resolve_destination_file_stem(source_video_file.name))
        if timestamp_options is not None:
            return destination_video_dataset.add_from_io(
                video=raw_video_stream,
                name=file_stem,
                start=timestamp_options.starting_timestamp,
                description=source_video_file.description,
            )

        if mcap_video_details is not None:
            return destination_video_dataset.add_mcap_from_io(
                mcap=raw_video_stream,
                name=file_stem,
                topic=mcap_video_details.mcap_channel_locator_topic,
                description=source_video_file.description,
                file_type=FileTypes.MCAP,
            )

    raise ValueError(
        "Unsupported video file ingest options for copying video file. "
        "Expected either _mcap_video_details or _timestamp_options to be set."
    )


def _finish_copy(
    new_file: VideoFile,
    timestamp_options: TimestampOptions | None,
    poll_timeout: timedelta | None,
) -> str | None:
    """Wait for ingest and apply source timing; a non-None return is the skip reason for a
    less-than-clean copy. May raise — the caller converts that into a skip too, so the
    already-uploaded file is always recorded as migrated.
    """
    skip_reasons: list[str] = []
    ingest_failed = False
    try:
        _await_ingestion(new_file, poll_timeout)
    except NominalIngestTimeout as error:
        logger.warning("Video file (rid: %s) did not finish ingesting: %s", new_file.rid, error)
        skip_reasons.append(
            f"upload succeeded but ingest did not complete within {poll_timeout}; "
            f"a rerun will not retry it — check {new_file.rid} by hand"
        )
    except NominalIngestFailed as error:
        # The destination rejected the media itself (e.g. a segmentation failure) — re-uploading
        # the same bytes cannot help.
        logger.warning("Video file (rid: %s) failed to ingest at destination: %s", new_file.rid, error)
        skip_reasons.append(
            f"upload succeeded but ingest failed at destination: {error}; "
            f"a rerun will not retry it — check {new_file.rid} by hand"
        )
        ingest_failed = True

    if timestamp_options is not None and not ingest_failed:
        # Applied even on timeout: the copy is recorded as migrated either way, so no rerun
        # will come back to set the video's timing. Skipped when ingest failed outright —
        # the file needs hand-checking anyway.
        try:
            retry_transient(
                lambda: new_file.update(
                    starting_timestamp=timestamp_options.starting_timestamp,
                    ending_timestamp=timestamp_options.ending_timestamp,
                ),
                description=f"timestamp update for video file {new_file.rid}",
            )
        except Exception as error:
            # The sent values come from the source's segment metadata; recording them here is
            # often the only way to diagnose a destination-side rejection (the server may not
            # say which argument it refused).
            logger.warning(
                "Timestamp update failed for video file (rid: %s), sent starting_timestamp=%s ending_timestamp=%s: %s",
                new_file.rid,
                timestamp_options.starting_timestamp,
                timestamp_options.ending_timestamp,
                error,
            )
            skip_reasons.append(
                f"the timestamp update was rejected: {error} "
                f"(sent starting_timestamp={timestamp_options.starting_timestamp}, "
                f"ending_timestamp={timestamp_options.ending_timestamp}); "
                f"a rerun will not retry it — set timing on {new_file.rid} by hand"
            )

    return "; ".join(skip_reasons) if skip_reasons else None


def _await_ingestion(new_file: VideoFile, poll_timeout: timedelta | None) -> None:
    """Wait for a copied video to finish ingesting.

    Raises NominalIngestTimeout/NominalIngestFailed (and, past the retry budget, transient
    polling errors) — the callers above convert those into skip-with-mapping outcomes.
    """
    deadline = None if poll_timeout is None else time.monotonic() + poll_timeout.total_seconds()

    def _poll() -> None:
        # Always measured against the original deadline, so retried status checks don't
        # stretch the overall poll budget.
        remaining = None if deadline is None else timedelta(seconds=max(0.0, deadline - time.monotonic()))
        new_file.poll_until_ingestion_completed(timeout=remaining)

    # A transient error on a single status check (e.g. a 502 from a gateway) must not
    # abandon an upload that already succeeded — a rerun would duplicate the file.
    retry_transient(_poll, description=f"ingest status poll for video file {new_file.rid}")


def _resolve_destination_file_stem(file_name: str) -> str:
    file_stem = Path(file_name).stem
    _, separator, suffix = file_stem.partition("Z_")
    return suffix if separator else file_stem
