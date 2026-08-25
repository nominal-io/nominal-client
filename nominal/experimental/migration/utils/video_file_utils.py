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


@dataclass(frozen=True)
class _IngestOutcome:
    """`failed` marks a terminal destination-side ingest error (as opposed to a timeout),
    which downstream steps like the timestamp update must not run against.
    """

    skip_reason: str | None = None
    failed: bool = False


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
    # failed upload creates no destination file record, so re-running does not duplicate.
    new_file = retry_transient(
        lambda: _upload_to_destination(
            source_video_file, destination_video_dataset, mcap_video_details, timestamp_options
        ),
        description=f"copy of video file {source_video_file.rid}",
    )

    ingest_outcome = _await_ingestion(new_file, poll_timeout)
    update_skip_reason: str | None = None
    if timestamp_options is not None and not ingest_outcome.failed:
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
            # The upload and ingest already succeeded — failing the whole copy here would
            # abort the asset and leave a rerun to upload a duplicate. Record the file as
            # migrated and surface the unset timing in the end-of-run summary instead.
            logger.warning(
                "Timestamp update failed for video file (rid: %s): %s", new_file.rid, error, extra=log_extras
            )
            update_skip_reason = (
                f"copied but the timestamp update was rejected: {error}; "
                f"a rerun will not retry it — set timing on {new_file.rid} by hand"
            )

    skip_reasons = [reason for reason in (ingest_outcome.skip_reason, update_skip_reason) if reason is not None]
    logger.debug(
        "New video file created %s in video dataset: %s (rid: %s)",
        new_file.name,
        destination_video_dataset.name,
        destination_video_dataset.rid,
    )
    return VideoFileCopyOutcome(file=new_file, skip_reason="; ".join(skip_reasons) if skip_reasons else None)


def _upload_to_destination(
    source_video_file: VideoFile,
    destination_video_dataset: Video,
    mcap_video_details: McapVideoDetails | None,
    timestamp_options: TimestampOptions | None,
) -> VideoFile:
    old_file_uri = source_video_file._clients.catalog.get_video_file_uri(
        source_video_file._clients.auth_header, source_video_file.rid
    ).uri

    # timeout bounds connect and per-chunk reads — the same stalled-server hang the poll deadline catches.
    response = requests.get(old_file_uri, stream=True, timeout=60)
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


def _await_ingestion(new_file: VideoFile, poll_timeout: timedelta | None) -> _IngestOutcome:
    """Wait for a copied video to finish ingesting; return a skip reason on timeout or failure."""
    deadline = None if poll_timeout is None else time.monotonic() + poll_timeout.total_seconds()
    first_attempt = True

    def _poll() -> None:
        nonlocal first_attempt
        if first_attempt:
            first_attempt = False
            remaining = poll_timeout
        else:
            # Resume against the original deadline, so flaky status checks don't stretch the budget.
            remaining = None if deadline is None else timedelta(seconds=max(0.0, deadline - time.monotonic()))
        new_file.poll_until_ingestion_completed(timeout=remaining)

    try:
        # A transient error on a single status check (e.g. a 502 from a gateway) must not
        # abandon an upload that already succeeded — a rerun would duplicate the file.
        retry_transient(_poll, description=f"ingest status poll for video file {new_file.rid}")
    except NominalIngestTimeout as error:
        logger.warning("Video file (rid: %s) did not finish ingesting: %s", new_file.rid, error)
        return _IngestOutcome(
            skip_reason=(
                f"upload succeeded but ingest did not complete within {poll_timeout}; "
                f"a rerun will not retry it — check {new_file.rid} by hand"
            )
        )
    except NominalIngestFailed as error:
        # The destination rejected the media itself (e.g. a segmentation failure) — re-uploading
        # the same bytes cannot help, so record it as a skip instead of failing the asset.
        logger.warning("Video file (rid: %s) failed to ingest at destination: %s", new_file.rid, error)
        return _IngestOutcome(
            skip_reason=(
                f"upload succeeded but ingest failed at destination: {error}; "
                f"a rerun will not retry it — check {new_file.rid} by hand"
            ),
            failed=True,
        )
    return _IngestOutcome()


def _resolve_destination_file_stem(file_name: str) -> str:
    file_stem = Path(file_name).stem
    _, separator, suffix = file_stem.partition("Z_")
    return suffix if separator else file_stem
