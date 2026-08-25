from __future__ import annotations

import logging

from nominal.core.video import Video
from nominal.core.video_file import VideoFile
from nominal.experimental.migration.dry_run import DRY_RUN_PREFIX
from nominal.experimental.migration.migrator.context import MigrationContext
from nominal.experimental.migration.resource_type import ResourceType
from nominal.experimental.migration.utils.video_file_utils import copy_video_file_to_video_dataset

logger = logging.getLogger(__name__)


class VideoFileMigrator:
    def __init__(self, ctx: MigrationContext) -> None:
        """Constructs a VideoFileMigrator with the given MigrationContext."""
        self.ctx = ctx

    def copy_from(self, source_file: VideoFile, destination_video: Video) -> None:
        mapped_rid = self.ctx.migration_state.get_mapped_rid(ResourceType.VIDEO_FILE, source_file.rid)
        if mapped_rid is not None:
            logger.debug("Skipping video file (rid: %s): already in migration state", source_file.rid)
            return

        if self.ctx.dry_run:
            logger.info(f"{DRY_RUN_PREFIX} Would copy video file %s to destination", source_file.rid)
            return

        try:
            outcome = copy_video_file_to_video_dataset(
                source_file, destination_video, poll_timeout=self.ctx.video_ingest_timeout
            )
        except Exception as error:
            # One bad file must not abort the whole asset task (and every sibling resource
            # behind it). With no mapping recorded, a rerun re-attempts this file.
            logger.exception("Failed to copy video file (rid: %s)", source_file.rid)
            self.ctx.migration_state.set_skip(ResourceType.VIDEO_FILE, source_file.rid, f"copy failed: {error}")
            return

        if outcome.file is not None:
            self.ctx.migration_state.record_mapping(ResourceType.VIDEO_FILE, source_file.rid, outcome.file.rid)
        # This attempt's outcome supersedes any skip recorded by an earlier run's attempt.
        # Ordered after record_mapping — each call persists separately, so a crash between the
        # two can only lose the summary line, never the mapping that stops a rerun re-uploading.
        self.ctx.migration_state.set_skip(ResourceType.VIDEO_FILE, source_file.rid, outcome.skip_reason)
