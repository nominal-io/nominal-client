from __future__ import annotations

import logging

from nominal.core.video import Video
from nominal.core.video_file import VideoFile
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

        new_file = copy_video_file_to_video_dataset(source_file, destination_video)
        if new_file is not None:
            self.ctx.migration_state.record_mapping(ResourceType.VIDEO_FILE, source_file.rid, new_file.rid)
