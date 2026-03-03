from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Sequence

from nominal.core.video import Video
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.utils.video_file_utils import copy_video_file_to_video_dataset

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VideoCopyOptions(ResourceCopyOptions):
    new_video_name: str | None = None
    new_video_description: str | None = None
    new_video_properties: dict[str, Any] | None = None
    new_video_labels: Sequence[str] | None = None
    include_files: bool = False


class VideoMigrator(Migrator[Video, Video, VideoCopyOptions]):
    def clone(self, source: Video) -> Video:
        return self.copy_from(source, VideoCopyOptions(include_files=True))

    def copy_from(self, source: Video, options: VideoCopyOptions) -> Video:
        log_extras = {
            "destination_client_workspace": self.ctx.destination_client.get_workspace(
                self.ctx.destination_client._clients.workspace_rid
            ).rid
        }
        logger.debug(
            "Copying video %s (rid: %s)",
            source.name,
            source.rid,
            extra=log_extras,
        )
        result = self.ctx.destination_client.create_video(
            name=options.new_video_name if options.new_video_name is not None else source.name,
            description=options.new_video_description
            if options.new_video_description is not None
            else source.description,
            properties=options.new_video_properties if options.new_video_properties is not None else source.properties,
            labels=options.new_video_labels if options.new_video_labels is not None else source.labels,
        )
        if options.include_files:
            for source_file in source.list_files():
                copy_video_file_to_video_dataset(source_file, result)
        logger.debug(
            "New video created: %s (rid: %s)",
            result.name,
            result.rid,
            extra=log_extras,
        )
        self.record_mapping("VIDEO", source.rid, result.rid)
        return result
