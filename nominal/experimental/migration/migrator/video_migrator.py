from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Sequence

from nominal.core.video import Video
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.migrator.video_file_migrator import VideoFileMigrator
from nominal.experimental.migration.resource_type import ResourceType

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VideoCopyOptions(ResourceCopyOptions):
    new_video_name: str | None = None
    new_video_description: str | None = None
    new_video_properties: dict[str, Any] | None = None
    new_video_labels: Sequence[str] | None = None
    include_files: bool = False


class VideoMigrator(Migrator[Video, VideoCopyOptions]):
    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.VIDEO

    def default_copy_options(self) -> VideoCopyOptions:
        return VideoCopyOptions(include_files=True)

    def _copy_from_impl(self, source: Video, options: VideoCopyOptions) -> Video:
        result = self._resolve_destination_video(source, options)

        if options.include_files:
            file_migrator = VideoFileMigrator(self.ctx)
            for source_file in source.list_files():
                file_migrator.copy_from(source_file, result)

        return result

    def _resolve_destination_video(self, source: Video, options: VideoCopyOptions) -> Video:
        mapped_rid = self.ctx.migration_state.get_mapped_rid(self.resource_type, source.rid)
        if mapped_rid is not None:
            logger.debug("Skipping %s (rid: %s): already in migration state", self.resource_label, source.rid)
            return self.ctx.destination_client.get_video(mapped_rid)
        new_video = self.ctx.destination_client.create_video(
            name=options.new_video_name if options.new_video_name is not None else source.name,
            description=options.new_video_description
            if options.new_video_description is not None
            else source.description,
            properties=options.new_video_properties if options.new_video_properties is not None else source.properties,
            labels=options.new_video_labels if options.new_video_labels is not None else source.labels,
        )
        self.ctx.migration_state.record_mapping(self.resource_type, source.rid, new_video.rid)
        return new_video

    def _get_resource_name(self, resource: Video) -> str:
        return resource.name
