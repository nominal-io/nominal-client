from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from nominal.core.video import Video
from nominal.experimental.migration.migrator.base import Migrator, ResourceCopyOptions
from nominal.experimental.migration.resource_type import ResourceType
from nominal.experimental.migration.utils.video_file_utils import copy_video_file_to_video_dataset


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
        return result

    def _get_resource_name(self, resource: Video) -> str:
        return resource.name
