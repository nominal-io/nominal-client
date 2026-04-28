from dataclasses import dataclass


@dataclass(frozen=True)
class AssetInclusionConfig:
    include_video: bool = True
    include_runs: bool = True
    include_events: bool = True
    include_attachments: bool = True
    include_checklists: bool = True
    include_workbooks: bool = True


class MigrationDatasetConfig:
    preserve_dataset_uuid: bool
    include_dataset_files: bool

    def __init__(self, preserve_dataset_uuid: bool, include_dataset_files: bool) -> None:
        """Args:
        preserve_dataset_uuid (bool): If true, preserves the original dataset UUIDs during migration.
        include_dataset_files (bool): If true, includes dataset files in the migration.
        """
        self.preserve_dataset_uuid = preserve_dataset_uuid
        self.include_dataset_files = include_dataset_files
