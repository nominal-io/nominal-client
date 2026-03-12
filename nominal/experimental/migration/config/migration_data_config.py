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
