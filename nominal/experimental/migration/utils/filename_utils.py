from pathlib import Path

from nominal.core._utils.filenames import sanitize_upload_filename


def resolve_destination_filename(file_name: str) -> str:
    """Remove the source upload prefix and sanitize the complete basename, preserving its suffixes."""
    file_name = sanitize_upload_filename(file_name)
    prefix, separator, _ = Path(file_name).stem.partition("Z_")
    if separator:
        file_name = file_name.removeprefix(prefix + separator)
    return file_name
