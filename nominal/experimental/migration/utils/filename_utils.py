from pathlib import Path

from nominal.core._utils.filenames import sanitize_upload_filename


def resolve_destination_filename(file_name: str) -> str:
    """Remove the source upload prefix and sanitize the complete basename, preserving its suffixes."""
    path = Path(file_name)
    prefix, separator, _ = path.stem.partition("Z_")
    file_name = path.name
    if separator:
        file_name = file_name.removeprefix(prefix + separator)
    return sanitize_upload_filename(file_name)
