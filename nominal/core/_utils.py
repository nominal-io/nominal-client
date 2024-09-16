from pathlib import Path

from .._utils import FileType
from .attachment import Attachment
from .dataset import Dataset
from .run import Run
from .video import Video


def rid_from_instance_or_string(value: Attachment | Run | Dataset | Video | str) -> str:
    if isinstance(value, str):
        return value
    elif isinstance(value, (Attachment, Run, Dataset, Video)):
        return value.rid
    elif hasattr(value, "rid"):
        return value.rid
    raise TypeError("{value!r} is not a string nor has the attribute 'rid'")


def verify_csv_path(path: Path | str) -> tuple[Path, FileType]:
    path = Path(path)
    file_type = FileType.from_path_dataset(path)
    if file_type.extension not in (".csv", ".csv.gz"):
        raise ValueError(f"file {path} must end with '.csv' or '.csv.gz'")
    return path, file_type
