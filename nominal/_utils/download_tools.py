import logging
import pathlib
import shutil
from urllib.parse import unquote, urlparse

import requests
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)


def filename_from_uri(uri: str) -> str:
    return unquote(pathlib.Path(urlparse(uri).path).name).replace(":", "_")


def download_presigned_uri(
    uri: str,
    destination: pathlib.Path,
    staleness_timeout: float = 30.0,
    force: bool = False,
) -> None:
    if not destination.parent.exists():
        if force:
            destination.parent.mkdir(exist_ok=True, parents=True)
        else:
            raise FileNotFoundError(f"Output directory does not exist and force=False: {destination.parent}")

    if destination.exists():
        if destination.is_dir():
            raise ValueError(f"Destination {destination} already exists as a directory-- cannot download {uri}")
        elif force:
            destination.unlink()
        else:
            raise FileExistsError(f"Cannot download {uri} => {destination}-- already exists and force=False")

    try:
        with destination.open("wb") as wf:
            with requests.get(uri, stream=True, timeout=staleness_timeout) as response:
                response.raise_for_status()
                shutil.copyfileobj(response.raw, wf)

                # Successfully downloaded file
                return
    except RequestException as ex:
        raise RuntimeError(f"Failed downloading {uri} => {destination}") from ex
