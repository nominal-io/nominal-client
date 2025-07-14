import pathlib
from urllib.parse import unquote, urlparse

import httpx


def download_presigned_uri(
    uri: str, destination: pathlib.Path, chunk_size: int = 8192, staleness_timeout: float = 30.0
) -> pathlib.Path:
    if destination.exists():
        if destination.is_dir():
            filename = unquote(pathlib.Path(urlparse(uri).path).name)
            destination = destination / filename
        else:
            raise FileExistsError(f"Cannot download {uri} to {destination}-- already exists!")
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)

    try:
        with httpx.stream("GET", uri, timeout=staleness_timeout) as response:
            response.raise_for_status()
            with destination.open("wb") as f:
                for chunk in response.iter_bytes(chunk_size=chunk_size):
                    f.write(chunk)

        return destination
    except httpx.RequestError as ex:
        raise RuntimeError(f"Failed downloading {uri} => {destination}") from ex
