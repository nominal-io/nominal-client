from __future__ import annotations

import gc
import gzip
import io
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Iterator, cast
from unittest.mock import MagicMock, patch

import pytest
import requests
from urllib3.response import HTTPResponse

from nominal.core._utils.multipart_downloader import (
    DownloadItem,
    MultipartFileDownloader,
    PresignedURLProvider,
    _PlannedDownload,
)
from nominal.exceptions import NominalDownloadRangeError, NominalError


def _provider() -> PresignedURLProvider:
    return PresignedURLProvider(fetch_fn=lambda: "https://example.com/file", ttl_secs=60.0, skew_secs=0.0)


@pytest.fixture
def downloader() -> Iterator[MultipartFileDownloader]:
    """Use the real executor and filesystem, replacing only the storage transport."""
    with MultipartFileDownloader(
        max_workers=1,
        timeout=30.0,
        max_part_retries=3,
        _session=MagicMock(spec=["head", "get", "close"]),
        _pool=ThreadPoolExecutor(max_workers=1),
    ) as downloader:
        yield downloader


@pytest.fixture
def mock_downloader(downloader: MultipartFileDownloader) -> MagicMock:
    """The downloader's session cast to MagicMock, for configuring HTTP responses in _fetch_range_bytes tests."""
    return cast(MagicMock, downloader._session)


@pytest.mark.parametrize("max_part_retries", [0, -1])
def test_downloader_requires_a_positive_attempt_count(max_part_retries: int) -> None:
    """Reject a configuration that could never issue a range request before preparing any files."""
    with pytest.raises(ValueError, match="max_part_retries must be positive"):
        MultipartFileDownloader.create(max_part_retries=max_part_retries)


def test_presigned_url_provider_caches_until_invalidated() -> None:
    """A late rejection of the old URL leaves the refreshed URL available to other workers."""
    calls = 0

    def fetch() -> str:
        nonlocal calls
        calls += 1
        return f"https://example.com/file/{calls}"

    provider = PresignedURLProvider(fetch_fn=fetch, ttl_secs=60.0, skew_secs=0.0)

    assert provider.get_url() == "https://example.com/file/1"
    assert provider.get_url() == "https://example.com/file/1"

    provider.invalidate("https://example.com/file/1")

    assert provider.get_url() == "https://example.com/file/2"
    provider.invalidate("https://example.com/file/1")
    assert provider.get_url() == "https://example.com/file/2"
    assert calls == 2


def test_batch_interleaves_large_small_and_empty_files(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock
) -> None:
    """A small object completes between parts of a large one, and an empty object needs no range request."""
    objects = {"large": b"abcdefghij", "small": b"k", "empty": b""}
    items = [DownloadItem(PresignedURLProvider(lambda name=name: name, 60, 0), tmp_path / name, 4) for name in objects]
    mock_downloader.head.side_effect = lambda url, **_: _mock_response(200, {"Content-Length": str(len(objects[url]))})
    requests_seen = []

    def get(url: str, headers: dict[str, str], **kwargs: object) -> MagicMock:
        start, end = map(int, headers["Range"].removeprefix("bytes=").split("-"))
        requests_seen.append((url, start, end))
        return _mock_response(
            206, {"Content-Range": f"bytes {start}-{end}/{len(objects[url])}"}, content=objects[url][start : end + 1]
        )

    mock_downloader.get.side_effect = get
    results = downloader.download_files(items)
    assert results.failed == {}
    assert [(p.name, p.read_bytes()) for p in results.succeeded] == list(objects.items())
    assert requests_seen == [("large", 0, 3), ("small", 0, 0), ("large", 4, 7), ("large", 8, 9)]


def test_download_files_removes_only_its_failed_artifacts(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock
) -> None:
    """A competing writer keeps its file, a failed transfer is removed, and a successful sibling survives."""
    paths = [tmp_path / name for name in ("claimed", "failed", "success")]
    items = [DownloadItem(PresignedURLProvider(lambda name=p.name: name, 60, 0), p, 4) for p in paths]

    def head(url: str, **kwargs: object) -> MagicMock:
        if url == "claimed":
            paths[0].write_bytes(b"someone else's file")
        return _mock_response(200, {"Content-Length": "4"})

    mock_downloader.head.side_effect = head
    mock_downloader.get.side_effect = [_mock_response(404), _mock_response(200, content=b"data")]
    results = downloader.download_files(items)

    assert results.succeeded == [paths[2]]
    assert isinstance(results.failed[paths[0]], FileExistsError)
    assert isinstance(results.failed[paths[1]], requests.HTTPError)
    assert paths[0].read_bytes() == b"someone else's file"
    assert not paths[1].exists()
    assert paths[2].read_bytes() == b"data"


def test_batch_failure_keeps_the_error_without_pinning_partial_responses(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock
) -> None:
    """Retaining a failed download result releases the partial bodies allocated by its retries."""
    released = []

    class Chunk(bytes):
        def __del__(self) -> None:
            released.append(True)

    mock_downloader.head.return_value = _mock_response(200, {"Content-Length": "4"})
    response = requests.Response()
    response.status_code = 206
    response.headers["Content-Range"] = "bytes 0-3/4"
    response.raw = MagicMock()
    response.raw.stream.side_effect = lambda *args, **kwargs: [Chunk(b"abc")]
    mock_downloader.get.return_value = response
    item = DownloadItem(_provider(), tmp_path / "file")
    result = downloader.download_files([item])

    assert "got 3" in str(result.failed[item.destination])
    gc.collect()
    assert len(released) == downloader.max_part_retries


def test_replacement_during_transfer_is_neither_written_nor_removed(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock
) -> None:
    """Replacing the owned output while its GET runs preserves the replacement and reports failure."""
    dest = tmp_path / "file"
    mock_downloader.head.return_value = _mock_response(200, {"Content-Length": "4"})

    def replace_output(*args: object, **kwargs: object) -> MagicMock:
        replacement = tmp_path / "other"
        replacement.write_bytes(b"someone else's file")
        replacement.replace(dest)
        return _mock_response(206, {"Content-Range": "bytes 0-3/4"}, content=b"data")

    mock_downloader.get.side_effect = replace_output
    with pytest.raises(FileExistsError, match="replaced"):
        downloader.download_file(DownloadItem(_provider(), dest, 4))
    assert dest.read_bytes() == b"someone else's file"
    mock_downloader.get.assert_called_once()


def test_duplicate_download_destination_fails_before_planning(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock
) -> None:
    """Conflicting destinations fail before any objects are probed or created."""
    it = DownloadItem(_provider(), tmp_path / "same")
    with pytest.raises(FileExistsError, match="Duplicate"):
        downloader.download_files([it, it])
    mock_downloader.head.assert_not_called()
    assert not it.destination.exists()


@pytest.mark.parametrize("part_size", [0, -1])
def test_invalid_download_part_size_fails_before_planning(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock, part_size: int
) -> None:
    """Invalid part sizes are rejected before making a remote request or touching the destination."""
    it = DownloadItem(_provider(), tmp_path / "file", part_size)
    with pytest.raises(ValueError, match="part_size"):
        downloader.download_file(it)
    mock_downloader.head.assert_not_called()
    assert not it.destination.exists()


def test_download_file_reports_original_transfer_error(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock
) -> None:
    """Single-file callers receive the storage exception and no partial output."""
    it = DownloadItem(_provider(), tmp_path / "failed", 4)
    mock_downloader.head.return_value = _mock_response(200, {"Content-Length": "8"})
    mock_downloader.get.return_value = _mock_response(404)
    with pytest.raises(requests.HTTPError) as raised:
        downloader.download_file(it)
    assert raised.value.response.status_code == 404
    assert not it.destination.exists()
    mock_downloader.get.assert_called_once()


@pytest.mark.parametrize(
    "status, headers, length",
    [
        (200, {"Content-Length": "1000"}, 1000),
        (206, {"Content-Range": "bytes 0-0/7"}, 7),
        (416, {"Content-Range": "bytes */0"}, 0),
    ],
)
def test_size_probe_streams_only_headers_when_head_is_unavailable(
    downloader: MultipartFileDownloader, mock_downloader: MagicMock, status: int, headers: dict[str, str], length: int
) -> None:
    """A GET probe extracts the representation size without reading its body, including empty objects."""
    mock_downloader.head.return_value = _mock_response(405)
    response = _mock_response(status, headers)
    mock_downloader.get.return_value = response
    assert downloader._head_or_probe(_provider()) == (length, None)
    assert mock_downloader.get.call_args.kwargs["stream"] is True
    response.raw.stream.assert_not_called()
    response.__exit__.assert_called_once()


def test_write_part_raises_on_short_write(tmp_path: Path) -> None:
    """A short filesystem write is reported as an I/O error with the affected offset."""
    path = tmp_path / "file.bin"
    path.write_bytes(b"\x00" * 5)
    data = b"hello"

    mock_fh = MagicMock()
    mock_fh.__enter__ = lambda s: mock_fh
    mock_fh.__exit__ = MagicMock(return_value=False)
    mock_fh.write.return_value = 3  # only 3 of 5 bytes written

    with (
        path.open("rb") as handle,
        patch.object(Path, "open", return_value=mock_fh),
        pytest.raises(OSError, match=r"Short write to .* at offset 0: wrote 3/5 bytes"),
    ):
        mock_fh.fileno.return_value = handle.fileno()
        _plan(path).write(0, bytearray(data))


# ---- _fetch_range_bytes tests ----


def _mock_response(
    status_code: int = 206,
    headers: dict[str, str] | None = None,
    content: bytes = b"",
) -> MagicMock:
    """Build a minimal fake requests.Response for _fetch_range_bytes tests."""
    r = MagicMock()
    r.__enter__.return_value = r
    r.status_code = status_code
    r.ok = 200 <= status_code < 300
    r.headers = headers or {}
    r.raw.stream.return_value = [content] if content else []
    if not r.ok:
        err = requests.HTTPError(response=r)
        r.raise_for_status.side_effect = err
    return r


def _plan(dest: Path, size: int = 4, part_size: int = 4, etag: str | None = None) -> _PlannedDownload:
    if not dest.exists():
        dest.touch()
    return _PlannedDownload(DownloadItem(_provider(), dest, part_size), size, etag, dest.stat())


def test_fetch_range_bytes_raises_on_etag_mismatch(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock
) -> None:
    """An ETag mismatch across parts raises RuntimeError after exhausting all retries."""
    mock_downloader.get.return_value = _mock_response(206, headers={"ETag": "new-etag"}, content=b"data")

    with pytest.raises(RuntimeError, match="ETag mismatch"):
        downloader._fetch_range_bytes(_plan(tmp_path / "file.bin", etag="original-etag"), 0)

    assert mock_downloader.get.call_count == downloader.max_part_retries


def test_fetch_range_bytes_retries_exhausted_on_connection_error(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock
) -> None:
    """A persistent connection error exhausts all retries and re-raises the last exception."""
    mock_downloader.get.side_effect = ConnectionError("timed out")

    with pytest.raises(ConnectionError, match="timed out"):
        downloader._fetch_range_bytes(_plan(tmp_path / "file.bin"), 0)

    assert mock_downloader.get.call_count == downloader.max_part_retries


def test_fetch_range_bytes_stops_retrying_on_permanent_4xx(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock
) -> None:
    """A non-expiry 4xx response (e.g. 404) stops retrying immediately."""
    mock_downloader.get.return_value = _mock_response(404)

    with pytest.raises(requests.HTTPError):
        downloader._fetch_range_bytes(_plan(tmp_path / "file.bin"), 0)

    assert mock_downloader.get.call_count == 1


def test_fetch_range_bytes_invalidates_url_on_expired_response(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock
) -> None:
    """A 403 response causes the provider URL to be invalidated and the request retried."""
    dest = tmp_path / "file.bin"
    dest.write_bytes(b"\x00\x00\x00\x00")

    mock_downloader.get.side_effect = [
        _mock_response(403),
        _mock_response(206, {"Content-Range": "bytes 0-3/4"}, content=b"data"),
    ]

    urls = iter(["expired-url", "fresh-url"])
    item = DownloadItem(PresignedURLProvider(lambda: next(urls), 60, 0), dest)
    downloader._fetch_range_bytes(_PlannedDownload(item, 4, None, dest.stat()), 0)

    assert [call.args[0] for call in mock_downloader.get.call_args_list] == ["expired-url", "fresh-url"]


def test_fetch_range_bytes_stops_at_once_when_the_server_ignores_range(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock
) -> None:
    """A whole-object reply to a part is refused after one attempt, without reading or writing all of it."""
    dest = tmp_path / "file.bin"
    dest.write_bytes(b"\x00" * 8)
    response = _mock_response(200, content=b"abcdefgh")
    mock_downloader.get.return_value = response

    with pytest.raises(NominalDownloadRangeError, match="may not support ranged downloads") as exc_info:
        downloader._fetch_range_bytes(_plan(dest, 8), 4)

    assert mock_downloader.get.call_count == 1
    assert isinstance(exc_info.value, NominalError)
    response.raw.stream.assert_not_called()
    assert dest.read_bytes() == b"\x00" * 8


def test_fetch_range_bytes_retries_a_truncated_body(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock
) -> None:
    """A short body may be transient, so it is retried, and never written at the part's offset."""
    dest = tmp_path / "file.bin"
    dest.write_bytes(b"\x00" * 8)
    mock_downloader.get.return_value = _mock_response(206, {"Content-Range": "bytes 4-7/8"}, content=b"ef")

    with pytest.raises(RuntimeError, match="Expected 4 bytes for range 4-7, got 2"):
        downloader._fetch_range_bytes(_plan(dest, 8), 4)

    assert mock_downloader.get.call_count == downloader.max_part_retries
    assert dest.read_bytes() == b"\x00" * 8


@pytest.mark.parametrize(
    "status,start,end,body,expected",
    [
        (200, 0, 3, b"data", b"data"),
        (206, 3, 3, b"d", b"\x00\x00\x00d"),
    ],
)
def test_fetch_range_bytes_accepts_complete_body_and_short_final_part(
    tmp_path: Path,
    downloader: MultipartFileDownloader,
    mock_downloader: MagicMock,
    status: int,
    start: int,
    end: int,
    body: bytes,
    expected: bytes,
) -> None:
    """Length validation accepts a full-file 200 response and a shorter final byte range."""
    dest = tmp_path / "file.bin"
    dest.write_bytes(b"\x00" * 4)
    mock_downloader.get.return_value = _mock_response(status, {"Content-Range": f"bytes {start}-{end}/4"}, content=body)

    downloader._fetch_range_bytes(_plan(dest, 4, end - start + 1), start)

    assert dest.read_bytes() == expected
    assert mock_downloader.get.call_count == 1


def test_partial_response_can_omit_the_total_size(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock
) -> None:
    """A valid unknown representation length still identifies the exact range from the download plan."""
    dest = tmp_path / "file.bin"
    dest.write_bytes(b"01234567")
    mock_downloader.get.return_value = _mock_response(206, {"Content-Range": "bytes 4-7/*"}, content=b"data")
    downloader._fetch_range_bytes(_plan(dest, 8), 4)
    assert dest.read_bytes() == b"0123data"


@pytest.mark.parametrize("split", [False, True], ids=["single-part", "multiple-parts"])
def test_encoded_download_preserves_stored_bytes(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock, split: bool
) -> None:
    """Ranges count encoded bytes; gzip responses must be assembled without per-part decompression."""
    stored = gzip.compress(b"readings\n" * 1024)
    destination = tmp_path / "readings.gz"
    destination.write_bytes(b"\x00" * len(stored))
    part_size = len(stored) // 2 if split else len(stored)
    ranges = [(start, min(start + part_size, len(stored))) for start in range(0, len(stored), part_size)]
    responses = []
    for start, stop in ranges:
        response = requests.Response()
        response.status_code = 206
        response.headers["Content-Range"] = f"bytes {start}-{stop - 1}/{len(stored)}"
        response.headers["Content-Encoding"] = "gzip"
        response.headers["Content-Length"] = str(stop - start)
        response.raw = HTTPResponse(
            body=io.BytesIO(stored[start:stop]),
            headers=response.headers,
            preload_content=False,
        )
        responses.append(response)
    mock_downloader.get.side_effect = responses

    for start, stop in ranges:
        downloader._fetch_range_bytes(_plan(destination, len(stored), part_size), start)

    assert destination.read_bytes() == stored
    assert mock_downloader.get.call_count == len(ranges)


@pytest.mark.parametrize("content_range", ["bytes 0-3/8", "bytes 4-7/10", None])
def test_wrong_partial_response_never_writes_into_a_different_offset(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock, content_range: str | None
) -> None:
    """A same-length 206 must identify the exact requested interval and object size."""
    dest = tmp_path / "file"
    dest.write_bytes(b"abcdefgh")
    headers = {} if content_range is None else {"Content-Range": content_range}
    response = _mock_response(206, headers, content=b"oops")
    mock_downloader.get.return_value = response
    with pytest.raises(NominalDownloadRangeError, match="Content-Range"):
        downloader._fetch_range_bytes(_plan(dest, 8), 4)
    assert dest.read_bytes() == b"abcdefgh"
    mock_downloader.get.assert_called_once()
