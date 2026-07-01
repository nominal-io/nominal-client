from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable, Iterator, Sequence, cast
from unittest.mock import MagicMock, patch

import pytest
import requests

from nominal.core._utils.multipart_downloader import (
    DownloadItem,
    MultipartFileDownloader,
    PresignedURL,
    PresignedURLProvider,
    _DataChunkBounds,
    _PlannedDownload,
)


def _provider(presigned: PresignedURL | None = None) -> PresignedURLProvider:
    presigned = presigned or PresignedURL(url="https://example.com/file")
    return PresignedURLProvider(fetch_fn=lambda: presigned, ttl_secs=60.0, skew_secs=0.0)


@pytest.fixture
def downloader() -> Iterator[MultipartFileDownloader]:
    # A real pool is needed because download_files now plans (and pre-allocates) on the pool in
    # parallel; the HTTP session stays mocked since these tests patch _plan_item / _run_downloads.
    pool = ThreadPoolExecutor(max_workers=2)
    try:
        yield MultipartFileDownloader(
            max_workers=2,
            timeout=30.0,
            max_part_retries=3,
            _session=MagicMock(spec=["head", "get", "close"]),
            _pool=pool,
        )
    finally:
        pool.shutdown(wait=True)


@pytest.fixture
def mock_downloader(downloader: MultipartFileDownloader) -> MagicMock:
    """The downloader's session cast to MagicMock, for configuring HTTP responses in _fetch_range_bytes tests."""
    return cast(MagicMock, downloader._session)


def test_presigned_url_provider_caches_until_invalidated() -> None:
    """URL is reused across calls until invalidate() is called, then a fresh URL is fetched."""
    calls = 0

    def fetch() -> PresignedURL:
        nonlocal calls
        calls += 1
        return PresignedURL(url=f"https://example.com/file/{calls}")

    provider = PresignedURLProvider(fetch_fn=fetch, ttl_secs=60.0, skew_secs=0.0)

    assert provider.get_url() == "https://example.com/file/1"
    assert provider.get_url() == "https://example.com/file/1"

    provider.invalidate()

    assert provider.get_url() == "https://example.com/file/2"
    assert calls == 2


def test_planned_download_ranges_partial_final_chunk(tmp_path: Path) -> None:
    """A file size that is not a multiple of part_size produces a shorter final chunk."""
    plan = _PlannedDownload(
        item=DownloadItem(provider=_provider(), destination=tmp_path / "file.bin", part_size=5),
        total_size=12,
        etag="etag",
    )

    assert list(plan.ranges()) == [
        _DataChunkBounds(index=0, start_bytes=0, end_bytes=4),
        _DataChunkBounds(index=1, start_bytes=5, end_bytes=9),
        _DataChunkBounds(index=2, start_bytes=10, end_bytes=11),
    ]


def test_planned_download_ranges_exact_multiple_fit(tmp_path: Path) -> None:
    """A file size that is an exact multiple of part_size produces no partial final chunk."""
    plan = _PlannedDownload(
        item=DownloadItem(provider=_provider(), destination=tmp_path / "file.bin", part_size=5),
        total_size=10,
        etag="etag",
    )

    assert list(plan.ranges()) == [
        _DataChunkBounds(index=0, start_bytes=0, end_bytes=4),
        _DataChunkBounds(index=1, start_bytes=5, end_bytes=9),
    ]


def test_download_file_returns_path_on_success(tmp_path: Path, downloader: MultipartFileDownloader) -> None:
    """download_file returns the destination path and the preallocated file exists on success."""
    item = DownloadItem(provider=_provider(), destination=tmp_path / "ok.bin", part_size=4)

    with (
        patch.object(
            downloader, "_plan_item", lambda item, session=None: _PlannedDownload(item=item, total_size=4, etag=None)
        ),
        patch.object(downloader, "_run_downloads", lambda plans, *, collect_errors: {}),
    ):
        result = downloader.download_file(item)

    assert result == item.destination
    assert result.exists()


def test_download_file_raises_on_execution_failure(tmp_path: Path, downloader: MultipartFileDownloader) -> None:
    """download_file raises the captured exception and cleans up the destination on failure."""
    item = DownloadItem(provider=_provider(), destination=tmp_path / "failed.bin", part_size=4)
    error = RuntimeError("download failed")

    with (
        patch.object(
            downloader, "_plan_item", lambda item, session=None: _PlannedDownload(item=item, total_size=4, etag=None)
        ),
        patch.object(downloader, "_run_downloads", lambda plans, *, collect_errors: {item.destination: error}),
        pytest.raises(RuntimeError, match="download failed"),
    ):
        downloader.download_file(item)
    assert not item.destination.exists()


def test_download_overwrites_existing_destination(tmp_path: Path, downloader: MultipartFileDownloader) -> None:
    """An existing destination is overwritten, not rejected.

    Re-downloading to the same path (e.g. a channel-sync retry or a repeated download into a kept
    output_dir) must not raise FileExistsError: _plan_and_preallocate unlinks the stale file and
    re-preallocates it to the planned size.
    """
    dest = tmp_path / "exists.bin"
    dest.write_bytes(b"stale-leftover-content-from-a-prior-download")  # 44 bytes

    item = DownloadItem(provider=_provider(), destination=dest, part_size=4)
    with (
        patch.object(
            downloader, "_plan_item", lambda item, session=None: _PlannedDownload(item=item, total_size=4, etag=None)
        ),
        patch.object(downloader, "_run_downloads", lambda plans, *, collect_errors: {}),
    ):
        results = downloader.download_files([item])

    assert list(results.succeeded) == [dest]
    assert results.failed == {}
    # The stale 44-byte file was cleared and re-preallocated to the planned 4 bytes (overwritten).
    assert dest.stat().st_size == 4


def test_download_files_execution_failure_excluded_from_succeeded(
    tmp_path: Path, downloader: MultipartFileDownloader
) -> None:
    """Execution failure is reported in failed, excluded from succeeded, and its file is cleaned up."""
    succeeded_item = DownloadItem(provider=_provider(), destination=tmp_path / "ok.bin", part_size=4)
    failed_item = DownloadItem(provider=_provider(), destination=tmp_path / "failed.bin", part_size=4)
    error = RuntimeError("download failed")

    def _make_plan(item: DownloadItem, session: object = None) -> _PlannedDownload:
        return _PlannedDownload(item=item, total_size=4, etag=None)

    def _exec_downloads(plans: Sequence[_PlannedDownload], *, collect_errors: bool) -> dict[Path, Exception]:
        assert collect_errors is True
        assert [p.item.destination for p in plans] == [succeeded_item.destination, failed_item.destination]
        return {failed_item.destination: error}

    with (
        patch.object(downloader, "_plan_item", _make_plan),
        patch.object(downloader, "_run_downloads", _exec_downloads),
    ):
        results = downloader.download_files([succeeded_item, failed_item])

    assert list(results.succeeded) == [succeeded_item.destination]
    assert results.failed == {failed_item.destination: error}
    assert succeeded_item.destination.exists()
    assert not failed_item.destination.exists()


def test_download_files_planning_failure_excluded_from_succeeded(
    tmp_path: Path, downloader: MultipartFileDownloader
) -> None:
    """Planning failure is reported in failed, excluded from succeeded, and no file is created."""
    item = DownloadItem(provider=_provider(), destination=tmp_path / "file.bin", part_size=4)
    error = RuntimeError("head request failed")

    def _failing_plan(_item: DownloadItem, _session: object = None) -> _PlannedDownload:
        raise error

    with patch.object(downloader, "_plan_item", _failing_plan):
        results = downloader.download_files([item])

    assert list(results.succeeded) == []
    assert results.failed == {item.destination: error}
    assert not item.destination.exists()


# ---- download_files_pipelined tests ----


def _plan_returning(total_size: int) -> Callable[..., _PlannedDownload]:
    """A _plan_item stand-in that returns a fixed-size plan (ignoring the link/probe)."""

    def _plan(item: DownloadItem, session: object = None) -> _PlannedDownload:
        return _PlannedDownload(item=item, total_size=total_size, etag=None)

    return _plan


def test_pipelined_fires_callbacks_once_per_file(tmp_path: Path, downloader: MultipartFileDownloader) -> None:
    """Each file is reported planned once and complete once; all files succeed."""
    items = [DownloadItem(provider=_provider(), destination=tmp_path / f"f{i}.bin", part_size=4) for i in range(3)]
    planned: list[Path] = []
    completed: list[Path] = []

    with (
        patch.object(downloader, "_plan_item", _plan_returning(8)),  # 8 bytes / 4 = 2 parts each
        patch.object(downloader, "_fetch_range_bytes", lambda *a, **k: None),
    ):
        results = downloader.download_files_pipelined(
            items, on_file_planned=planned.append, on_file_complete=completed.append
        )

    assert sorted(results.succeeded) == sorted(it.destination for it in items)
    assert results.failed == {}
    assert sorted(planned) == sorted(it.destination for it in items)
    assert sorted(completed) == sorted(it.destination for it in items)


def test_pipelined_part_failure_recorded_and_cleaned_up(tmp_path: Path, downloader: MultipartFileDownloader) -> None:
    """A part failure fails only its file (recorded + artifact deleted); other files still succeed."""
    ok = DownloadItem(provider=_provider(), destination=tmp_path / "ok.bin", part_size=4)
    bad = DownloadItem(provider=_provider(), destination=tmp_path / "bad.bin", part_size=4)

    def _fetch(provider: object, start: int, end: int, etag: str | None, destination: Path) -> None:
        if destination == bad.destination:
            raise RuntimeError("boom")

    with (
        patch.object(downloader, "_plan_item", _plan_returning(8)),
        patch.object(downloader, "_fetch_range_bytes", _fetch),
    ):
        results = downloader.download_files_pipelined([ok, bad])

    assert list(results.succeeded) == [ok.destination]
    assert bad.destination in results.failed
    assert ok.destination.exists()
    assert not bad.destination.exists()


def test_pipelined_planning_failure_recorded(tmp_path: Path, downloader: MultipartFileDownloader) -> None:
    """A planning failure is recorded in failed and the file never downloads."""
    item = DownloadItem(provider=_provider(), destination=tmp_path / "f.bin", part_size=4)
    error = RuntimeError("link generation failed")

    def _failing_plan(_item: DownloadItem, _session: object = None) -> _PlannedDownload:
        raise error

    with patch.object(downloader, "_plan_item", _failing_plan):
        results = downloader.download_files_pipelined([item])

    assert list(results.succeeded) == []
    assert results.failed == {item.destination: error}
    assert not item.destination.exists()


# ---- _plan_item PresignedURL passthrough tests ----


def test_plan_item_skips_probe_when_size_known(tmp_path: Path, downloader: MultipartFileDownloader) -> None:
    """When the provider already knows the object size, planning uses it and skips the HEAD/GET probe."""
    provider = _provider(PresignedURL(url="https://example.com/f", total_size=123, etag="abc"))
    item = DownloadItem(provider=provider, destination=tmp_path / "f.bin", part_size=4)

    with patch.object(downloader, "_head_or_probe", side_effect=AssertionError("should not probe")) as probe:
        plan = downloader._plan_item(item)

    assert plan.total_size == 123
    assert plan.etag == "abc"
    probe.assert_not_called()


def test_plan_item_probes_when_size_unknown(tmp_path: Path, downloader: MultipartFileDownloader) -> None:
    """When the provider doesn't know the size, planning falls back to the HEAD/GET probe."""
    item = DownloadItem(provider=_provider(), destination=tmp_path / "f.bin", part_size=4)

    with patch.object(downloader, "_head_or_probe", return_value=(64, "etag")) as probe:
        plan = downloader._plan_item(item)

    assert plan.total_size == 64
    assert plan.etag == "etag"
    probe.assert_called_once()


# ---- _write_part tests ----


def test_write_part_raises_on_short_write(tmp_path: Path) -> None:
    """_write_part raises OSError when the OS reports fewer bytes written than supplied."""
    path = tmp_path / "file.bin"
    path.write_bytes(b"\x00" * 5)
    data = b"hello"

    mock_fh = MagicMock()
    mock_fh.__enter__ = lambda s: mock_fh
    mock_fh.__exit__ = MagicMock(return_value=False)
    mock_fh.write.return_value = 3  # only 3 of 5 bytes written

    with (
        patch.object(Path, "open", return_value=mock_fh),
        pytest.raises(OSError, match=r"Short write to .* at offset 0: wrote 3/5 bytes"),
    ):
        MultipartFileDownloader._write_part(path, 0, data)


# ---- _fetch_range_bytes tests ----


def _mock_response(
    status_code: int = 206,
    headers: dict[str, str] | None = None,
    content: bytes = b"",
) -> MagicMock:
    """Build a minimal fake requests.Response for _fetch_range_bytes tests."""
    r = MagicMock()
    r.status_code = status_code
    r.ok = 200 <= status_code < 300
    r.headers = headers or {}
    r.iter_content.return_value = [content] if content else []
    if not r.ok:
        err = requests.HTTPError(response=r)
        r.raise_for_status.side_effect = err
    return r


def test_fetch_range_bytes_raises_on_etag_mismatch(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock
) -> None:
    """An ETag mismatch across parts raises RuntimeError after exhausting all retries."""
    mock_downloader.get.return_value = _mock_response(206, headers={"ETag": "new-etag"}, content=b"data")

    with pytest.raises(RuntimeError, match="ETag mismatch"):
        downloader._fetch_range_bytes(_provider(), 0, 3, "original-etag", tmp_path / "file.bin")

    assert mock_downloader.get.call_count == downloader.max_part_retries


def test_fetch_range_bytes_retries_exhausted_on_connection_error(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock
) -> None:
    """A persistent connection error exhausts all retries and re-raises the last exception."""
    mock_downloader.get.side_effect = ConnectionError("timed out")

    with pytest.raises(ConnectionError, match="timed out"):
        downloader._fetch_range_bytes(_provider(), 0, 3, None, tmp_path / "file.bin")

    assert mock_downloader.get.call_count == downloader.max_part_retries


def test_fetch_range_bytes_stops_retrying_on_permanent_4xx(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock
) -> None:
    """A non-expiry 4xx response (e.g. 404) stops retrying immediately."""
    mock_downloader.get.return_value = _mock_response(404)

    with pytest.raises(requests.HTTPError):
        downloader._fetch_range_bytes(_provider(), 0, 3, None, tmp_path / "file.bin")

    assert mock_downloader.get.call_count == 1


def test_fetch_range_bytes_invalidates_url_on_expired_response(
    tmp_path: Path, downloader: MultipartFileDownloader, mock_downloader: MagicMock
) -> None:
    """A 403 response causes the provider URL to be invalidated and the request retried."""
    dest = tmp_path / "file.bin"
    dest.write_bytes(b"\x00\x00\x00\x00")

    mock_downloader.get.side_effect = [_mock_response(403), _mock_response(206, content=b"data")]

    provider = _provider()
    with patch.object(provider, "invalidate", wraps=provider.invalidate) as mock_invalidate:
        downloader._fetch_range_bytes(provider, 0, 3, None, dest)

    mock_invalidate.assert_called_once()
    assert mock_downloader.get.call_count == 2
