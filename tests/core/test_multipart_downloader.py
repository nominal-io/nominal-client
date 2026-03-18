from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from nominal.core._utils.multipart_downloader import (
    DownloadItem,
    MultipartFileDownloader,
    PresignedURLProvider,
    _DataChunkBounds,
    _PlannedDownload,
)


def _provider() -> PresignedURLProvider:
    return PresignedURLProvider(fetch_fn=lambda: "https://example.com/file", ttl_secs=60.0, skew_secs=0.0)


@pytest.fixture
def downloader() -> MultipartFileDownloader:
    return MultipartFileDownloader(
        max_workers=1,
        timeout=30.0,
        max_part_retries=3,
        _session=MagicMock(spec=["head", "get", "close"]),
        _pool=MagicMock(spec=["submit", "shutdown"]),
    )


def test_presigned_url_provider_caches_until_invalidated() -> None:
    """URL is reused across calls until invalidate() is called, then a fresh URL is fetched."""
    calls = 0

    def fetch() -> str:
        nonlocal calls
        calls += 1
        return f"https://example.com/file/{calls}"

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

    downloader._plan_item = lambda it: _PlannedDownload(item=it, total_size=4, etag=None)
    downloader._run_downloads = lambda plans, *, collect_errors: {}

    result = downloader.download_file(item)

    assert result == item.destination
    assert result.exists()


def test_download_file_raises_on_execution_failure(tmp_path: Path, downloader: MultipartFileDownloader) -> None:
    """download_file raises the captured exception and cleans up the destination on failure."""
    item = DownloadItem(provider=_provider(), destination=tmp_path / "failed.bin", part_size=4)

    downloader._plan_item = lambda it: _PlannedDownload(item=it, total_size=4, etag=None)
    downloader._run_downloads = lambda plans, *, collect_errors: {item.destination: RuntimeError("download failed")}

    with pytest.raises(RuntimeError, match="download failed"):
        downloader.download_file(item)
    assert not item.destination.exists()


def test_download_files_execution_failure_excluded_from_succeeded(
    tmp_path: Path, downloader: MultipartFileDownloader
) -> None:
    """Execution failure is reported in failed, excluded from succeeded, and its file is cleaned up."""
    succeeded_item = DownloadItem(provider=_provider(), destination=tmp_path / "ok.bin", part_size=4)
    failed_item = DownloadItem(provider=_provider(), destination=tmp_path / "failed.bin", part_size=4)
    error = RuntimeError("download failed")

    def _make_plan(item: DownloadItem) -> _PlannedDownload:
        return _PlannedDownload(item=item, total_size=4, etag=None)

    def _exec_downloads(plans: list[_PlannedDownload], *, collect_errors: bool) -> dict[Path, Exception]:
        assert collect_errors is True
        assert [p.item.destination for p in plans] == [succeeded_item.destination, failed_item.destination]
        return {failed_item.destination: error}

    downloader._plan_item = _make_plan
    downloader._run_downloads = _exec_downloads

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

    def _failing_plan(_: DownloadItem) -> _PlannedDownload:
        raise error

    downloader._plan_item = _failing_plan

    results = downloader.download_files([item])

    assert list(results.succeeded) == []
    assert results.failed == {item.destination: error}
    assert not item.destination.exists()
