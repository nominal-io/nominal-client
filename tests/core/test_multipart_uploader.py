from __future__ import annotations

import logging
import pathlib
import threading
from concurrent.futures import CancelledError, Future, wait
from types import SimpleNamespace
from typing import Any, Callable, Iterator
from unittest.mock import MagicMock

import pytest
import requests

from nominal.core._utils.upload_pacing import _ThrottleGate
from nominal.core.filetype import FileType, FileTypes
from nominal.core.multipart_uploader import (
    DEFAULT_SMALL_FILE_ROUTE_MAX_BYTES,
    MAX_SMALL_FILE_ROUTE_BYTES,
    MultipartUploader,
)
from nominal.exceptions import (
    NominalMultipartUploadError,
    NominalMultipartUploadFailed,
    NominalRequestThrottledError,
)

WriteFile = Callable[[str, int], pathlib.Path]
MakeUploader = Callable[..., "tuple[MultipartUploader, FakeUploadService, FakePutSession]"]


class FakeClock:
    """Deterministic clock whose sleep advances time — no real sleeping in fake-time tests."""

    def __init__(self) -> None:
        """Start at time zero with no sleeps recorded."""
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def fake_clock() -> FakeClock:
    """A fresh deterministic clock for tests that drive time seams directly."""
    return FakeClock()


@pytest.fixture
def install_test_gate() -> Callable[..., FakeClock]:
    """Factory swapping an uploader's gate for one on a fake clock: retries instant and countable.

    The lane is made wide enough that admission never blocks; the gate's user-visible behavior
    (throttle absorption, budgets, the concurrency cap) is pinned through the uploader tests.
    """

    def _install(up: MultipartUploader, *, deadline_seconds: float = 120.0) -> FakeClock:
        clock = FakeClock()
        up._streams.gate = _ThrottleGate(
            max_concurrency=1000,
            deadline_seconds=deadline_seconds,
            clock=clock,
            sleep=lambda delay, _: clock.sleep(delay),
            jitter=lambda d: d,
        )
        return clock

    return _install


class FakeUploadService:
    """Counts calls; optional per-method gates let tests hold a call open.

    The object key is the request filename so a test can target one file out of a batch by
    name — a counter would race, since initiates run concurrently.
    """

    def __init__(self, *, fail_sign_for_key: str | None = None, throttle_once_at: str | None = None) -> None:
        """Start with every gate open, so a test opts in to holding a call."""
        self.lock = threading.Lock()
        self.calls: list[str] = []
        self.throttle_once_at = throttle_once_at
        self.fail_sign_for_key = fail_sign_for_key
        self.aborted: list[str] = []
        self.completed_etags: dict[str, dict[int, str]] = {}  # key -> {part number: etag}
        self.upload_file_args: list[tuple[str, int | None, int]] = []  # (file name, size_bytes, body length)
        self.upload_file_started = threading.Event()
        self.upload_file_release = threading.Event()
        self.upload_file_release.set()
        self._verify = False

    def _record(self, name: str) -> None:
        with self.lock:
            self.calls.append(name)
            if self.throttle_once_at == name:
                self.throttle_once_at = None
                raise _throttled()

    def initiate_multipart_upload(self, auth: str, request: Any) -> Any:
        self._record("initiate")
        return SimpleNamespace(key=request.filename, upload_id=f"uid-{request.filename}", bucket="uploads")

    def sign_part(self, auth: str, key: str, part: int, upload_id: str, bucket: str | None = None) -> Any:
        self._record("sign")
        if self.fail_sign_for_key is not None and key == self.fail_sign_for_key:
            raise ConnectionError(f"sign failed for {key}")
        return SimpleNamespace(url=f"https://s3.example/{key}/{part}", headers={})

    def complete_multipart_upload(
        self, auth: str, key: str, upload_id: str, parts: Any, bucket: str | None = None
    ) -> Any:
        self._record("complete")
        with self.lock:
            self.completed_etags[key] = {p.part_number: p.etag for p in parts}
        return SimpleNamespace(location=f"s3://bucket/{key}")

    def abort_multipart_upload(self, auth: str, key: str, upload_id: str, bucket: str | None = None) -> None:
        self._record("abort")
        with self.lock:
            self.aborted.append(key)

    def list_parts(self, auth: str, key: str, upload_id: str) -> None:
        raise AssertionError("list_parts must not be called: etags come from the PUT responses")

    def upload_file(
        self, auth: str, body: bytes, file_name: str, size_bytes: int | None = None, workspace: str | None = None
    ) -> str:
        self._record("upload_file")
        with self.lock:
            self.upload_file_args.append((file_name, size_bytes, len(body)))
        self.upload_file_started.set()
        self.upload_file_release.wait(timeout=10)
        return f"s3://bucket/{file_name}"


class FakePutSession:
    """Stands in for the direct-to-storage PUT session; `put_release` parks a part thread."""

    def __init__(self) -> None:
        """Start with the PUT gate open, so a test opts in to holding a PUT."""
        self.put_started = threading.Event()
        self.put_release = threading.Event()
        self.put_release.set()
        self.closed = False

    def put(self, url: str, data: Any = None, headers: Any = None, verify: Any = None, timeout: Any = None) -> Any:
        self.put_started.set()
        self.put_release.wait(timeout=10)
        return SimpleNamespace(status_code=200, headers={"ETag": '"etag-1"'}, raise_for_status=lambda: None)

    def close(self) -> None:
        self.closed = True


class SplitPutSession:
    """Hold part 1 in flight while part 2 reaches the receiver and fails."""

    def __init__(self) -> None:
        """Start with part one held until the test releases it."""
        self.part_one_parked = threading.Event()
        self.release = threading.Event()
        self.part_two_failed = threading.Event()
        self.events: list[str] = []

    def put(self, url: str, **kwargs: Any) -> Any:
        if url.endswith("/2"):
            assert self.part_one_parked.wait(timeout=10)
            self.part_two_failed.set()
            raise ConnectionError("part 2 failed")
        self.part_one_parked.set()
        assert self.release.wait(timeout=10)
        self.events.append("part one finished")
        return SimpleNamespace(status_code=200, headers={"ETag": '"etag-1"'}, raise_for_status=lambda: None)

    def close(self) -> None:
        pass


class RecordingPutSession:
    """Records the bytes PUT for each part and hands back a distinct ETag per part number."""

    def __init__(self) -> None:
        """Start with nothing recorded; the ETag is derived from the part number in the URL."""
        self.lock = threading.Lock()
        self.parts: dict[int, bytes] = {}  # part number -> bytes sent
        self.closed = False

    def put(self, url: str, data: Any = None, headers: Any = None, verify: Any = None, timeout: Any = None) -> Any:
        part = int(url.rsplit("/", 1)[1])
        with self.lock:
            self.parts[part] = data
        return SimpleNamespace(status_code=200, headers={"ETag": f'"etag-{part}"'}, raise_for_status=lambda: None)

    def close(self) -> None:
        self.closed = True


def fake_nominal_client(service: FakeUploadService | None = None) -> MagicMock:
    """A NominalClient stand-in shaped like `create` consumes it: a bundle with an upload client."""
    client = MagicMock()
    client._clients.upload = service if service is not None else FakeUploadService()
    client._clients.auth_header = "Bearer test"
    client._clients.header_provider = None
    client._clients.resolve_default_workspace_rid.return_value = "rid.workspace.test"
    return client


@pytest.fixture
def make_uploader() -> Iterator[MakeUploader]:
    """Factory for uploaders wired to hand fakes; closes every uploader it made at teardown.

    The small-file route is pinned OFF unless a test opts in: the fixtures here are all tiny,
    and the multipart tests must keep exercising multipart under the production default
    (which routes them single-shot).
    """
    created: list[MultipartUploader] = []

    def _make(
        service: FakeUploadService | None = None, **create_kwargs: Any
    ) -> tuple[MultipartUploader, FakeUploadService, FakePutSession]:
        service = service or FakeUploadService()
        create_kwargs.setdefault("small_file_route_max_bytes", None)
        # File-level transient retry off unless a test opts in: most failure tests pin the
        # single-attempt contracts, and the default budget would real-sleep between attempts.
        create_kwargs.setdefault("file_retry_timeout", None)
        up = MultipartUploader.create(fake_nominal_client(service), **create_kwargs)
        created.append(up)
        session = FakePutSession()
        up._streams.session.close()  # release the real session create() built before swapping it out
        up._streams.session = session  # swap the S3 session for a fake; TLS never touched in unit tests
        return up, service, session

    yield _make
    for up in created:
        up.close(cancel_pending=True)  # idempotent: a no-op for uploaders the test already closed


class TestValidation:
    def test_part_size_must_be_positive(self, make_uploader: MakeUploader, write_file: WriteFile) -> None:
        """A non-positive part size is rejected synchronously at enqueue."""
        up, _, _ = make_uploader()
        path = write_file("f.csv", 100)
        with up, pytest.raises(ValueError, match="part_size"):
            up.enqueue_file(path, part_size=0)

    def test_too_many_parts_rejected(self, make_uploader: MakeUploader, write_file: WriteFile) -> None:
        """A plan needing more parts than the storage provider allows is rejected at enqueue."""
        up, _, _ = make_uploader()
        path = write_file("f.csv", 20_001)
        with up, pytest.raises(ValueError, match="10000|10,000"):
            up.enqueue_file(path, part_size=2)

    def test_multi_part_below_provider_minimum_warns_and_uploads(
        self,
        make_uploader: MakeUploader,
        write_file: WriteFile,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A deliberately small part_size is allowed — network tuning is legitimate — but warned about.

        The storage provider may still reject sub-minimum non-final parts at completion, so the
        caller gets a warning rather than a silent gamble.
        """
        up, service, _ = make_uploader()
        path = write_file("f.csv", 4096)
        with caplog.at_level(logging.WARNING), up:
            up.enqueue_file(path, part_size=1024).result(timeout=10)
        assert "5 MiB minimum" in caplog.text
        assert service.calls.count("complete") == 1

    def test_part_size_is_ignored_on_the_small_file_route(
        self, make_uploader: MakeUploader, write_file: WriteFile
    ) -> None:
        """A single-shot file uploads regardless of part_size — only the multipart route consumes it."""
        up, service, _ = make_uploader(small_file_route_max_bytes=1024)
        path = write_file("small.csv", 100)
        with up:
            up.enqueue_file(path, part_size=1).result(timeout=10)
        assert service.calls == ["upload_file"]

    def test_missing_file_raises_synchronously(self, make_uploader: MakeUploader, tmp_path: pathlib.Path) -> None:
        """A nonexistent path fails at enqueue on the calling thread, not later inside a worker."""
        up, _, _ = make_uploader()
        with up, pytest.raises(FileNotFoundError):
            up.enqueue_file(tmp_path / "missing.csv")


@pytest.mark.parametrize("small_file_route_max_bytes", [None, 1024], ids=["multipart", "single-shot"])
@pytest.mark.parametrize(
    ("source_name", "name", "file_type", "expected"),
    [
        ("source.csv", "report.csv", FileTypes.CSV, "report.csv"),
        ("source.parquet.tar.gz", "archive.parquet.tar.gz", FileTypes.PARQUET_TAR_GZ, "archive.parquet.tar.gz"),
        ("report.csv.csv", None, FileTypes.CSV, "report.csv.csv"),
        ("source.dat", "x.İ", FileType(".i\u0307", "application/octet-stream"), "x.İ"),
        ("flight..ulg", None, None, "flight..ulg"),
        ("flight..ulg", "copy", None, "copy..ulg"),
        ("source.csv", "data.weird", FileTypes.CSV, "data.weird"),
        ("a.b.CSV", None, FileTypes.CSV, "a.b.CSV"),
    ],
)
def test_upload_routes_share_filename_resolution(
    make_uploader, write_file, small_file_route_max_bytes, source_name, name, file_type, expected
) -> None:
    """Both upload routes accept full name overrides and preserve names derived from file paths."""
    up, _, _ = make_uploader(small_file_route_max_bytes=small_file_route_max_bytes)
    path = write_file(source_name, 10)
    with up:
        location = up.enqueue_file(path, file_type=file_type, name=name).result(timeout=10)
    assert location == f"s3://bucket/{expected}"


@pytest.mark.parametrize("extension", ["csv", ".bad/name"])
def test_invalid_default_extension_is_rejected_before_queueing(make_uploader, write_file, extension: str) -> None:
    """An invalid default fails before storage requests when a suffix-free name requires it."""
    up, service, _ = make_uploader()
    path = write_file("source.dat", 10)
    with up:
        with pytest.raises(ValueError, match="extension|unsafe for storage"):
            up.enqueue_file(path, file_type=FileType(extension, "application/octet-stream"), name="source")
    assert service.calls == []


class TestNonBlockingEnqueueAndCancellation:
    def test_enqueue_never_blocks_and_queued_files_do_not_initiate(
        self, make_uploader: MakeUploader, write_file: WriteFile
    ) -> None:
        """Enqueue returns immediately, and a still-queued file can be cancelled without any request."""
        up, service, session = make_uploader(max_storage_workers=2, max_files_in_flight=1)
        session.put_release.clear()  # first file's PUT holds its driver open
        paths = [write_file(f"f{i}.csv", 100) for i in range(10)]
        futures = [up.enqueue_file(p) for p in paths]  # returns immediately, driver pool of 1
        assert session.put_started.wait(timeout=10)
        assert service.calls.count("initiate") == 1  # only the running driver initiated
        # a queued file can be cancelled and never touches the network
        assert futures[-1].cancel() is True
        session.put_release.set()
        for fut in futures[:-1]:
            fut.result(timeout=10)
        with pytest.raises(CancelledError):
            futures[-1].result(timeout=1)
        up.close()
        assert service.calls.count("initiate") == 9  # the cancelled file never initiated

    def test_running_file_is_not_cancellable(self, make_uploader: MakeUploader, write_file: WriteFile) -> None:
        """Cancelling a file whose upload already started returns False and the file completes."""
        up, service, session = make_uploader(max_files_in_flight=1)
        session.put_release.clear()
        path = write_file("f.csv", 100)
        fut = up.enqueue_file(path)
        assert session.put_started.wait(timeout=10)
        assert fut.cancel() is False
        session.put_release.set()
        fut.result(timeout=10)
        up.close()


class TestFailureHandling:
    def test_part_failure_aborts_once_and_surfaces(self, make_uploader: MakeUploader, write_file: WriteFile) -> None:
        """A part that fails every retry fails the file and aborts its multipart upload exactly once."""
        up, service, session = make_uploader(max_part_retries=2)
        session.put = MagicMock(side_effect=ConnectionError("boom"))
        path = write_file("f.csv", 100)
        fut = up.enqueue_file(path)
        with pytest.raises(NominalMultipartUploadFailed):
            fut.result(timeout=10)
        up.close()
        assert service.calls.count("abort") == 1

    def test_failed_upload_aborts_after_its_running_sibling_finishes(
        self, make_uploader: MakeUploader, write_file: WriteFile
    ) -> None:
        """The service sees the abort after the last in-flight PUT returns and before the file future settles."""
        up, service, _ = make_uploader(max_part_retries=1)
        session = SplitPutSession()
        up._streams.session = session
        service.abort_multipart_upload = lambda *args, **kwargs: session.events.append("abort")
        with up:
            fut = up.enqueue_file(write_file("two-parts.bin", 8), part_size=4)
            try:
                assert session.part_two_failed.wait(timeout=10)
            finally:
                session.release.set()
            with pytest.raises(NominalMultipartUploadFailed):
                fut.result(timeout=10)
        assert session.events == ["part one finished", "abort"]

    def test_missing_etag_fails_part_immediately(self, make_uploader: MakeUploader, write_file: WriteFile) -> None:
        """A PUT response without an ETag fails the part on its first attempt, never retried.

        No retry can conjure an ETag the provider never sent; completing without one would
        produce a silently corrupt object, so the part dies rather than re-uploading its bytes.
        """
        up, service, session = make_uploader(max_part_retries=3)
        session.put = MagicMock(
            return_value=SimpleNamespace(status_code=200, headers={}, raise_for_status=lambda: None)
        )
        path = write_file("f.csv", 100)
        fut = up.enqueue_file(path)
        with pytest.raises(NominalMultipartUploadError, match="ETag"):
            fut.result(timeout=10)
        up.close()
        assert session.put.call_count == 1
        assert service.calls.count("abort") == 1

    def test_one_file_failing_leaves_the_others_alone(self, make_uploader: MakeUploader, write_file: WriteFile) -> None:
        """Files are independent: one bad file must not take the batch down with it."""
        up, service, _ = make_uploader(service=FakeUploadService(fail_sign_for_key="bad.csv"), max_part_retries=2)
        good = write_file("good.csv", 100)
        bad = write_file("bad.csv", 100)
        with up:
            good_fut = up.enqueue_file(good)
            bad_fut = up.enqueue_file(bad)
            assert good_fut.result(timeout=10) == "s3://bucket/good.csv"
            with pytest.raises(NominalMultipartUploadFailed):
                bad_fut.result(timeout=10)
        assert service.aborted == ["bad.csv"]


class TestClose:
    def test_close_during_cancellation_waits_for_storage(
        self, make_uploader: MakeUploader, write_file: WriteFile
    ) -> None:
        up, _, session = make_uploader(max_files_in_flight=1)
        session.put_release.clear()
        active = up.enqueue_file(write_file("active.csv", 10))
        assert session.put_started.wait(timeout=5)
        queued = up.enqueue_file(write_file("queued.csv", 10))
        observed = []

        def close_again(_: Future[str]) -> None:
            session.put_release.set()
            up.close()
            observed.append((active.done(), session.closed))

        # Future.cancel invokes callbacks on the closing thread, before the outer close joins.
        queued.add_done_callback(close_again)
        up.close(cancel_pending=True)
        assert observed == [(True, True)]

    def test_close_drains_and_settles_everything(self, make_uploader: MakeUploader, write_file: WriteFile) -> None:
        """A plain close runs every enqueued file to completion before returning."""
        up, service, _ = make_uploader(max_files_in_flight=2)
        futures = [up.enqueue_file(write_file(f"f{i}.csv", 100)) for i in range(8)]
        up.close()
        assert all(f.done() for f in futures)
        assert service.calls.count("complete") == 8

    def test_enqueue_after_close_raises(self, make_uploader: MakeUploader, write_file: WriteFile) -> None:
        """A closed uploader refuses new files instead of silently dropping them."""
        up, _, _ = make_uploader()
        up.close()
        with pytest.raises(RuntimeError):
            up.enqueue_file(write_file("f.csv", 100))

    def test_cancel_pending_drops_queued_files(self, make_uploader: MakeUploader, write_file: WriteFile) -> None:
        """A cancelling close drops queued files without ever initiating their uploads."""
        up, service, session = make_uploader(max_files_in_flight=1)
        session.put_release.clear()
        futures = [up.enqueue_file(write_file(f"f{i}.csv", 100)) for i in range(6)]
        assert session.put_started.wait(timeout=10)
        # Keep the driver occupied until close has cancelled every queued file.
        futures[-1].add_done_callback(lambda _: session.put_release.set())
        up.close(cancel_pending=True)
        assert all(f.done() for f in futures)
        assert all(f.cancelled() for f in futures[1:])
        assert service.calls.count("initiate") == 1

    def test_exit_on_exception_takes_cancel_path(self, make_uploader: MakeUploader, write_file: WriteFile) -> None:
        """Leaving the context on an exception drops pending files instead of finishing the batch."""
        up, service, session = make_uploader(max_files_in_flight=1)
        session.put_release.clear()
        futures: list[Future[str]] = []
        with pytest.raises(KeyboardInterrupt):
            with up:
                futures = [up.enqueue_file(write_file(f"f{i}.csv", 100)) for i in range(6)]
                assert session.put_started.wait(timeout=10)
                # __exit__ must cancel the queue before the running upload can finish.
                futures[-1].add_done_callback(lambda _: session.put_release.set())
                raise KeyboardInterrupt
        assert all(f.cancelled() for f in futures[1:])

    def test_cancel_pending_with_queued_sibling_parts_still_returns(
        self, make_uploader: MakeUploader, write_file: WriteFile
    ) -> None:
        """Cancelling a batch with one busy worker settles queued parts and aborts its upload."""
        part_size = 5 * 1024 * 1024
        # max_storage_workers=1: part 1 occupies the only part worker, so part 2 queues at close.
        up, service, _ = make_uploader(max_storage_workers=1, max_part_retries=1)
        session = SplitPutSession()
        up._streams.session = session
        fut = up.enqueue_file(write_file("two-parts.bin", part_size + 1), part_size=part_size)
        assert session.part_one_parked.wait(timeout=10)  # part 1 running, part 2 queued behind it

        closer = threading.Thread(target=up.close, kwargs={"cancel_pending": True})
        closer.start()
        assert up._streams.draining.wait(timeout=10)  # the part lane is revoked; only now let part 1 finish
        session.release.set()

        closer.join(timeout=10)
        assert not closer.is_alive(), "close(cancel_pending=True) did not return"
        assert fut.done()
        with pytest.raises(CancelledError):
            fut.result(timeout=10)
        assert service.calls.count("abort") == 1

    def test_futures_are_waitable_after_a_cancelling_close(
        self, make_uploader: MakeUploader, write_file: WriteFile
    ) -> None:
        """Dropped futures must be settled AND waiter-visible once close returns.

        Revoking them via `shutdown(cancel_futures=True)` would leave them
        CANCELLED-but-never-notified — a state `concurrent.futures.wait` blocks on forever —
        so close settles every issued future itself. This pins the public guarantee that
        idiomatic stdlib waiting cannot hang after a cancelling close.
        """
        up, service, session = make_uploader(max_files_in_flight=1)
        session.put_release.clear()
        futures = [up.enqueue_file(write_file(f"f{i}.csv", 100)) for i in range(6)]
        assert session.put_started.wait(timeout=10)
        # Ensure the wait below observes futures actually cancelled by close.
        futures[-1].add_done_callback(lambda _: session.put_release.set())
        up.close(cancel_pending=True)

        done, not_done = wait(futures, timeout=5)
        assert not_done == set(), "close returned with futures still invisible to wait()"
        assert all(f.cancelled() for f in futures[1:])


class TestNoDeadlock:
    def test_bound_of_one_with_multipart_file_completes(
        self, make_uploader: MakeUploader, write_file: WriteFile
    ) -> None:
        """The tightest driver bound still completes a multipart file (drivers never starve parts)."""
        up, _, _ = make_uploader(max_storage_workers=2, max_files_in_flight=1)
        path = write_file("f.csv", 100)
        with up:
            assert up.enqueue_file(path).result(timeout=10)

    def test_long_puts_do_not_stall_small_files(self, make_uploader: MakeUploader, write_file: WriteFile) -> None:
        """A saturated part pool must not delay the small-file route.

        max_storage_workers=1: the big file's parked PUT occupies the ENTIRE part pool. Under
        one shared pool the small file would queue behind it and never run.
        """
        service = FakeUploadService()
        up, service, session = make_uploader(
            service=service, small_file_route_max_bytes=512, max_storage_workers=1, max_small_file_workers=2
        )
        session.put_release.clear()  # every PUT parks its part thread
        big = write_file("big.csv", 4096)
        small = write_file("small.csv", 100)
        big_fut = up.enqueue_file(big)
        assert session.put_started.wait(timeout=10)  # the part pool's only worker is now parked
        small_fut = up.enqueue_file(small)
        assert small_fut.result(timeout=10).startswith("s3://")  # small lane unaffected
        session.put_release.set()
        big_fut.result(timeout=10)
        up.close()


class TestPartLayout:
    def test_each_part_carries_its_own_byte_range_and_etag(
        self, make_uploader: MakeUploader, tmp_path: pathlib.Path
    ) -> None:
        """Every part PUTs exactly its slice, and completion files each ETag under its part number.

        A slice or ETag mix-up completes an object that is silently corrupt — nothing downstream
        would catch it — so the mapping is asserted end to end rather than inferred.
        """
        part_size = 5 * 1024 * 1024  # the provider minimum, so a 3-part plan is legal
        body = b"A" * part_size + b"B" * part_size + b"C" * 7
        up, service, _ = make_uploader()
        session = RecordingPutSession()
        up._streams.session = session
        path = tmp_path / "three-parts.bin"
        path.write_bytes(body)

        with up:
            up.enqueue_file(path, part_size=part_size).result(timeout=30)

        assert session.parts == {1: b"A" * part_size, 2: b"B" * part_size, 3: b"C" * 7}
        assert service.completed_etags["three-parts.bin"] == {1: '"etag-1"', 2: '"etag-2"', 3: '"etag-3"'}

    def test_an_empty_file_puts_one_zero_byte_part(self, make_uploader: MakeUploader, write_file: WriteFile) -> None:
        """Completion needs at least one part to list, so an empty file still uploads one."""
        up, service, _ = make_uploader()
        session = RecordingPutSession()
        up._streams.session = session
        path = write_file("empty.csv", 0)

        with up:
            up.enqueue_file(path).result(timeout=10)

        assert session.parts == {1: b""}
        assert service.completed_etags["empty.csv"] == {1: '"etag-1"'}


def test_source_truncated_while_queued_cannot_complete_as_a_shorter_file(
    make_uploader: MakeUploader, write_file: WriteFile
) -> None:
    """An upload whose source shrinks after enqueue aborts before completing the shorter object."""
    up, service, session = make_uploader(max_files_in_flight=1)
    session.put_release.clear()
    with up:
        first = up.enqueue_file(write_file("first.csv", 8))
        try:
            assert session.put_started.wait(timeout=10)
            queued = write_file("queued.csv", 8)
            second = up.enqueue_file(queued)
            queued.write_bytes(b"x")
        finally:
            session.put_release.set()
        first.result(timeout=10)
        with pytest.raises(ValueError, match="ended early"):
            second.result(timeout=10)
    assert service.aborted == ["queued.csv"]
    assert set(service.completed_etags) == {"first.csv"}


class TestCreateValidation:
    @pytest.mark.parametrize("value", [0, -1])
    @pytest.mark.parametrize("kwarg", ["max_storage_workers", "max_small_file_workers", "max_files_in_flight"])
    def test_pool_sizes_must_be_positive(self, kwarg: str, value: int) -> None:
        """A zero-width pool would silently swallow the whole batch; there is no unbounded mode."""
        with pytest.raises(ValueError, match=f"{kwarg} must be positive"):
            MultipartUploader.create(fake_nominal_client(), **{kwarg: value})

    @pytest.mark.parametrize("value", [0, -1, MAX_SMALL_FILE_ROUTE_BYTES + 1])
    def test_small_file_route_size_must_sit_inside_the_ceiling(self, value: int) -> None:
        """Single-shot is disproportionately expensive server-side for big files, so it's capped."""
        with pytest.raises(ValueError, match="small_file_route_max_bytes must be in"):
            MultipartUploader.create(fake_nominal_client(), small_file_route_max_bytes=value)


@pytest.mark.parametrize("threshold", [None, 16], ids=["multipart", "single-request"])
def test_enqueuing_a_payload_larger_than_the_buffer_budget_fails_before_transfer(
    threshold: int | None, make_uploader: MakeUploader, write_file: WriteFile
) -> None:
    """Reject a body or part that can never fit before issuing storage requests."""
    up, service, _ = make_uploader(max_buffer_bytes=4, small_file_route_max_bytes=threshold)
    with up, pytest.raises(ValueError, match="max_buffer_bytes"):
        up.enqueue_file(write_file("six.csv", 6), part_size=5)
    assert service.calls == []


class TestSmallFileRoute:
    def test_declares_the_real_size_and_never_touches_storage_directly(
        self, make_uploader: MakeUploader, write_file: WriteFile
    ) -> None:
        """`size_bytes` makes the server stream to storage instead of capping at its memory limit."""
        up, service, session = make_uploader(small_file_route_max_bytes=1024)
        path = write_file("small.csv", 100)

        with up:
            assert up.enqueue_file(path).result(timeout=10) == "s3://bucket/small.csv"

        assert service.calls == ["upload_file"]  # the whole upload costs exactly one API request
        assert service.upload_file_args == [("small.csv", 100, 100)]  # (file name, size_bytes, body length)
        assert not session.put_started.is_set()  # no direct-to-storage PUT on this route

    @pytest.mark.parametrize(
        ("route_config", "file_size"),
        [
            pytest.param({"small_file_route_max_bytes": 64}, 200, id="above-threshold"),
            pytest.param({"small_file_route_max_bytes": 1024}, 0, id="zero-byte"),
            pytest.param({"small_file_route_max_bytes": None}, 100, id="route-disabled"),
        ],
    )
    def test_files_outside_the_route_take_multipart(
        self,
        make_uploader: MakeUploader,
        write_file: WriteFile,
        route_config: dict[str, Any],
        file_size: int,
    ) -> None:
        """Files the single-shot route cannot take — too big, empty, or route off — go multipart.

        Zero-byte files always go multipart because the single-shot endpoint rejects a declared
        size of 0, while a single zero-byte part completes normally.
        """
        up, service, _ = make_uploader(**route_config)
        path = write_file("f.csv", file_size)

        with up:
            up.enqueue_file(path).result(timeout=10)

        assert service.upload_file_args == []
        assert service.calls == ["initiate", "sign", "complete"]

    def test_the_route_is_on_by_default_with_files_split_at_one_mib(self, write_file: WriteFile) -> None:
        """`create()`'s own default must route at-threshold files single-shot and larger multipart."""
        service = FakeUploadService()
        up = MultipartUploader.create(fake_nominal_client(service))
        up._streams.session.close()  # release the real session create() built before swapping it out
        up._streams.session = RecordingPutSession()

        with up:
            up.enqueue_file(write_file("at_threshold.csv", DEFAULT_SMALL_FILE_ROUTE_MAX_BYTES)).result(timeout=10)
            up.enqueue_file(write_file("above.csv", DEFAULT_SMALL_FILE_ROUTE_MAX_BYTES + 1)).result(timeout=10)

        assert [name for (name, _, _) in service.upload_file_args] == ["at_threshold.csv"]


def _throttled() -> requests.exceptions.HTTPError:
    """The server refusing a request because the caller is over its request budget."""
    response = requests.Response()
    response.status_code = 429
    return requests.exceptions.HTTPError("429 too many requests", response=response)


class TestThrottlingIsAbsorbedByTheGate:
    @pytest.mark.parametrize("endpoint", ["initiate", "sign", "complete", "upload_file"])
    def test_throttled_file_completes_after_one_endpoint_retry(
        self,
        endpoint: str,
        make_uploader: MakeUploader,
        write_file: WriteFile,
        install_test_gate: Callable[..., FakeClock],
    ) -> None:
        service = FakeUploadService(throttle_once_at=endpoint)
        up, _, _ = make_uploader(
            service=service, small_file_route_max_bytes=1024 if endpoint == "upload_file" else None
        )
        install_test_gate(up)
        with up:
            assert up.enqueue_file(write_file("f.csv", 100)).result(timeout=10) == "s3://bucket/f.csv"
        assert service.calls.count(endpoint) == 2
        assert service.aborted == []

    def test_a_permanently_throttled_sign_spends_exactly_one_budget(
        self,
        make_uploader: MakeUploader,
        write_file: WriteFile,
        install_test_gate: Callable[..., Any],
    ) -> None:
        """A sign that throttles forever must exhaust one gate budget, not one per part retry.

        It also has to surface intact as `NominalRequestThrottledError` instead of being folded
        into a multipart failure group, which would hide why the file died.
        """
        up, service, _ = make_uploader(max_part_retries=3)
        clock = install_test_gate(up, deadline_seconds=3.0)
        calls = {"n": 0}

        def always_throttle(*args: Any, **kwargs: Any) -> Any:
            calls["n"] += 1
            clock.now += 1.0  # each attempt burns a second of the budget
            raise _throttled()

        service.sign_part = always_throttle
        path = write_file("f.csv", 100)

        with up:
            with pytest.raises(NominalRequestThrottledError):
                up.enqueue_file(path).result(timeout=10)

        # A 3s budget at 1s per attempt is exactly 3 signs. If the per-part retry loop caught the
        # throttled-out call and handed it a fresh budget, this would be 3 * max_part_retries.
        assert calls["n"] == 3
        assert service.aborted == ["f.csv"]


class TestFileLevelTransientRetry:
    """The uploader's own resilience: transient failures retry whole files; permanent ones don't.

    The retry seams (`_retry_clock`, `_retry_wait`, `_retry_jitter`) are injected so no test
    sleeps for real; `_retry_wait` returning True means "a cancelling close started".
    """

    @staticmethod
    def _install_retry_seams(up: MultipartUploader, clock: Any) -> None:
        up._retry_clock = clock
        up._retry_wait = lambda seconds: (clock.sleep(seconds), False)[1]
        up._retry_jitter = lambda delay: delay

    def test_a_transient_failure_retries_the_file_and_succeeds(
        self, make_uploader: MakeUploader, write_file: WriteFile, fake_clock: Any
    ) -> None:
        """A file whose attempt dies of network weather is re-run whole and completes."""
        service = FakeUploadService(fail_sign_for_key="f.csv")  # sign dies with ConnectionError
        up, service, _ = make_uploader(service=service, max_part_retries=1, file_retry_timeout=60.0)
        self._install_retry_seams(up, fake_clock)
        original_wait = up._retry_wait

        def heal_then_wait(seconds: float) -> bool:
            service.fail_sign_for_key = None  # the network comes back during the backoff
            return original_wait(seconds)

        up._retry_wait = heal_then_wait
        path = write_file("f.csv", 100)

        with up:
            assert up.enqueue_file(path).result(timeout=10) == "s3://bucket/f.csv"

        assert service.calls.count("abort") == 1  # the failed attempt rolled itself back
        assert service.calls.count("complete") == 1  # the retry completed cleanly

    def test_a_permanent_failure_never_retries(
        self, make_uploader: MakeUploader, write_file: WriteFile, fake_clock: Any
    ) -> None:
        """A non-network failure settles immediately even with a generous retry budget."""
        up, service, _ = make_uploader(file_retry_timeout=60.0)
        self._install_retry_seams(up, fake_clock)
        service.initiate_multipart_upload = MagicMock(side_effect=RuntimeError("not weather"))
        path = write_file("f.csv", 100)

        with up:
            with pytest.raises(RuntimeError, match="not weather"):
                up.enqueue_file(path).result(timeout=10)

        assert service.initiate_multipart_upload.call_count == 1
        assert fake_clock.sleeps == []  # no backoff was ever taken

    def test_the_retry_budget_bounds_a_sustained_outage(
        self, make_uploader: MakeUploader, write_file: WriteFile, fake_clock: Any
    ) -> None:
        """A failure that never heals surfaces once the file's retry budget is spent."""
        service = FakeUploadService(fail_sign_for_key="f.csv")
        up, service, _ = make_uploader(service=service, max_part_retries=1, file_retry_timeout=10.0)
        self._install_retry_seams(up, fake_clock)
        path = write_file("f.csv", 100)

        with up:
            with pytest.raises(NominalMultipartUploadFailed):
                up.enqueue_file(path).result(timeout=10)

        # Attempts start at 0, 1, 3 and 7 seconds. Waking at the deadline surfaces the last failure.
        assert fake_clock.now == 10.0
        assert service.calls.count("abort") == 4

    def test_a_cancelling_close_interrupts_the_backoff_wait(
        self, make_uploader: MakeUploader, write_file: WriteFile
    ) -> None:
        """A file parked in a retry backoff settles as cancelled the moment close begins."""
        service = FakeUploadService(fail_sign_for_key="f.csv")
        up, service, _ = make_uploader(service=service, max_part_retries=1, file_retry_timeout=60.0)
        up._retry_wait = lambda seconds: True  # the drain flag is raised mid-wait
        path = write_file("f.csv", 100)

        with up:
            with pytest.raises(CancelledError):
                up.enqueue_file(path).result(timeout=10)

        assert service.calls.count("sign") == 1  # the retry never ran; the wait saw the close

    def test_a_throttle_budget_exhaustion_is_retried_at_the_file_level(
        self,
        make_uploader: MakeUploader,
        write_file: WriteFile,
        install_test_gate: Callable[..., Any],
        fake_clock: Any,
    ) -> None:
        """A file that outlives one gate budget gets a fresh one instead of failing the batch."""
        up, service, _ = make_uploader(small_file_route_max_bytes=1024, file_retry_timeout=60.0)
        gate_clock = install_test_gate(up, deadline_seconds=1.0)
        self._install_retry_seams(up, fake_clock)
        calls = {"n": 0}
        original = service.upload_file

        def throttle_out_once(*args: Any, **kwargs: Any) -> Any:
            calls["n"] += 1
            if calls["n"] == 1:
                gate_clock.now += 2.0  # burn the whole gate budget in one attempt
                raise _throttled()
            return original(*args, **kwargs)

        service.upload_file = throttle_out_once
        path = write_file("small.csv", 100)

        with up:
            assert up.enqueue_file(path).result(timeout=10) == "s3://bucket/small.csv"

        assert calls["n"] == 2  # one throttled-out attempt, one clean retry


class TestNominalConcurrencyCap:
    def test_max_nominal_concurrency_bounds_simultaneous_api_requests(
        self, make_uploader: MakeUploader, write_file: WriteFile
    ) -> None:
        """The `max_nominal_concurrency` knob is a hard cap on in-flight Nominal API requests.

        Six small files race through a two-wide lane: exactly two requests run at once — the
        lane fills — and the peak never exceeds it across the whole batch.
        """
        up, service, _ = make_uploader(
            small_file_route_max_bytes=1024, max_small_file_workers=6, max_nominal_concurrency=2
        )
        lock = threading.Lock()
        active = [0]
        peak = [0]
        lane_full = threading.Event()
        original = service.upload_file

        def tracked(*args: Any, **kwargs: Any) -> Any:
            with lock:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
                if active[0] == 2:
                    lane_full.set()
            try:
                return original(*args, **kwargs)
            finally:
                with lock:
                    active[0] -= 1

        service.upload_file = tracked
        service.upload_file_release.clear()  # park in-flight requests so concurrency is observable
        futures = [up.enqueue_file(write_file(f"s{i}.csv", 10)) for i in range(6)]

        assert lane_full.wait(timeout=10)  # the lane fills to its width...
        service.upload_file_release.set()
        for fut in futures:
            assert fut.result(timeout=10).startswith("s3://")
        assert peak[0] == 2  # ...and is never exceeded across all six files
