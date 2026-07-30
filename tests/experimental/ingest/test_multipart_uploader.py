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

from nominal.core.exceptions import (
    NominalMultipartUploadError,
    NominalMultipartUploadFailed,
    NominalRequestThrottledError,
)
from nominal.experimental.ingest._multipart_uploader import (
    DEFAULT_SMALL_FILE_ROUTE_MAX_BYTES,
    MAX_SMALL_FILE_ROUTE_BYTES,
    MultipartUploader,
)

WriteFile = Callable[[str, int], pathlib.Path]
MakeUploader = Callable[..., "tuple[MultipartUploader, FakeUploadService, FakePutSession]"]


class FakeUploadService:
    """Counts calls; optional per-method gates let tests hold a call open.

    The object key is the request filename so a test can target one file out of a batch by
    name — a counter would race, since initiates run concurrently.
    """

    def __init__(self, *, fail_sign_for_key: str | None = None) -> None:
        """Start with every gate open, so a test opts in to holding a call."""
        self.lock = threading.Lock()
        self.calls: list[str] = []
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

    def initiate_multipart_upload(self, auth: str, request: Any) -> Any:
        self._record("initiate")
        return SimpleNamespace(key=request.filename, upload_id=f"uid-{request.filename}")

    def sign_part(self, auth: str, key: str, part: int, upload_id: str) -> Any:
        self._record("sign")
        if self.fail_sign_for_key is not None and key == self.fail_sign_for_key:
            raise ConnectionError(f"sign failed for {key}")
        return SimpleNamespace(url=f"https://s3.example/{key}/{part}", headers={})

    def complete_multipart_upload(self, auth: str, key: str, upload_id: str, parts: Any) -> Any:
        self._record("complete")
        with self.lock:
            self.completed_etags[key] = {p.part_number: p.etag for p in parts}
        return SimpleNamespace(location=f"s3://bucket/{key}")

    def abort_multipart_upload(self, auth: str, key: str, upload_id: str) -> None:
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
    """A PUT session that parks part 1, so part 2's fate is observable while part 1 is uploading.

    With `part_two_fails` (the default) part 2 waits for part 1 to park before raising, which
    makes "a later part failed while an earlier one is still uploading" a guarantee, not a race.
    """

    def __init__(self) -> None:
        """Part 1 stays parked until `release` is set; part 2 fails unless told otherwise."""
        self.part_one_parked = threading.Event()
        self.release = threading.Event()
        self.part_two_fails = True
        self.part_one_fails_after_release = False
        self.put_counts: dict[int, int] = {}  # part number -> PUT attempts
        self.lock = threading.Lock()
        self.closed = False

    def put(self, url: str, data: Any = None, headers: Any = None, verify: Any = None, timeout: Any = None) -> Any:
        part = int(url.rsplit("/", 1)[1])
        with self.lock:
            self.put_counts[part] = self.put_counts.get(part, 0) + 1
        if part == 2:
            if self.part_two_fails:
                self.part_one_parked.wait(timeout=10)  # order this failure after part 1 parks
                raise ConnectionError("part 2 failed")
        else:
            self.part_one_parked.set()
            self.release.wait(timeout=10)
            if self.part_one_fails_after_release:
                raise ConnectionError("part 1 failed after release")
        return SimpleNamespace(status_code=200, headers={"ETag": '"etag-1"'}, raise_for_status=lambda: None)

    def close(self) -> None:
        self.closed = True


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
        up._session.close()  # release the real session create() built before swapping it out
        up._session = session  # swap the S3 session for a fake; TLS never touched in unit tests
        return up, service, session

    yield _make
    for up in created:
        up.close(cancel_pending=True)  # idempotent: a no-op for uploaders the test already closed


def settled_latch(futures: list[Future[str]]) -> threading.Event:
    """An event set once every one of `futures` has settled, cancellations included.

    Done callbacks fire the instant a future is cancelled, so this observes a mid-close drop
    at the earliest possible moment. `concurrent.futures.wait` only wakes once the pools'
    workers have drained (and thereby waiter-notified) the cancelled items — a point close
    guarantees only by the time it returns, too late for a probe that runs DURING close.
    """
    latch = threading.Event()
    lock = threading.Lock()
    remaining = len(futures)

    def on_settled(_fut: Future[str]) -> None:
        nonlocal remaining
        with lock:
            remaining -= 1
            if remaining == 0:
                latch.set()

    for fut in futures:
        fut.add_done_callback(on_settled)
    return latch


class TestRoutesAndRequestCounts:
    def test_single_part_multipart_makes_exactly_three_calls(
        self, make_uploader: MakeUploader, write_file: WriteFile
    ) -> None:
        """A single-part multipart file costs exactly initiate + sign + complete."""
        up, service, _ = make_uploader()
        path = write_file("f.csv", 100)
        with up:
            assert up.enqueue_file(path).result(timeout=10).startswith("s3://")
        assert service.calls == ["initiate", "sign", "complete"]


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

    def test_late_part_failure_does_not_wait_for_earlier_parts(
        self, make_uploader: MakeUploader, write_file: WriteFile
    ) -> None:
        """A failing part must cancel and abort while its lower-numbered siblings still upload.

        Collecting part results in index order would block on part 1 first, so this file's
        failure would surface only after the whole 5 MiB part had finished uploading — and, for a
        real multi-GiB file, not for many minutes.
        """
        part_size = 5 * 1024 * 1024  # the provider minimum, so a 2-part plan is legal
        up, service, _ = make_uploader(max_part_retries=1)
        session = SplitPutSession()
        up._session = session
        path = write_file("two-parts.bin", part_size + 1)
        try:
            fut = up.enqueue_file(path, part_size=part_size)
            assert session.part_one_parked.wait(timeout=10)  # part 1 is mid-PUT and stays there
            with pytest.raises(NominalMultipartUploadFailed):
                fut.result(timeout=10)
            assert service.calls.count("abort") == 1
        finally:
            session.release.set()
            up.close()

    def test_a_failed_file_revokes_its_running_sibling_parts(
        self, make_uploader: MakeUploader, write_file: WriteFile
    ) -> None:
        """Once a file fails, a surviving sibling part must not retry against the aborted upload.

        Part 2 exhausts its retries and fails the file while part 1 is mid-PUT; when part 1's
        own attempt then fails, its retry boundary must see the revocation and stop — not
        re-sign and re-PUT its bytes at an upload id the abort just invalidated (which wastes
        budget and can leave the multipart upload alive server-side). max_part_retries=3 is
        what makes this bite: without the revoke, part 1 would visibly retry twice more.
        """
        part_size = 5 * 1024 * 1024
        up, service, _ = make_uploader(max_part_retries=3)
        session = SplitPutSession()
        session.part_one_fails_after_release = True
        up._session = session
        path = write_file("two-parts.bin", part_size + 1)
        try:
            fut = up.enqueue_file(path, part_size=part_size)
            with pytest.raises(NominalMultipartUploadFailed):
                fut.result(timeout=10)  # part 2 exhausts its 3 attempts while part 1 is parked
            assert service.calls.count("abort") == 1
            session.release.set()  # part 1's PUT now fails; its retry boundary must see the revoke
            up.close()  # joins the part pool, so part 1's task has fully settled
            assert session.put_counts == {1: 1, 2: 3}  # part 1 never retried after the revoke
            assert service.calls.count("sign") == 4  # 1 for part 1 + 3 for part 2; no re-sign
        finally:
            session.release.set()
            up.close()

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

    def test_initiate_failure_settles_the_future_without_aborting(
        self, make_uploader: MakeUploader, write_file: WriteFile
    ) -> None:
        """Initiate is what yields the upload id, so a failure there leaves nothing to roll back."""
        up, service, _ = make_uploader()
        service.initiate_multipart_upload = MagicMock(side_effect=RuntimeError("initiate failed"))
        path = write_file("f.csv", 100)
        with up:
            with pytest.raises(RuntimeError, match="initiate failed"):
                up.enqueue_file(path).result(timeout=10)
        assert service.aborted == []

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
        session.put_release.set()
        up.close(cancel_pending=True)
        assert all(f.done() for f in futures)
        cancelled = [f for f in futures if f.cancelled()]
        assert len(cancelled) >= 1  # queued drivers dropped
        assert service.calls.count("initiate") < 6  # ...and never initiated

    def test_exit_on_exception_takes_cancel_path(self, make_uploader: MakeUploader, write_file: WriteFile) -> None:
        """Leaving the context on an exception drops pending files instead of finishing the batch."""
        up, service, session = make_uploader(max_files_in_flight=1)
        session.put_release.clear()
        futures: list[Future[str]] = []
        with pytest.raises(KeyboardInterrupt):
            with up:
                futures = [up.enqueue_file(write_file(f"f{i}.csv", 100)) for i in range(6)]
                assert session.put_started.wait(timeout=10)
                session.put_release.set()
                raise KeyboardInterrupt
        assert any(f.cancelled() for f in futures)

    def test_cancel_pending_with_queued_sibling_parts_still_returns(
        self, make_uploader: MakeUploader, write_file: WriteFile
    ) -> None:
        """Close must return when a running driver has parts still QUEUED at revoke time.

        Revoking the part lane with `shutdown(cancel_futures=True)` drains those work items and
        leaves their futures CANCELLED-but-never-notified, which `concurrent.futures.wait` does
        not count as done — the driver would block forever and close would never return. Parts
        must instead RUN and short-circuit, so their futures settle normally.
        """
        part_size = 5 * 1024 * 1024
        # max_storage_workers=1: part 1 occupies the only part worker, so part 2 queues at close.
        up, service, _ = make_uploader(max_storage_workers=1, max_part_retries=1)
        session = SplitPutSession()
        session.part_two_fails = False  # part 2 must be revoked by close, not fail on its own
        up._session = session
        fut = up.enqueue_file(write_file("two-parts.bin", part_size + 1), part_size=part_size)
        assert session.part_one_parked.wait(timeout=10)  # part 1 running, part 2 queued behind it

        closer = threading.Thread(target=up.close, kwargs={"cancel_pending": True})
        closer.start()
        assert up._draining.wait(timeout=10)  # the part lane is revoked; only now let part 1 finish
        session.release.set()

        closer.join(timeout=10)
        assert not closer.is_alive(), "close(cancel_pending=True) did not return"
        assert fut.done()
        with pytest.raises(CancelledError):
            fut.result(timeout=10)
        assert service.calls.count("abort") == 1

    def test_cancel_pending_revokes_queued_small_files_before_joining_drivers(
        self, make_uploader: MakeUploader, write_file: WriteFile
    ) -> None:
        """Queued small files must be dropped as the cancelling close starts, not after it joins.

        The driver join it precedes is not quick: it lasts a whole round of in-flight part PUTs
        (up to `timeout`) plus the capped abort pass. A small pool left live across that window
        keeps uploading files the caller was promised were dropped, and spends the request budget
        the time-budgeted aborts need to roll their multipart uploads back.
        """
        service = FakeUploadService()
        service.upload_file_release.clear()  # the first small file parks inside upload_file
        up, service, session = make_uploader(
            service=service,
            small_file_route_max_bytes=128,
            max_small_file_workers=1,
            max_storage_workers=1,
            max_files_in_flight=1,
        )
        session.put_release.clear()  # a driver parked mid-PUT is what makes the driver join block

        big_fut = up.enqueue_file(write_file("big.csv", 4096))
        assert session.put_started.wait(timeout=10)
        running = up.enqueue_file(write_file("s0.csv", 100))
        assert service.upload_file_started.wait(timeout=10)  # the only small worker is now parked
        queued = [up.enqueue_file(write_file(f"s{i}.csv", 100)) for i in range(1, 4)]
        settled = settled_latch(queued)

        closer = threading.Thread(target=up.close, kwargs={"cancel_pending": True})
        closer.start()
        assert up._draining.wait(timeout=10)
        # The queued files have to settle while the small worker is STILL parked and the driver
        # is STILL mid-PUT: the small lane never gets another turn, and close never gets past its
        # driver join. Revoked late, this is unreachable — nothing here can settle them.
        # (The probe is deliberately shorter than the fakes' 10s self-release valves, so a late
        # revoke fails here rather than being rescued by a park expiring.)
        assert settled.wait(timeout=5), "queued small files outlived the start of a cancelling close"

        service.upload_file_release.set()  # let the in-flight small finish...
        session.put_release.set()  # ...and the driver, so close can join and return
        closer.join(timeout=10)
        assert not closer.is_alive(), "close(cancel_pending=True) did not return"

        assert running.result(timeout=10) == "s3://bucket/s0.csv"  # in-flight small still settles
        assert big_fut.done()
        assert all(f.cancelled() for f in queued)
        assert service.calls.count("upload_file") == 1  # the queued ones never ran

    def test_a_task_starting_after_a_cancelling_close_settles_as_cancelled(
        self, make_uploader: MakeUploader, write_file: WriteFile
    ) -> None:
        """A file whose task slips past close's cancel pass still settles CancelledError, request-free.

        A pool worker can dequeue a queued file in the instant between close revoking the pools
        and cancelling the issued futures, marking it running before `cancel()` can land; the
        task-start drain check settles it like every other dropped file instead of letting it
        spend real requests. That window is too narrow to hit from outside, so this pins the
        guard by raising the drain flag directly — the first act of every cancelling close.
        """
        up, service, _ = make_uploader(small_file_route_max_bytes=1024)
        up._draining.set()

        small_fut = up.enqueue_file(write_file("small.csv", 100))
        multipart_fut = up.enqueue_file(write_file("big.csv", 4096))

        with pytest.raises(CancelledError):
            small_fut.result(timeout=10)
        with pytest.raises(CancelledError):
            multipart_fut.result(timeout=10)
        assert service.calls == []  # dropped before spending a single request

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
        session.put_release.set()
        up.close(cancel_pending=True)

        done, not_done = wait(futures, timeout=5)
        assert not_done == set(), "close returned with futures still invisible to wait()"
        assert any(f.cancelled() for f in futures)  # the drop actually happened

    def test_close_closes_the_s3_session_and_never_the_upload_client(self, make_uploader: MakeUploader) -> None:
        """Close releases the storage session it owns; the shared upload client is not its to touch."""
        up, _, session = make_uploader()
        up.close()
        assert session.closed


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
        up._session = session
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
        up._session = session
        path = write_file("empty.csv", 0)

        with up:
            up.enqueue_file(path).result(timeout=10)

        assert session.parts == {1: b""}
        assert service.completed_etags["empty.csv"] == {1: '"etag-1"'}


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
        up._session.close()  # release the real session create() built before swapping it out
        up._session = RecordingPutSession()

        with up:
            up.enqueue_file(write_file("at_threshold.csv", DEFAULT_SMALL_FILE_ROUTE_MAX_BYTES)).result(timeout=10)
            up.enqueue_file(write_file("above.csv", DEFAULT_SMALL_FILE_ROUTE_MAX_BYTES + 1)).result(timeout=10)

        assert [name for (name, _, _) in service.upload_file_args] == ["at_threshold.csv"]


def _throttled() -> requests.exceptions.HTTPError:
    """The server refusing a request because the caller is over its request budget."""
    response = requests.Response()
    response.status_code = 429
    return requests.exceptions.HTTPError("429 too many requests", response=response)


def _throttle_once(original: Callable[..., Any]) -> tuple[Callable[..., Any], dict[str, int]]:
    """Wrap `original` so its first call throttles; return the wrapper and a call counter."""
    calls = {"n": 0}

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        calls["n"] += 1
        if calls["n"] == 1:
            raise _throttled()
        return original(*args, **kwargs)

    return wrapper, calls


def _throttle_initiate_once(service: FakeUploadService) -> dict[str, int]:
    """Make the first initiate call throttle; return its call counter."""
    wrapper, calls = _throttle_once(service.initiate_multipart_upload)
    service.initiate_multipart_upload = wrapper
    return calls


def _throttle_sign_once(service: FakeUploadService) -> dict[str, int]:
    """Make the first sign call throttle; return its call counter."""
    wrapper, calls = _throttle_once(service.sign_part)
    service.sign_part = wrapper
    return calls


def _throttle_complete_once(service: FakeUploadService) -> dict[str, int]:
    """Make the first complete call throttle; return its call counter."""
    wrapper, calls = _throttle_once(service.complete_multipart_upload)
    service.complete_multipart_upload = wrapper
    return calls


class TestThrottlingIsAbsorbedByTheGate:
    """Every Nominal API request the uploader makes has to go through the gate.

    A throttle is the server saying "later", not "no"; an ungated call site turns one into a
    dead file. These pin each call site by throttling it and requiring the file to survive.
    """

    @pytest.mark.parametrize(
        "throttle_endpoint_once",
        [
            pytest.param(_throttle_initiate_once, id="initiate"),
            pytest.param(_throttle_sign_once, id="sign"),
            pytest.param(_throttle_complete_once, id="complete"),
        ],
    )
    def test_a_throttled_multipart_call_is_retried_rather_than_failing_the_file(
        self,
        throttle_endpoint_once: Callable[[FakeUploadService], dict[str, int]],
        make_uploader: MakeUploader,
        write_file: WriteFile,
        install_test_gate: Callable[..., Any],
    ) -> None:
        """A single throttle on any multipart call site is retried instead of failing the file."""
        up, service, _ = make_uploader()
        install_test_gate(up)
        calls = throttle_endpoint_once(service)
        path = write_file("f.csv", 100)

        with up:
            assert up.enqueue_file(path).result(timeout=10) == "s3://bucket/f.csv"

        assert calls["n"] == 2  # throttled once, retried inside the gate, then succeeded
        assert service.aborted == []

    def test_a_throttled_small_route_upload_is_retried_rather_than_failing_the_file(
        self,
        make_uploader: MakeUploader,
        write_file: WriteFile,
        install_test_gate: Callable[..., Any],
    ) -> None:
        """A single throttle on the single-shot route is retried instead of failing the file."""
        up, service, _ = make_uploader(small_file_route_max_bytes=1024)
        install_test_gate(up)
        wrapper, calls = _throttle_once(service.upload_file)
        service.upload_file = wrapper
        path = write_file("small.csv", 100)

        with up:
            assert up.enqueue_file(path).result(timeout=10) == "s3://bucket/small.csv"

        assert calls["n"] == 2

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

        # Backoff doubles from 1s: sleeps 1+2+4+... land the clock past the 10s budget, and the
        # final failure surfaces rather than waiting again.
        assert fake_clock.now >= 10.0
        assert service.calls.count("abort") >= 2  # multiple whole-file attempts each rolled back

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
