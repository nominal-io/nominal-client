from __future__ import annotations

import pathlib
import threading
from concurrent.futures import CancelledError
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from conjure_python_client import ServiceConfiguration
from conjure_python_client._http.configuration import SslConfiguration

from nominal.core.exceptions import NominalMultipartUploadFailed, NominalRequestThrottledError
from nominal.experimental.ingest._multipart_uploader import MAX_SMALL_FILE_ROUTE_BYTES, MultipartUploader
from nominal.experimental.ingest._upload_pacing import _AdaptivePacer, _ThrottleGate


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

    def initiate_multipart_upload(self, auth, request):
        self._record("initiate")
        return SimpleNamespace(key=request.filename, upload_id=f"uid-{request.filename}")

    def sign_part(self, auth, key, part, upload_id):
        self._record("sign")
        if self.fail_sign_for_key is not None and key == self.fail_sign_for_key:
            raise ConnectionError(f"sign failed for {key}")
        return SimpleNamespace(url=f"https://s3.example/{key}/{part}", headers={})

    def complete_multipart_upload(self, auth, key, upload_id, parts):
        self._record("complete")
        with self.lock:
            self.completed_etags[key] = {p.part_number: p.etag for p in parts}
        return SimpleNamespace(location=f"s3://bucket/{key}")

    def abort_multipart_upload(self, auth, key, upload_id):
        self._record("abort")
        with self.lock:
            self.aborted.append(key)

    def list_parts(self, auth, key, upload_id):
        raise AssertionError("list_parts must not be called: etags come from the PUT responses")

    def upload_file(self, auth, body, file_name, size_bytes=None, workspace=None):
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

    def put(self, url, data=None, headers=None, verify=None, timeout=None):
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
        self.closed = False

    def put(self, url, data=None, headers=None, verify=None, timeout=None):
        if url.endswith("/2"):
            if self.part_two_fails:
                self.part_one_parked.wait(timeout=10)  # order this failure after part 1 parks
                raise ConnectionError("part 2 failed")
        else:
            self.part_one_parked.set()
            self.release.wait(timeout=10)
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

    def put(self, url, data=None, headers=None, verify=None, timeout=None):
        part = int(url.rsplit("/", 1)[1])
        with self.lock:
            self.parts[part] = data
        return SimpleNamespace(status_code=200, headers={"ETag": f'"etag-{part}"'}, raise_for_status=lambda: None)

    def close(self) -> None:
        self.closed = True


def make_uploader(tmp_path: pathlib.Path, service: FakeUploadService | None = None, **create_kwargs):
    service = service or FakeUploadService()
    clients = MagicMock()
    clients.auth_header = "Bearer test"
    clients.resolve_default_workspace_rid.return_value = "rid.workspace.test"
    up = MultipartUploader.create(clients, upload_client=service, **create_kwargs)
    session = FakePutSession()
    up._session = session  # swap the S3 session for a fake; TLS never touched in unit tests
    return up, service, session


def write_file(tmp_path: pathlib.Path, name: str, size: int) -> pathlib.Path:
    p = tmp_path / name
    p.write_bytes(b"x" * size)
    return p


def real_clients():
    """A clients bundle complete enough for `create` to build its own upload client."""
    config = ServiceConfiguration(security=SslConfiguration(trust_store_path=None), uris=["https://api.example.test"])
    clients = MagicMock()
    clients.auth_header = "Bearer test"
    clients.header_provider = None
    clients._user_agent = "test-agent/0"
    clients._service_config = config
    clients.resolve_default_workspace_rid.return_value = "rid.workspace.test"
    return clients


class TestRoutesAndRequestCounts:
    def test_single_part_multipart_makes_exactly_three_calls(self, tmp_path) -> None:
        up, service, _ = make_uploader(tmp_path)
        path = write_file(tmp_path, "f.csv", 100)
        with up:
            assert up.enqueue_file(path).result(timeout=10).startswith("s3://")
        assert service.calls == ["initiate", "sign", "complete"]

    def test_small_route_makes_exactly_one_call(self, tmp_path) -> None:
        up, service, _ = make_uploader(tmp_path, small_file_route_max_bytes=1024)
        path = write_file(tmp_path, "f.csv", 100)
        with up:
            up.enqueue_file(path).result(timeout=10)
        assert service.calls == ["upload_file"]

    def test_zero_byte_file_takes_multipart(self, tmp_path) -> None:
        up, service, _ = make_uploader(tmp_path, small_file_route_max_bytes=1024)
        path = write_file(tmp_path, "empty.csv", 0)
        with up:
            up.enqueue_file(path).result(timeout=10)
        assert service.calls == ["initiate", "sign", "complete"]


class TestValidation:
    def test_part_size_must_be_positive(self, tmp_path) -> None:
        up, _, _ = make_uploader(tmp_path)
        path = write_file(tmp_path, "f.csv", 100)
        with up, pytest.raises(ValueError, match="part_size"):
            up.enqueue_file(path, part_size=0)

    def test_too_many_parts_rejected(self, tmp_path) -> None:
        up, _, _ = make_uploader(tmp_path)
        path = write_file(tmp_path, "f.csv", 20_001)
        with up, pytest.raises(ValueError, match="10000|10,000"):
            up.enqueue_file(path, part_size=2)

    def test_multi_part_below_provider_minimum_rejected(self, tmp_path) -> None:
        # Promoted from a warning: non-final parts below 5 MiB fail at complete-time
        # server-side, after all bytes are spent — reject synchronously instead.
        up, _, _ = make_uploader(tmp_path)
        path = write_file(tmp_path, "f.csv", 4096)
        with up, pytest.raises(ValueError, match="5 MiB"):
            up.enqueue_file(path, part_size=1024)

    def test_missing_file_raises_synchronously(self, tmp_path) -> None:
        up, _, _ = make_uploader(tmp_path)
        with up, pytest.raises(FileNotFoundError):
            up.enqueue_file(tmp_path / "missing.csv")


class TestNonBlockingEnqueueAndCancellation:
    def test_enqueue_never_blocks_and_queued_files_do_not_initiate(self, tmp_path) -> None:
        service = FakeUploadService()
        service.upload_file_release.clear()  # unrelated; keep smalls out of this test
        up, service, session = make_uploader(tmp_path, service=service, max_workers=2, max_multipart_files_in_flight=1)
        session.put_release.clear()  # first file's PUT holds its driver open
        paths = [write_file(tmp_path, f"f{i}.csv", 100) for i in range(10)]
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

    def test_running_file_is_not_cancellable(self, tmp_path) -> None:
        up, service, session = make_uploader(tmp_path, max_multipart_files_in_flight=1)
        session.put_release.clear()
        path = write_file(tmp_path, "f.csv", 100)
        fut = up.enqueue_file(path)
        assert session.put_started.wait(timeout=10)
        assert fut.cancel() is False
        session.put_release.set()
        fut.result(timeout=10)
        up.close()


class TestFailureHandling:
    def test_part_failure_aborts_once_and_surfaces(self, tmp_path) -> None:
        up, service, session = make_uploader(tmp_path, max_part_retries=2)
        session.put = MagicMock(side_effect=ConnectionError("boom"))
        path = write_file(tmp_path, "f.csv", 100)
        fut = up.enqueue_file(path)
        with pytest.raises(NominalMultipartUploadFailed):
            fut.result(timeout=10)
        up.close()
        assert service.calls.count("abort") == 1

    def test_late_part_failure_does_not_wait_for_earlier_parts(self, tmp_path) -> None:
        """A failing part must cancel and abort while its lower-numbered siblings still upload.

        Collecting part results in index order would block on part 1 first, so this file's
        failure would surface only after the whole 5 MiB part had finished uploading — and, for a
        real multi-GiB file, not for many minutes.
        """
        part_size = 5 * 1024 * 1024  # the provider minimum, so a 2-part plan is legal
        up, service, _ = make_uploader(tmp_path, max_part_retries=1)
        session = SplitPutSession()
        up._session = session
        path = write_file(tmp_path, "two-parts.bin", part_size + 1)
        try:
            fut = up.enqueue_file(path, part_size=part_size)
            assert session.part_one_parked.wait(timeout=10)  # part 1 is mid-PUT and stays there
            with pytest.raises(NominalMultipartUploadFailed):
                fut.result(timeout=10)
            assert service.calls.count("abort") == 1
        finally:
            session.release.set()
            up.close()

    def test_missing_etag_fails_part_immediately(self, tmp_path) -> None:
        up, service, session = make_uploader(tmp_path, max_part_retries=3)
        session.put = MagicMock(
            return_value=SimpleNamespace(status_code=200, headers={}, raise_for_status=lambda: None)
        )
        path = write_file(tmp_path, "f.csv", 100)
        fut = up.enqueue_file(path)
        with pytest.raises(Exception, match="ETag"):
            fut.result(timeout=10)
        up.close()
        # max_part_retries=3 on purpose: no retry can conjure an ETag the provider never sent, so
        # the part must die on its first attempt rather than re-uploading its bytes twice more.
        assert session.put.call_count == 1
        assert service.calls.count("abort") == 1

    def test_initiate_failure_settles_the_future_without_aborting(self, tmp_path) -> None:
        """Initiate is what yields the upload id, so a failure there leaves nothing to roll back."""
        up, service, _ = make_uploader(tmp_path)
        service.initiate_multipart_upload = MagicMock(side_effect=RuntimeError("initiate failed"))
        path = write_file(tmp_path, "f.csv", 100)
        with up:
            with pytest.raises(RuntimeError, match="initiate failed"):
                up.enqueue_file(path).result(timeout=10)
        assert service.aborted == []

    def test_one_file_failing_leaves_the_others_alone(self, tmp_path) -> None:
        """Files are independent: one bad file must not take the batch down with it."""
        up, service, _ = make_uploader(
            tmp_path, service=FakeUploadService(fail_sign_for_key="bad.csv"), max_part_retries=2
        )
        good = write_file(tmp_path, "good.csv", 100)
        bad = write_file(tmp_path, "bad.csv", 100)
        with up:
            good_fut = up.enqueue_file(good)
            bad_fut = up.enqueue_file(bad)
            assert good_fut.result(timeout=10) == "s3://bucket/good.csv"
            with pytest.raises(NominalMultipartUploadFailed):
                bad_fut.result(timeout=10)
        assert service.aborted == ["bad.csv"]


class TestClose:
    def test_close_drains_and_settles_everything(self, tmp_path) -> None:
        up, service, _ = make_uploader(tmp_path, max_multipart_files_in_flight=2)
        futures = [up.enqueue_file(write_file(tmp_path, f"f{i}.csv", 100)) for i in range(8)]
        up.close()
        assert all(f.done() for f in futures)
        assert service.calls.count("complete") == 8

    def test_enqueue_after_close_raises(self, tmp_path) -> None:
        up, _, _ = make_uploader(tmp_path)
        up.close()
        with pytest.raises(RuntimeError):
            up.enqueue_file(write_file(tmp_path, "f.csv", 100))

    def test_cancel_pending_drops_queued_files(self, tmp_path) -> None:
        up, service, session = make_uploader(tmp_path, max_multipart_files_in_flight=1)
        session.put_release.clear()
        futures = [up.enqueue_file(write_file(tmp_path, f"f{i}.csv", 100)) for i in range(6)]
        assert session.put_started.wait(timeout=10)
        session.put_release.set()
        up.close(cancel_pending=True)
        assert all(f.done() for f in futures)
        cancelled = [f for f in futures if f.cancelled()]
        assert len(cancelled) >= 1  # queued drivers dropped
        assert service.calls.count("initiate") < 6  # ...and never initiated

    def test_exit_on_exception_takes_cancel_path(self, tmp_path) -> None:
        up, service, session = make_uploader(tmp_path, max_multipart_files_in_flight=1)
        session.put_release.clear()
        futures = []
        with pytest.raises(KeyboardInterrupt):
            with up:
                futures = [up.enqueue_file(write_file(tmp_path, f"f{i}.csv", 100)) for i in range(6)]
                assert session.put_started.wait(timeout=10)
                session.put_release.set()
                raise KeyboardInterrupt
        assert any(f.cancelled() for f in futures)

    def test_cancel_pending_with_queued_sibling_parts_still_returns(self, tmp_path) -> None:
        """Close must return when a running driver has parts still QUEUED at revoke time.

        Revoking the part lane with `shutdown(cancel_futures=True)` drains those work items and
        leaves their futures CANCELLED-but-never-notified, which `concurrent.futures.wait` does
        not count as done — the driver would block forever and close would never return. Parts
        must instead RUN and short-circuit, so their futures settle normally.
        """
        part_size = 5 * 1024 * 1024
        # max_workers=1: part 1 occupies the only part worker, so part 2 is queued at close time.
        up, service, _ = make_uploader(tmp_path, max_workers=1, max_part_retries=1)
        session = SplitPutSession()
        session.part_two_fails = False  # part 2 must be revoked by close, not fail on its own
        up._session = session
        fut = up.enqueue_file(write_file(tmp_path, "two-parts.bin", part_size + 1), part_size=part_size)
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

    def test_close_closes_owned_sessions_but_not_injected_client(self, tmp_path) -> None:
        up, _, session = make_uploader(tmp_path)
        up.close()
        assert session.closed  # S3 session owned + closed
        assert up._owned_client_session is None  # injected client: nothing owned to close


class TestDedicatedUploadClient:
    def test_no_injected_client_builds_one_that_does_not_retry_throttles(self) -> None:
        up = MultipartUploader.create(real_clients())
        try:
            owned = up._owned_client_session
            assert owned is not None
            adapter = owned.get_adapter("https://api.example.test")
            # 429/503 must reach the gate in one round trip, not after transport retry exhaustion
            assert list(adapter.max_retries.status_forcelist) == [308]
        finally:
            up.close()

    def test_close_closes_the_owned_client_session(self) -> None:
        up = MultipartUploader.create(real_clients())
        real_session = up._owned_client_session
        assert real_session is not None
        owned = MagicMock()
        up._owned_client_session = owned
        try:
            up.close()
            owned.close.assert_called_once()
        finally:
            real_session.close()


class TestNoDeadlock:
    def test_bound_of_one_with_multipart_file_completes(self, tmp_path) -> None:
        up, service, _ = make_uploader(tmp_path, max_workers=2, max_multipart_files_in_flight=1)
        path = write_file(tmp_path, "f.csv", 100)
        with up:
            assert up.enqueue_file(path).result(timeout=10)

    def test_long_puts_do_not_stall_small_files(self, tmp_path) -> None:
        service = FakeUploadService()
        # max_workers=1: the big file is a single part, so its parked PUT occupies the ENTIRE
        # part pool. Under one shared pool the small file would queue behind it and never run.
        up, service, session = make_uploader(
            tmp_path, service=service, small_file_route_max_bytes=512, max_workers=1, small_route_workers=2
        )
        session.put_release.clear()  # every PUT parks its part thread
        big = write_file(tmp_path, "big.csv", 4096)
        small = write_file(tmp_path, "small.csv", 100)
        big_fut = up.enqueue_file(big)
        assert session.put_started.wait(timeout=10)  # the part pool's only worker is now parked
        small_fut = up.enqueue_file(small)
        assert small_fut.result(timeout=10).startswith("s3://")  # small lane unaffected
        session.put_release.set()
        big_fut.result(timeout=10)
        up.close()


class TestPartLayout:
    def test_each_part_carries_its_own_byte_range_and_etag(self, tmp_path) -> None:
        """Every part PUTs exactly its slice, and completion files each ETag under its part number.

        A slice or ETag mix-up completes an object that is silently corrupt — nothing downstream
        would catch it — so the mapping is asserted end to end rather than inferred.
        """
        part_size = 5 * 1024 * 1024  # the provider minimum, so a 3-part plan is legal
        body = b"A" * part_size + b"B" * part_size + b"C" * 7
        up, service, _ = make_uploader(tmp_path)
        session = RecordingPutSession()
        up._session = session
        path = tmp_path / "three-parts.bin"
        path.write_bytes(body)

        with up:
            up.enqueue_file(path, part_size=part_size).result(timeout=30)

        assert session.parts == {1: b"A" * part_size, 2: b"B" * part_size, 3: b"C" * 7}
        assert service.completed_etags["three-parts.bin"] == {1: '"etag-1"', 2: '"etag-2"', 3: '"etag-3"'}

    def test_an_empty_file_puts_one_zero_byte_part(self, tmp_path) -> None:
        """Completion needs at least one part to list, so an empty file still uploads one."""
        up, service, _ = make_uploader(tmp_path)
        session = RecordingPutSession()
        up._session = session
        path = write_file(tmp_path, "empty.csv", 0)

        with up:
            up.enqueue_file(path).result(timeout=10)

        assert session.parts == {1: b""}
        assert service.completed_etags["empty.csv"] == {1: '"etag-1"'}


class TestCreateValidation:
    @pytest.mark.parametrize("value", [0, -1])
    @pytest.mark.parametrize("kwarg", ["max_workers", "small_route_workers", "max_multipart_files_in_flight"])
    def test_pool_sizes_must_be_positive(self, kwarg: str, value: int) -> None:
        """A zero-width pool would silently swallow the whole batch; there is no unbounded mode."""
        with pytest.raises(ValueError, match=f"{kwarg} must be positive"):
            MultipartUploader.create(MagicMock(), upload_client=FakeUploadService(), **{kwarg: value})

    def test_small_file_route_ceiling_is_four_mib(self) -> None:
        assert MAX_SMALL_FILE_ROUTE_BYTES == 4 * 1024 * 1024

    @pytest.mark.parametrize("value", [0, -1, MAX_SMALL_FILE_ROUTE_BYTES + 1])
    def test_small_file_route_size_must_sit_inside_the_ceiling(self, value: int) -> None:
        """The single-shot endpoint holds a server thread per upload, so the opt-in is capped."""
        with pytest.raises(ValueError, match="small_file_route_max_bytes must be in"):
            MultipartUploader.create(MagicMock(), upload_client=FakeUploadService(), small_file_route_max_bytes=value)


class TestSmallFileRoute:
    def test_declares_the_real_size_and_never_touches_storage_directly(self, tmp_path) -> None:
        """`size_bytes` makes the server stream to storage instead of capping at its memory limit."""
        up, service, session = make_uploader(tmp_path, small_file_route_max_bytes=1024)
        path = write_file(tmp_path, "small.csv", 100)

        with up:
            assert up.enqueue_file(path).result(timeout=10) == "s3://bucket/small.csv"

        assert service.upload_file_args == [("small.csv", 100, 100)]  # (file name, size_bytes, body length)
        assert not session.put_started.is_set()  # no direct-to-storage PUT on this route

    def test_a_file_above_the_threshold_still_takes_multipart(self, tmp_path) -> None:
        up, service, _ = make_uploader(tmp_path, small_file_route_max_bytes=64)
        path = write_file(tmp_path, "big.csv", 200)

        with up:
            up.enqueue_file(path).result(timeout=10)

        assert service.upload_file_args == []
        assert service.calls == ["initiate", "sign", "complete"]


class _Throttled(Exception):
    """The server refusing a request because the caller is over its request budget."""

    status_code = 429


class _FakeClock:
    """Deterministic clock whose sleep advances it, so a throttle retry never really waits."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def install_test_gate(up: MultipartUploader, *, deadline_seconds: float = 120.0) -> _FakeClock:
    """Swap in a gate on a fake clock, so throttle retries are instant and countable.

    The pace rate is pinned (floor == initial) so pacing contributes no wall clock of its own
    and cannot drift between attempts; rate adaptation itself is covered in test_upload_pacing.
    """
    clock = _FakeClock()
    pacer = _AdaptivePacer(initial_rate=1000.0, min_rate=1000.0, clock=clock, sleep=clock.sleep)
    up._gate = _ThrottleGate(pacer, deadline_seconds=deadline_seconds, clock=clock)
    return clock


def throttle_first_call(service: FakeUploadService, method: str, counter: dict[str, int]):
    """Replace `service.method` with one that throttles once, then defers to the original."""
    original = getattr(service, method)

    def throttled_once(*args, **kwargs):
        counter["n"] += 1
        if counter["n"] == 1:
            raise _Throttled()
        return original(*args, **kwargs)

    setattr(service, method, throttled_once)


class TestThrottlingIsAbsorbedByTheGate:
    """Every Nominal API request the uploader makes has to go through the gate.

    A throttle is the server saying "later", not "no"; an ungated call site turns one into a
    dead file. These pin each call site by throttling it and requiring the file to survive.
    """

    @pytest.mark.parametrize("method", ["initiate_multipart_upload", "sign_part", "complete_multipart_upload"])
    def test_a_throttled_multipart_call_is_retried_rather_than_failing_the_file(self, tmp_path, method: str) -> None:
        up, service, _ = make_uploader(tmp_path)
        install_test_gate(up)
        calls = {"n": 0}
        throttle_first_call(service, method, calls)
        path = write_file(tmp_path, "f.csv", 100)

        with up:
            assert up.enqueue_file(path).result(timeout=10) == "s3://bucket/f.csv"

        assert calls["n"] == 2  # throttled once, retried inside the gate, then succeeded
        assert service.aborted == []

    def test_a_throttled_small_route_upload_is_retried_rather_than_failing_the_file(self, tmp_path) -> None:
        up, service, _ = make_uploader(tmp_path, small_file_route_max_bytes=1024)
        install_test_gate(up)
        calls = {"n": 0}
        throttle_first_call(service, "upload_file", calls)
        path = write_file(tmp_path, "small.csv", 100)

        with up:
            assert up.enqueue_file(path).result(timeout=10) == "s3://bucket/small.csv"

        assert calls["n"] == 2

    def test_a_permanently_throttled_sign_spends_exactly_one_budget(self, tmp_path) -> None:
        """A sign that throttles forever must exhaust one gate budget, not one per part retry.

        It also has to surface intact as `NominalRequestThrottledError` instead of being folded
        into a multipart failure group, which would hide why the file died.
        """
        up, service, _ = make_uploader(tmp_path, max_part_retries=3)
        clock = install_test_gate(up, deadline_seconds=3.0)
        calls = {"n": 0}

        def always_throttle(*args, **kwargs):
            calls["n"] += 1
            clock.now += 1.0  # each attempt burns a second of the budget
            raise _Throttled()

        service.sign_part = always_throttle
        path = write_file(tmp_path, "f.csv", 100)

        with up:
            with pytest.raises(NominalRequestThrottledError):
                up.enqueue_file(path).result(timeout=10)

        # A 3s budget at 1s per attempt is exactly 3 signs. If the per-part retry loop caught the
        # throttled-out call and handed it a fresh budget, this would be 3 * max_part_retries.
        assert calls["n"] == 3
        assert service.aborted == ["f.csv"]
