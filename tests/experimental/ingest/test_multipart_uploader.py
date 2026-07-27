from __future__ import annotations

import pathlib
import threading
from concurrent.futures import CancelledError
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from conjure_python_client import ServiceConfiguration
from conjure_python_client._http.configuration import SslConfiguration

from nominal.core.exceptions import NominalMultipartUploadFailed
from nominal.experimental.ingest._multipart_uploader import MultipartUploader


class FakeUploadService:
    """Counts calls; optional per-method gates let tests hold a call open."""

    def __init__(self) -> None:
        """Start with every gate open, so a test opts in to holding a call."""
        self.lock = threading.Lock()
        self.calls: list[str] = []
        self._n = 0
        self.upload_file_started = threading.Event()
        self.upload_file_release = threading.Event()
        self.upload_file_release.set()
        self._verify = False

    def _record(self, name: str) -> None:
        with self.lock:
            self.calls.append(name)

    def initiate_multipart_upload(self, auth, request):
        self._record("initiate")
        with self.lock:
            self._n += 1
            key = f"key-{self._n}"
        return SimpleNamespace(key=key, upload_id=f"uid-{key}")

    def sign_part(self, auth, key, part, upload_id):
        self._record("sign")
        return SimpleNamespace(url=f"https://s3.example/{key}/{part}", headers={})

    def complete_multipart_upload(self, auth, key, upload_id, parts):
        self._record("complete")
        return SimpleNamespace(location=f"s3://bucket/{key}")

    def abort_multipart_upload(self, auth, key, upload_id):
        self._record("abort")

    def upload_file(self, auth, body, file_name, size_bytes=None, workspace=None):
        self._record("upload_file")
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
    """A PUT session that parks part 1 and fails part 2, so a *late* part failure is observable.

    Part 2 waits for part 1 to park before failing, which makes "the last part failed while an
    earlier one is still uploading" a guarantee rather than a race.
    """

    def __init__(self) -> None:
        """Part 1 stays parked until `release` is set; part 2 always fails."""
        self.part_one_parked = threading.Event()
        self.release = threading.Event()
        self.closed = False

    def put(self, url, data=None, headers=None, verify=None, timeout=None):
        if url.endswith("/2"):
            self.part_one_parked.wait(timeout=10)  # order this failure after part 1 parks
            raise ConnectionError("part 2 failed")
        self.part_one_parked.set()
        self.release.wait(timeout=10)
        return SimpleNamespace(status_code=200, headers={"ETag": '"etag-1"'}, raise_for_status=lambda: None)

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
        up, service, session = make_uploader(tmp_path)
        session.put = MagicMock(
            return_value=SimpleNamespace(status_code=200, headers={}, raise_for_status=lambda: None)
        )
        path = write_file(tmp_path, "f.csv", 100)
        fut = up.enqueue_file(path)
        with pytest.raises(Exception, match="ETag"):
            fut.result(timeout=10)
        up.close()


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
