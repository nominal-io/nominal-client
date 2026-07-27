from __future__ import annotations

import logging
import pathlib
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from unittest.mock import MagicMock, patch

import pytest
import requests

from nominal.core._utils.multipart_uploader import (
    DEFAULT_THROTTLE_DEADLINE_S,
    MAX_SMALL_FILE_ROUTE_BYTES,
    MultipartUploader,
    _AdaptiveLimiter,
    _FileUpload,
    _is_throttle_error,
    _PartBounds,
    _PartResult,
    _PlannedUpload,
    _ThrottleGate,
)
from nominal.core.exceptions import (
    NominalMultipartUploadError,
    NominalMultipartUploadFailed,
    NominalRequestThrottledError,
)
from nominal.core.filetype import FileTypes
from nominal.experimental.ingest._ingest_builder import _Upload, _upload_all
from nominal.protos.ingest.v2 import file_ingest_pb2, ingest_service_pb2


def _plan(total_size: int, part_size: int) -> _PlannedUpload:
    return _PlannedUpload(path=pathlib.Path("x"), key="k", upload_id="u", total_size=total_size, part_size=part_size)


def test_parts_partial_final_chunk() -> None:
    assert list(_plan(12, 5).parts()) == [
        _PartBounds(part_number=1, offset=0, size=5),
        _PartBounds(part_number=2, offset=5, size=5),
        _PartBounds(part_number=3, offset=10, size=2),
    ]


def test_parts_exact_multiple() -> None:
    assert list(_plan(10, 5).parts()) == [
        _PartBounds(part_number=1, offset=0, size=5),
        _PartBounds(part_number=2, offset=5, size=5),
    ]


def test_parts_single_part() -> None:
    assert list(_plan(4, 5).parts()) == [_PartBounds(part_number=1, offset=0, size=4)]


def test_parts_empty_file_is_one_zero_byte_part() -> None:
    assert list(_plan(0, 5).parts()) == [_PartBounds(part_number=1, offset=0, size=0)]


def _done_future(result: _PartResult | None = None, exc: BaseException | None = None) -> "Future[_PartResult]":
    f: Future[_PartResult] = Future()
    if exc is not None:
        f.set_exception(exc)
    else:
        f.set_result(result if result is not None else _PartResult(part_number=1, etag='"e"'))
    return f


def _coordinator(num_parts: int, complete=None, abort=None) -> tuple[_FileUpload, "Future[str]"]:
    fut: Future[str] = Future()
    fu = _FileUpload(
        future=fut,
        num_parts=num_parts,
        complete=complete or (lambda etags: "s3://bucket/obj"),
        abort=abort or MagicMock(),
    )
    return fu, fut


def test_coordinator_all_parts_succeed_completes_once() -> None:
    complete = MagicMock(return_value="s3://bucket/obj")
    abort = MagicMock()
    fu, fut = _coordinator(2, complete=complete, abort=abort)
    fu.part_futures = [_done_future(_PartResult(1, '"a"')), _done_future(_PartResult(2, '"b"'))]

    for pf in fu.part_futures:
        fu.on_part_done(pf)

    assert fut.result() == "s3://bucket/obj"
    complete.assert_called_once_with({1: '"a"', 2: '"b"'})
    abort.assert_not_called()


def test_coordinator_part_failure_settles_and_aborts() -> None:
    complete = MagicMock()
    abort = MagicMock()
    fu, fut = _coordinator(2, complete=complete, abort=abort)
    err = RuntimeError("part failed")
    ok_future = _done_future()
    bad_future = _done_future(exc=err)
    fu.part_futures = [ok_future, bad_future]

    fu.on_part_done(bad_future)  # failure arrives first
    fu.on_part_done(ok_future)  # absorbed, must not complete

    with pytest.raises(RuntimeError, match="part failed"):
        fut.result()
    complete.assert_not_called()
    abort.assert_called_once_with(err)


def test_coordinator_completion_failure_settles_with_that_error() -> None:
    cerr = RuntimeError("complete failed")
    complete = MagicMock(side_effect=cerr)
    abort = MagicMock()
    fu, fut = _coordinator(1, complete=complete, abort=abort)
    fu.part_futures = [_done_future()]

    fu.on_part_done(fu.part_futures[0])

    with pytest.raises(RuntimeError, match="complete failed"):
        fut.result()
    abort.assert_called_once_with(cerr)


def test_coordinator_abort_failure_preserves_root_error() -> None:
    err = RuntimeError("root part error")
    abort = MagicMock(side_effect=RuntimeError("abort failed"))
    fu, fut = _coordinator(1, complete=MagicMock(), abort=abort)
    fu.part_futures = [_done_future(exc=err)]

    fu.on_part_done(fu.part_futures[0])  # must not raise out of the callback

    with pytest.raises(RuntimeError, match="root part error"):
        fut.result()
    abort.assert_called_once_with(err)


def test_coordinator_is_settle_once() -> None:
    complete = MagicMock(return_value="s3://bucket/obj")
    abort = MagicMock()
    fu, fut = _coordinator(1, complete=complete, abort=abort)
    fu.part_futures = [_done_future(exc=RuntimeError("first"))]

    fu.on_part_done(fu.part_futures[0])
    fu.on_part_done(_done_future())  # a late/extra callback

    assert complete.call_count == 0
    assert abort.call_count == 1


class _FakeUploadService:
    """Minimal fake of upload_api.UploadService for the whole multipart lifecycle.

    The object key is the request filename, so results stay deterministic even though the
    initiate calls run concurrently on the pool (a counter would race).
    """

    def __init__(self, *, fail_on_key: str | None = None, fail_on_initiate: bool = False) -> None:
        self._verify = False
        self._fail_on_key = fail_on_key
        self._fail_on_initiate = fail_on_initiate
        self.aborted: list[str] = []
        self.initiate_calls = 0
        self.sign_part_calls = 0
        self.complete_calls = 0
        self.completed_etags: dict[int, str] = {}
        self.upload_file_calls: list[tuple[str, int | None, int]] = []  # (file_name, size_bytes, body_len)

    def initiate_multipart_upload(self, auth_header, request):
        self.initiate_calls += 1
        if self._fail_on_initiate:
            raise RuntimeError("initiate failed")
        return MagicMock(key=request.filename, upload_id=f"uid-{request.filename}")

    def upload_file(self, auth_header, body, file_name, size_bytes=None, workspace=None) -> str:
        self.upload_file_calls.append((file_name, size_bytes, len(body)))
        return f"s3://backend/{file_name}"

    def sign_part(self, auth_header, key, part, upload_id):
        self.sign_part_calls += 1
        if self._fail_on_key is not None and key == self._fail_on_key:
            raise RuntimeError(f"sign failed for {key}")
        return MagicMock(url=f"https://s3/{key}/{part}", headers={})

    def list_parts(self, auth_header, key, upload_id):
        raise AssertionError("list_parts must not be called: etags come from the PUT responses")

    def complete_multipart_upload(self, auth_header, key, upload_id, parts):
        self.complete_calls += 1
        self.completed_etags = {p.part_number: p.etag for p in parts}
        return MagicMock(location=f"s3://bucket/{key}")

    def abort_multipart_upload(self, auth_header, key, upload_id):
        self.aborted.append(key)


def _test_gate(*, limit: int = 8) -> _ThrottleGate:
    """A gate for tests that don't exercise throttling.

    No real sleep and a short deadline, so a test that unexpectedly hits a throttled call fails
    fast instead of blocking for up to the real 120s default.
    """
    return _ThrottleGate(
        _AdaptiveLimiter(initial=limit, min_limit=1, max_limit=limit),
        deadline_seconds=1.0,
        sleep=lambda _seconds: None,
    )


def _uploader(client: _FakeUploadService) -> MultipartUploader:
    session = MagicMock(spec=["put", "close"])
    put_response = MagicMock()
    put_response.status_code = 200
    put_response.headers = {"ETag": '"etag"'}  # S3 returns the part ETag on the PUT
    session.put.return_value = put_response
    return MultipartUploader(
        max_workers=4,
        timeout=30.0,
        max_part_retries=2,
        _upload_client=client,
        _auth_header="auth",
        _workspace_rid=None,
        _session=session,
        _pool=ThreadPoolExecutor(max_workers=4),
        _closed=False,
        _gate=_test_gate(),
    )


def test_enqueue_file_resolves_to_location(tmp_path) -> None:
    f = tmp_path / "data.csv"  # name -> "data", safe_filename -> "data.csv", key -> "data.csv"
    f.write_bytes(b"0123456789")
    client = _FakeUploadService()
    with _uploader(client) as up:
        fut = up.enqueue_file(f, file_type=FileTypes.CSV, part_size=4)
        assert fut.result(timeout=5) == "s3://bucket/data.csv"


def test_single_part_file_costs_exactly_three_api_requests(tmp_path) -> None:
    """The central performance claim: initiate + sign_part + complete, and nothing else.

    Counted at the service boundary rather than by intercepting the gate, so this asserts the
    observable request count instead of the uploader's internals.
    """
    client = _FakeUploadService()
    session = MagicMock(spec=["put", "close"])
    session.put.return_value = MagicMock(status_code=200, headers={"ETag": '"e1"'})
    up = MultipartUploader(
        max_workers=4,
        timeout=30.0,
        max_part_retries=2,
        _upload_client=client,
        _auth_header="auth",
        _workspace_rid=None,
        _session=session,
        _pool=ThreadPoolExecutor(max_workers=4),
        _closed=False,
        _gate=_test_gate(),
    )
    f = tmp_path / "data.csv"
    f.write_bytes(b"0123456789")
    with up:
        assert up.enqueue_file(f, file_type=FileTypes.CSV, part_size=1000).result(timeout=5) == "s3://bucket/data.csv"

    # list_parts is covered by the fake raising AssertionError if it is ever called.
    assert (client.initiate_calls, client.sign_part_calls, client.complete_calls) == (1, 1, 1)
    assert client.completed_etags == {1: '"e1"'}


def test_multipart_etags_are_completed_in_part_number_order(tmp_path) -> None:
    client = _FakeUploadService()
    session = MagicMock(spec=["put", "close"])
    etags = iter(['"p1"', '"p2"', '"p3"'])

    def put(url, **kwargs):  # type: ignore[no-untyped-def]
        return MagicMock(status_code=200, headers={"ETag": next(etags)})

    session.put.side_effect = put
    up = MultipartUploader(
        max_workers=1,  # single worker so the etag iterator maps 1:1 onto ascending part numbers
        timeout=30.0,
        max_part_retries=2,
        _upload_client=client,
        _auth_header="auth",
        _workspace_rid=None,
        _session=session,
        _pool=ThreadPoolExecutor(max_workers=1),
        _closed=False,
        _gate=_test_gate(limit=1),
    )
    f = tmp_path / "data.bin"
    f.write_bytes(b"ABCDEFGHIJKL")  # 12 bytes, part_size 5 -> 3 parts
    with up:
        assert up.enqueue_file(f, file_type=FileTypes.CSV, part_size=5).result(timeout=10)

    assert client.completed_etags == {1: '"p1"', 2: '"p2"', 3: '"p3"'}


def test_missing_etag_header_fails_the_part_and_aborts(tmp_path) -> None:
    client = _FakeUploadService()
    session = MagicMock(spec=["put", "close"])
    session.put.return_value = MagicMock(status_code=200, headers={})  # no ETag
    up = MultipartUploader(
        max_workers=4,
        timeout=30.0,
        max_part_retries=3,  # >1 on purpose: proves the missing ETag does NOT consume retries
        _upload_client=client,
        _auth_header="auth",
        _workspace_rid=None,
        _session=session,
        _pool=ThreadPoolExecutor(max_workers=4),
        _closed=False,
        _gate=_test_gate(),
    )
    f = tmp_path / "data.csv"
    f.write_bytes(b"0123456789")
    with up:
        # Raised directly, not wrapped in NominalMultipartUploadFailed: a missing ETag is not
        # retryable, so it never accumulates attempt errors.
        with pytest.raises(NominalMultipartUploadError, match="no ETag for part 1"):
            up.enqueue_file(f, file_type=FileTypes.CSV, part_size=1000).result(timeout=5)

    assert client.aborted == ["data.csv"]  # the file still aborts its multipart upload
    assert session.put.call_count == 1  # failed on the first attempt; no pointless re-uploads


def test_initiate_failure_settles_future(tmp_path) -> None:
    f = tmp_path / "data.csv"
    f.write_bytes(b"data")
    client = _FakeUploadService(fail_on_initiate=True)
    with _uploader(client) as up:
        fut = up.enqueue_file(f, file_type=FileTypes.CSV)
        with pytest.raises(RuntimeError, match="initiate failed"):
            fut.result(timeout=5)
    assert client.aborted == []  # no upload_id was ever obtained -> nothing to abort


def test_enqueue_file_reads_correct_bytes_per_part(tmp_path) -> None:
    f = tmp_path / "data.bin"
    f.write_bytes(b"ABCDEFGHIJKL")  # 12 bytes, part_size 5 -> 5,5,2
    client = _FakeUploadService()
    up = _uploader(client)
    session = up._session
    try:
        up.enqueue_file(f, file_type=FileTypes.CSV, part_size=5).result(timeout=5)
    finally:
        up.close()

    sent = sorted(kwargs["data"] for _, kwargs in session.put.call_args_list)
    assert sent == sorted([b"ABCDE", b"FGHIJ", b"KL"])


def test_one_file_fails_others_still_resolve(tmp_path) -> None:
    good = tmp_path / "good.csv"  # key -> "good.csv"
    good.write_bytes(b"good-bytes")
    bad = tmp_path / "bad.csv"  # key -> "bad.csv"; signing this key fails
    bad.write_bytes(b"bad-bytes")
    client = _FakeUploadService(fail_on_key="bad.csv")
    with _uploader(client) as up:
        good_fut = up.enqueue_file(good, file_type=FileTypes.CSV, part_size=4)
        bad_fut = up.enqueue_file(bad, file_type=FileTypes.CSV, part_size=4)
        assert good_fut.result(timeout=5) == "s3://bucket/good.csv"
        # _upload_part wraps exhausted-retry failures in NominalMultipartUploadFailed (an ExceptionGroup subclass).
        with pytest.raises(NominalMultipartUploadFailed):
            bad_fut.result(timeout=5)
    assert client.aborted == ["bad.csv"]


def test_enqueue_missing_file_raises_synchronously(tmp_path) -> None:
    client = _FakeUploadService()
    with _uploader(client) as up:
        with pytest.raises(FileNotFoundError):
            up.enqueue_file(tmp_path / "nope.csv", file_type=FileTypes.CSV)


def test_close_shuts_down_pool_and_closes_session(tmp_path) -> None:
    client = _FakeUploadService()
    up = _uploader(client)
    up.close()
    assert up._closed is True
    up._session.close.assert_called_once()


def _make_upload(path: pathlib.Path) -> _Upload:
    item = ingest_service_pb2.IngestItem(file=file_ingest_pb2.FileIngestItem())
    return _Upload(path=path, file_type=FileTypes.CSV, target=item.file.source)


class _FakeUploader:
    """Stands in for MultipartUploader: enqueue_file returns an immediately-resolved future."""

    def __init__(self, results: dict[str, object]) -> None:
        self._results = results  # path name -> location str OR Exception

    def enqueue_file(self, path, *, file_type=None, name=None, part_size=None):
        fut: "Future[str]" = Future()
        outcome = self._results[path.name]
        if isinstance(outcome, Exception):
            fut.set_exception(outcome)
        else:
            fut.set_result(outcome)
        return fut

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None


def test_upload_all_fills_targets(tmp_path) -> None:
    a = tmp_path / "a.csv"
    b = tmp_path / "b.csv"
    a.write_bytes(b"a")
    b.write_bytes(b"b")
    uploads = [_make_upload(a), _make_upload(b)]
    fake = _FakeUploader({"a.csv": "s3://bucket/a", "b.csv": "s3://bucket/b"})

    with patch("nominal.experimental.ingest._ingest_builder.MultipartUploader.create", return_value=fake):
        _upload_all(uploads, None, MagicMock())

    assert uploads[0].target.s3.path == "s3://bucket/a"
    assert uploads[1].target.s3.path == "s3://bucket/b"


def test_upload_all_raises_on_failure(tmp_path) -> None:
    a = tmp_path / "a.csv"
    a.write_bytes(b"a")
    uploads = [_make_upload(a)]
    fake = _FakeUploader({"a.csv": RuntimeError("upload failed")})

    with patch("nominal.experimental.ingest._ingest_builder.MultipartUploader.create", return_value=fake):
        with pytest.raises(RuntimeError, match="upload failed"):
            _upload_all(uploads, None, MagicMock())


def test_create_smoke(tmp_path) -> None:
    session = MagicMock(spec=["put", "close"])
    put_response = MagicMock()
    put_response.status_code = 200
    put_response.headers = {"ETag": '"etag"'}
    session.put.return_value = put_response

    f = tmp_path / "data.csv"
    f.write_bytes(b"0123456789")
    client = _FakeUploadService()

    with patch("nominal.core._utils.multipart_uploader.create_multipart_request_session", return_value=session):
        # max_workers=1 for a deterministic single-threaded test.
        with MultipartUploader.create(
            upload_client=client, auth_header="auth", workspace_rid=None, max_workers=1
        ) as up:
            fut = up.enqueue_file(f, file_type=FileTypes.CSV, part_size=4)
            assert fut.result(timeout=5) == "s3://bucket/data.csv"


def test_empty_file_uploads_single_zero_byte_part(tmp_path) -> None:
    f = tmp_path / "empty.csv"
    f.write_bytes(b"")
    client = _FakeUploadService()
    up = _uploader(client)
    session = up._session
    try:
        fut = up.enqueue_file(f, file_type=FileTypes.CSV, part_size=5)
        assert fut.result(timeout=5) == "s3://bucket/empty.csv"
    finally:
        up.close()

    session.put.assert_called_once()
    _, kwargs = session.put.call_args
    assert kwargs["data"] == b""


def test_more_files_than_workers_no_deadlock(tmp_path) -> None:
    # Same shape as `_uploader(client)`, but a single-worker pool: `_run_upload` submits part
    # tasks and returns without ever waiting on a pool future, so this must not deadlock even
    # when many files are enqueued against one worker.
    client = _FakeUploadService()
    session = MagicMock(spec=["put", "close"])
    put_response = MagicMock()
    put_response.status_code = 200
    put_response.headers = {"ETag": '"etag"'}
    session.put.return_value = put_response
    up = MultipartUploader(
        max_workers=4,
        timeout=30.0,
        max_part_retries=2,
        _upload_client=client,
        _auth_header="auth",
        _workspace_rid=None,
        _session=session,
        _pool=ThreadPoolExecutor(max_workers=1),
        _closed=False,
        _gate=_test_gate(),
    )
    with up:
        files = [tmp_path / f"file{i}.csv" for i in range(5)]
        for f in files:
            f.write_bytes(b"0123456789")
        futures = [up.enqueue_file(f, file_type=FileTypes.CSV, part_size=4) for f in files]
        for f, fut in zip(files, futures):
            assert fut.result(timeout=10) == f"s3://bucket/{f.name}"


def test_create_rejects_nonpositive_max_files_in_flight() -> None:
    with pytest.raises(ValueError, match="max_files_in_flight must be positive"):
        MultipartUploader.create(
            upload_client=_FakeUploadService(), auth_header="auth", workspace_rid=None, max_files_in_flight=0
        )


def test_max_files_in_flight_backpressures_enqueue(tmp_path) -> None:
    """With max_files_in_flight=2, the 3rd enqueue blocks until an in-flight file completes.

    Slots are acquired on the caller thread and released when the file's future settles (on a
    pool worker), so the caller's blocked acquire() always unblocks as files finish.
    """
    gate = threading.Event()
    reached_complete = threading.Semaphore(0)

    class _GatingClient(_FakeUploadService):
        def complete_multipart_upload(self, auth_header, key, upload_id, parts):  # type: ignore[no-untyped-def]
            reached_complete.release()  # this file reached its final (complete) step
            gate.wait(10)  # hold it open (future unsettled -> slot held) until the test releases
            return super().complete_multipart_upload(auth_header, key, upload_id, parts)

    session = MagicMock(spec=["put", "close"])
    put_response = MagicMock()
    put_response.status_code = 200
    put_response.headers = {"ETag": '"etag"'}
    session.put.return_value = put_response
    up = MultipartUploader(
        max_workers=8,
        timeout=30.0,
        max_part_retries=2,
        _upload_client=_GatingClient(),
        _auth_header="auth",
        _workspace_rid=None,
        _session=session,
        _pool=ThreadPoolExecutor(max_workers=8),
        _closed=False,
        _gate=_test_gate(),
        _file_slots=threading.BoundedSemaphore(2),
    )
    paths = [tmp_path / f"f{i}.csv" for i in range(3)]
    for p in paths:
        p.write_bytes(b"data")

    try:
        f0 = up.enqueue_file(paths[0], file_type=FileTypes.CSV)
        f1 = up.enqueue_file(paths[1], file_type=FileTypes.CSV)
        # both files fill a slot and block in complete()
        assert reached_complete.acquire(timeout=10)
        assert reached_complete.acquire(timeout=10)

        third_returned = threading.Event()
        holder: dict[str, Future[str]] = {}

        def enqueue_third() -> None:
            holder["fut"] = up.enqueue_file(paths[2], file_type=FileTypes.CSV)
            third_returned.set()

        threading.Thread(target=enqueue_third, daemon=True).start()
        assert not third_returned.wait(0.5)  # blocked: both slots are held

        gate.set()  # let the two in-flight files complete -> release their slots
        assert third_returned.wait(10)  # 3rd enqueue now proceeds

        assert f0.result(timeout=10) == f"s3://bucket/{paths[0].name}"
        assert f1.result(timeout=10) == f"s3://bucket/{paths[1].name}"
        assert holder["fut"].result(timeout=10) == f"s3://bucket/{paths[2].name}"
    finally:
        gate.set()
        up.close()


def _small_file_uploader(client: _FakeUploadService, *, threshold: int) -> MultipartUploader:
    session = MagicMock(spec=["put", "close"])
    session.put.return_value = MagicMock(status_code=200, headers={"ETag": '"etag"'})
    return MultipartUploader(
        max_workers=4,
        timeout=30.0,
        max_part_retries=2,
        _upload_client=client,
        _auth_header="auth",
        _workspace_rid="ws-1",
        _session=session,
        _pool=ThreadPoolExecutor(max_workers=4),
        _closed=False,
        _gate=_test_gate(),
        _small_file_route_max_bytes=threshold,
    )


def test_small_file_route_uses_upload_file_when_enabled(tmp_path) -> None:
    """A file <= small_file_route_max_bytes uploads single-shot via upload_file, bypassing multipart."""
    client = _FakeUploadService()
    up = _small_file_uploader(client, threshold=1024)
    f = tmp_path / "small.csv"
    f.write_bytes(b"x" * 100)  # 100 bytes <= 1024
    with up:
        assert up.enqueue_file(f, file_type=FileTypes.CSV).result(timeout=5) == "s3://backend/small.csv"

    assert client.upload_file_calls == [("small.csv", 100, 100)]  # (file_name, size_bytes, body_len)
    assert client.initiate_calls == 0  # multipart bypassed
    up._session.put.assert_not_called()  # no direct-to-S3 PUT


def test_small_file_route_falls_back_to_multipart_above_threshold(tmp_path) -> None:
    client = _FakeUploadService()
    up = _small_file_uploader(client, threshold=64)
    f = tmp_path / "big.csv"
    f.write_bytes(b"x" * 200)  # 200 > 64 -> multipart
    with up:
        assert up.enqueue_file(f, file_type=FileTypes.CSV, part_size=1000).result(timeout=5) == "s3://bucket/big.csv"

    assert client.upload_file_calls == []
    assert client.initiate_calls == 1


def test_small_file_route_disabled_uses_multipart(tmp_path) -> None:
    client = _FakeUploadService()
    with _uploader(client) as up:  # no small_file_route_max_bytes -> disabled
        f = tmp_path / "small.csv"
        f.write_bytes(b"x" * 10)  # tiny, but the route is off
        up.enqueue_file(f, file_type=FileTypes.CSV, part_size=1000).result(timeout=5)

    assert client.upload_file_calls == []
    assert client.initiate_calls == 1


def test_create_rejects_oversized_small_file_route() -> None:
    with pytest.raises(ValueError, match="small_file_route_max_bytes must be in"):
        MultipartUploader.create(
            upload_client=_FakeUploadService(),
            auth_header="auth",
            workspace_rid=None,
            small_file_route_max_bytes=MAX_SMALL_FILE_ROUTE_BYTES + 1,
        )


def test_zero_byte_file_never_takes_the_small_file_route(tmp_path) -> None:
    """The single-shot endpoint rejects a declared size of zero, so empty files must go multipart."""
    client = _FakeUploadService()
    up = _small_file_uploader(client, threshold=4096)
    f = tmp_path / "empty.csv"
    f.write_bytes(b"")
    with up:
        assert up.enqueue_file(f, file_type=FileTypes.CSV, part_size=1000).result(timeout=5) == "s3://bucket/empty.csv"

    assert client.upload_file_calls == []
    assert client.initiate_calls == 1


def test_small_file_route_ceiling_is_four_mib() -> None:
    assert MAX_SMALL_FILE_ROUTE_BYTES == 4 * 1024 * 1024
    with pytest.raises(ValueError, match="small_file_route_max_bytes must be in"):
        MultipartUploader.create(
            upload_client=_FakeUploadService(),
            auth_header="auth",
            workspace_rid=None,
            small_file_route_max_bytes=MAX_SMALL_FILE_ROUTE_BYTES + 1,
        )


@pytest.mark.parametrize("part_size", [0, -1])
def test_enqueue_rejects_nonpositive_part_size(tmp_path, part_size: int) -> None:
    """Raise on the caller's thread: part_size=0 used to surface as ZeroDivisionError via the future."""
    client = _FakeUploadService()
    f = tmp_path / "data.csv"
    f.write_bytes(b"0123456789")
    with _uploader(client) as up:
        with pytest.raises(ValueError, match="part_size must be positive"):
            up.enqueue_file(f, file_type=FileTypes.CSV, part_size=part_size)


def test_enqueue_rejects_a_plan_exceeding_the_part_limit(tmp_path) -> None:
    client = _FakeUploadService()
    f = tmp_path / "data.csv"
    f.write_bytes(b"x" * 20_001)
    with _uploader(client) as up:
        with pytest.raises(ValueError, match="would need 20001 parts"):
            up.enqueue_file(f, file_type=FileTypes.CSV, part_size=1)


def test_enqueue_warns_on_undersized_multipart_parts(tmp_path, caplog) -> None:
    client = _FakeUploadService()
    f = tmp_path / "data.csv"
    f.write_bytes(b"x" * 100)
    with _uploader(client) as up:
        with caplog.at_level(logging.WARNING):
            up.enqueue_file(f, file_type=FileTypes.CSV, part_size=10).result(timeout=5)
    assert "below the storage provider's 5 MiB minimum" in caplog.text


def test_enqueue_does_not_warn_for_a_single_small_part(tmp_path, caplog) -> None:
    """A single-part upload has no minimum size, so warning there would be noise."""
    client = _FakeUploadService()
    f = tmp_path / "data.csv"
    f.write_bytes(b"x" * 100)
    with _uploader(client) as up:
        with caplog.at_level(logging.WARNING):
            up.enqueue_file(f, file_type=FileTypes.CSV, part_size=1000).result(timeout=5)
    assert "5 MiB minimum" not in caplog.text


def test_returned_future_is_not_cancellable(tmp_path) -> None:
    """Cancelling used to release the in-flight slot mid-upload and then skip the abort."""
    client = _FakeUploadService()
    session = MagicMock(spec=["put", "close"])
    session.put.return_value = MagicMock(status_code=200, headers={"ETag": '"e1"'})
    up = MultipartUploader(
        max_workers=2,
        timeout=30.0,
        max_part_retries=2,
        _upload_client=client,
        _auth_header="auth",
        _workspace_rid=None,
        _session=session,
        _pool=ThreadPoolExecutor(max_workers=2),
        _closed=False,
        _gate=_test_gate(),
        _file_slots=threading.BoundedSemaphore(1),
    )
    f = tmp_path / "data.csv"
    f.write_bytes(b"0123456789")
    with up:
        fut = up.enqueue_file(f, file_type=FileTypes.CSV, part_size=1000)
        assert fut.cancel() is False
        assert fut.result(timeout=5) == "s3://bucket/data.csv"


def test_coordinator_absorbs_a_cancelled_part_future() -> None:
    """fut.exception() raises CancelledError on a cancelled future; reading it would hang the file."""
    complete = MagicMock(return_value="s3://bucket/obj")
    abort = MagicMock()
    fu, fut = _coordinator(2, complete=complete, abort=abort)
    cancelled: Future[_PartResult] = Future()
    assert cancelled.cancel()
    fu.part_futures = [cancelled]

    fu.on_part_done(cancelled)  # must not raise, must not settle

    assert not fut.done()
    complete.assert_not_called()
    abort.assert_not_called()


def test_adaptive_limiter_blocks_at_limit_and_admits_on_release() -> None:
    lim = _AdaptiveLimiter(initial=2, min_limit=1, max_limit=10)
    lim.acquire()
    lim.acquire()  # 2 in flight == limit 2
    admitted = threading.Event()
    threading.Thread(target=lambda: (lim.acquire(), admitted.set()), daemon=True).start()
    assert not admitted.wait(0.2)  # blocked at the limit
    lim.release()
    assert admitted.wait(1.0)  # a freed slot admits the waiter


def test_adaptive_limiter_grows_on_success() -> None:
    lim = _AdaptiveLimiter(initial=1, min_limit=1, max_limit=10)
    for _ in range(20):
        lim.on_success()
    assert 3.0 < lim.limit <= 10.0  # additive increase grew it toward, but not past, the ceiling


def test_adaptive_limiter_shrinks_on_throttle_with_cooldown() -> None:
    now = [0.0]
    lim = _AdaptiveLimiter(initial=8, min_limit=1, max_limit=10, decrease=0.5, cooldown=1.0, clock=lambda: now[0])
    lim.on_throttle()
    assert lim.limit == 4.0  # 8 * 0.5
    lim.on_throttle()  # within the cooldown window -> ignored (one overload shouldn't collapse it)
    assert lim.limit == 4.0
    now[0] = 2.0
    lim.on_throttle()  # past cooldown -> cuts again
    assert lim.limit == 2.0


def test_is_throttle_error_recognizes_retry_exhaustion() -> None:
    assert _is_throttle_error(requests.exceptions.RetryError("too many 429 error responses"))


def test_is_throttle_error_recognizes_a_429_response() -> None:
    exc = requests.HTTPError("rejected")
    exc.response = MagicMock(status_code=429)
    assert _is_throttle_error(exc)


def test_is_throttle_error_rejects_other_failures() -> None:
    server_error = requests.HTTPError("boom")
    server_error.response = MagicMock(status_code=500)
    assert not _is_throttle_error(server_error)
    assert not _is_throttle_error(ValueError("nothing to do with rate limits"))


def test_is_throttle_error_does_not_inspect_exception_groups() -> None:
    """By design, classification happens on the raw request error before per-part wrapping.

    A wrapped group never reaches the gate, so we deliberately do not unwrap one. This test
    documents that boundary -- if it ever starts returning True, classification has leaked
    downstream of where it belongs.
    """
    inner = requests.exceptions.RetryError("too many 429 error responses")
    wrapped = NominalMultipartUploadError(f"part 1 attempt 1: {inner}")
    group = NominalMultipartUploadFailed("part 1 failed after 3 attempts", [wrapped])
    assert not _is_throttle_error(group)


def test_adaptive_limiter_stays_within_bounds() -> None:
    now = [0.0]
    lim = _AdaptiveLimiter(initial=5, min_limit=2, max_limit=6, cooldown=1.0, clock=lambda: now[0])
    for _ in range(200):
        lim.on_success()
    assert lim.limit == 6.0
    for tick in range(12):
        now[0] = float(tick * 2)  # step past the cooldown so every throttle lands
        lim.on_throttle()
    assert lim.limit == 2.0


def test_adaptive_limiter_admits_waiter_after_shrinking_below_inflight() -> None:
    """Shrinking the limit under the in-flight count must not lose a wakeup."""
    lim = _AdaptiveLimiter(initial=4, min_limit=1, max_limit=4)
    for _ in range(4):
        lim.acquire()
    lim.on_throttle()  # limit 4 -> 2 while 4 are in flight

    admitted = threading.Event()
    threading.Thread(target=lambda: (lim.acquire(), admitted.set()), daemon=True).start()
    assert not admitted.wait(0.2)
    for _ in range(3):
        lim.release()  # drains to 1 in flight, below the new limit of 2
    assert admitted.wait(1.0)


class _FakeClock:
    """Manual clock plus a sleep that advances it, so backoff is instant and deterministic."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _gate(limiter: _AdaptiveLimiter, clock: _FakeClock, *, deadline: float = 120.0) -> _ThrottleGate:
    return _ThrottleGate(
        limiter,
        deadline_seconds=deadline,
        clock=clock.time,
        sleep=clock.sleep,
        backoff=lambda attempt: float(2**attempt),  # deterministic, no jitter, for assertions
    )


def test_gate_returns_result_and_grows_the_limit_on_success() -> None:
    limiter = _AdaptiveLimiter(initial=2, min_limit=1, max_limit=8)
    clock = _FakeClock()
    before = limiter.limit

    assert _gate(limiter, clock).call(lambda: "ok") == "ok"

    assert limiter.limit > before
    assert clock.sleeps == []


def test_gate_retries_a_throttled_call_then_succeeds() -> None:
    limiter = _AdaptiveLimiter(initial=8, min_limit=1, max_limit=8)
    clock = _FakeClock()
    calls = {"n": 0}

    def op() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise requests.exceptions.RetryError("too many 429 error responses")
        return "done"

    assert _gate(limiter, clock).call(op) == "done"

    assert calls["n"] == 3
    assert clock.sleeps == [1.0, 2.0]  # 2**0, 2**1
    assert limiter.limit < 8.0  # shrank on the way


def test_gate_raises_when_the_throttle_deadline_is_exhausted() -> None:
    limiter = _AdaptiveLimiter(initial=4, min_limit=1, max_limit=4)
    clock = _FakeClock()
    root = requests.exceptions.RetryError("too many 429 error responses")

    def op() -> str:
        raise root

    with pytest.raises(NominalRequestThrottledError) as excinfo:
        _gate(limiter, clock, deadline=10.0).call(op)

    assert excinfo.value.__cause__ is root
    assert "for 10.0s (budget 10.0s) across 5 attempts" in str(excinfo.value)  # elapsed, not just the budget
    assert clock.now <= 10.0  # never sleeps past the deadline


def test_gate_fails_fast_on_a_non_throttle_error() -> None:
    limiter = _AdaptiveLimiter(initial=4, min_limit=1, max_limit=4)
    clock = _FakeClock()

    def op() -> str:
        raise ValueError("bad request")

    with pytest.raises(ValueError, match="bad request"):
        _gate(limiter, clock).call(op)

    assert clock.sleeps == []  # no retry
    limiter.acquire()  # the slot was released, so this must not block


def test_gate_admits_only_up_to_the_limit() -> None:
    limiter = _AdaptiveLimiter(initial=1, min_limit=1, max_limit=1)
    gate = _gate(limiter, _FakeClock())
    inside = threading.Event()
    release = threading.Event()

    def blocking_op() -> str:
        inside.set()
        release.wait(10)
        return "first"

    threading.Thread(target=lambda: gate.call(blocking_op), daemon=True).start()
    assert inside.wait(10)

    second_done = threading.Event()
    threading.Thread(target=lambda: (gate.call(lambda: "second"), second_done.set()), daemon=True).start()
    assert not second_done.wait(0.3)  # limit is 1 and it is taken

    release.set()
    assert second_done.wait(10)


def test_gate_does_not_hold_its_slot_while_backing_off() -> None:
    """A thread sleeping out a throttle must not occupy the concurrency it just asked to shrink."""
    limiter = _AdaptiveLimiter(initial=1, min_limit=1, max_limit=1)
    backoff_entered = threading.Event()
    release_backoff = threading.Event()

    def sleep(_seconds: float) -> None:
        backoff_entered.set()
        release_backoff.wait(10)

    gate = _ThrottleGate(limiter, deadline_seconds=120.0, clock=time.monotonic, sleep=sleep, backoff=lambda _a: 0.01)
    attempts = {"n": 0}

    def op() -> str:
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise requests.exceptions.RetryError("too many 429 error responses")
        return "ok"

    threading.Thread(target=lambda: gate.call(op), daemon=True).start()
    assert backoff_entered.wait(10)

    acquired = threading.Event()
    threading.Thread(target=lambda: (limiter.acquire(), acquired.set()), daemon=True).start()
    assert acquired.wait(5), "the backing-off thread is still holding its slot"
    release_backoff.set()


def test_files_exceed_workers_at_gate_limit_one_without_deadlock(tmp_path) -> None:
    """The invariant: no pool task waits on another pool task, so the tightest possible gate
    (limit 1) against a single-worker pool and many files must still drain.
    """
    client = _FakeUploadService()
    session = MagicMock(spec=["put", "close"])
    session.put.return_value = MagicMock(status_code=200, headers={"ETag": '"e1"'})
    up = MultipartUploader(
        max_workers=1,
        timeout=30.0,
        max_part_retries=2,
        _upload_client=client,
        _auth_header="auth",
        _workspace_rid=None,
        _session=session,
        _pool=ThreadPoolExecutor(max_workers=1),
        _closed=False,
        _gate=_test_gate(limit=1),
    )
    with up:
        files = [tmp_path / f"file{i}.csv" for i in range(5)]
        for f in files:
            f.write_bytes(b"0123456789")
        futures = [up.enqueue_file(f, file_type=FileTypes.CSV, part_size=4) for f in files]
        for f, fut in zip(files, futures):
            assert fut.result(timeout=20) == f"s3://bucket/{f.name}"


def test_throttled_sign_part_is_retried_rather_than_failing_the_file(tmp_path) -> None:
    """A throttled sign used to burn max_part_retries and kill the file; now the gate absorbs it."""
    calls = {"n": 0}

    class _ThrottleSignOnce(_FakeUploadService):
        def sign_part(self, auth_header, key, part, upload_id):  # type: ignore[no-untyped-def]
            calls["n"] += 1
            if calls["n"] == 1:
                raise requests.exceptions.RetryError("too many 429 error responses")
            return super().sign_part(auth_header, key, part, upload_id)

    client = _ThrottleSignOnce()
    session = MagicMock(spec=["put", "close"])
    session.put.return_value = MagicMock(status_code=200, headers={"ETag": '"e1"'})
    limiter = _AdaptiveLimiter(initial=8, min_limit=1, max_limit=8)
    clock = _FakeClock()
    up = MultipartUploader(
        max_workers=4,
        timeout=30.0,
        max_part_retries=2,
        _upload_client=client,
        _auth_header="auth",
        _workspace_rid=None,
        _session=session,
        _pool=ThreadPoolExecutor(max_workers=4),
        _closed=False,
        _gate=_ThrottleGate(
            limiter, deadline_seconds=120.0, clock=clock.time, sleep=clock.sleep, backoff=lambda a: float(2**a)
        ),
    )
    f = tmp_path / "data.csv"
    f.write_bytes(b"0123456789")
    with up:
        assert up.enqueue_file(f, file_type=FileTypes.CSV, part_size=1000).result(timeout=5) == "s3://bucket/data.csv"

    assert calls["n"] == 2  # throttled once, retried inside the gate, succeeded
    assert limiter.limit < 8.0


def test_throttled_upload_file_is_retried_rather_than_failing_the_file(tmp_path) -> None:
    """A throttled upload_file (small-file route) used to fail the file outright; now the gate absorbs it."""
    calls = {"n": 0}

    class _ThrottleUploadFileOnce(_FakeUploadService):
        def upload_file(self, auth_header, body, file_name, size_bytes=None, workspace=None):  # type: ignore[no-untyped-def]
            calls["n"] += 1
            if calls["n"] == 1:
                raise requests.exceptions.RetryError("too many 429 error responses")
            return super().upload_file(auth_header, body, file_name, size_bytes=size_bytes, workspace=workspace)

    client = _ThrottleUploadFileOnce()
    session = MagicMock(spec=["put", "close"])
    limiter = _AdaptiveLimiter(initial=8, min_limit=1, max_limit=8)
    clock = _FakeClock()
    up = MultipartUploader(
        max_workers=4,
        timeout=30.0,
        max_part_retries=2,
        _upload_client=client,
        _auth_header="auth",
        _workspace_rid=None,
        _session=session,
        _pool=ThreadPoolExecutor(max_workers=4),
        _closed=False,
        _gate=_ThrottleGate(
            limiter, deadline_seconds=120.0, clock=clock.time, sleep=clock.sleep, backoff=lambda a: float(2**a)
        ),
        _small_file_route_max_bytes=1024,
    )
    f = tmp_path / "small.csv"
    f.write_bytes(b"x" * 10)  # 10 bytes <= 1024 -> small-file route
    with up:
        assert up.enqueue_file(f, file_type=FileTypes.CSV).result(timeout=5) == "s3://backend/small.csv"

    assert calls["n"] == 2  # throttled once, retried inside the gate, succeeded


def test_throttled_initiate_is_retried_rather_than_failing_the_file(tmp_path) -> None:
    """A throttled initiate used to fail the file outright; now the gate absorbs it."""
    calls = {"n": 0}

    class _ThrottleInitiateOnce(_FakeUploadService):
        def initiate_multipart_upload(self, auth_header, request):  # type: ignore[no-untyped-def]
            calls["n"] += 1
            if calls["n"] == 1:
                raise requests.exceptions.RetryError("too many 429 error responses")
            return super().initiate_multipart_upload(auth_header, request)

    client = _ThrottleInitiateOnce()
    session = MagicMock(spec=["put", "close"])
    session.put.return_value = MagicMock(status_code=200, headers={"ETag": '"e1"'})
    limiter = _AdaptiveLimiter(initial=8, min_limit=1, max_limit=8)
    clock = _FakeClock()
    up = MultipartUploader(
        max_workers=4,
        timeout=30.0,
        max_part_retries=2,
        _upload_client=client,
        _auth_header="auth",
        _workspace_rid=None,
        _session=session,
        _pool=ThreadPoolExecutor(max_workers=4),
        _closed=False,
        _gate=_ThrottleGate(
            limiter, deadline_seconds=120.0, clock=clock.time, sleep=clock.sleep, backoff=lambda a: float(2**a)
        ),
    )
    f = tmp_path / "data.csv"
    f.write_bytes(b"0123456789")
    with up:
        assert up.enqueue_file(f, file_type=FileTypes.CSV, part_size=1000).result(timeout=5) == "s3://bucket/data.csv"

    assert calls["n"] == 2  # throttled once, retried inside the gate, succeeded


def test_permanently_throttled_sign_spends_only_one_gate_budget(tmp_path) -> None:
    """A sign that never stops being throttled must exhaust exactly ONE gate throttle budget --
    not get caught by the per-part retry loop, wrapped, and given a fresh budget max_part_retries
    times over. The failure must also surface intact as NominalRequestThrottledError, not buried
    inside a NominalMultipartUploadFailed group.
    """
    calls = {"n": 0}

    class _AlwaysThrottleSign(_FakeUploadService):
        def sign_part(self, auth_header, key, part, upload_id):  # type: ignore[no-untyped-def]
            calls["n"] += 1
            raise requests.exceptions.RetryError("too many 429 error responses")

    client = _AlwaysThrottleSign()
    session = MagicMock(spec=["put", "close"])
    clock = _FakeClock()
    limiter = _AdaptiveLimiter(initial=8, min_limit=1, max_limit=8)
    up = MultipartUploader(
        max_workers=4,
        timeout=30.0,
        max_part_retries=3,
        _upload_client=client,
        _auth_header="auth",
        _workspace_rid=None,
        _session=session,
        _pool=ThreadPoolExecutor(max_workers=4),
        _closed=False,
        _gate=_ThrottleGate(limiter, deadline_seconds=3.0, clock=clock.time, sleep=clock.sleep, backoff=lambda _a: 1.0),
    )
    f = tmp_path / "data.csv"
    f.write_bytes(b"0123456789")
    with up:
        fut = up.enqueue_file(f, file_type=FileTypes.CSV, part_size=1000)
        with pytest.raises(NominalRequestThrottledError):
            fut.result(timeout=5)

    # deadline=3.0, backoff=1.0 -> one gate budget is exactly 4 sign attempts (elapsed 0,1,2,3).
    # If a throttled-out sign were re-caught and retried by the part loop, this would be 4 * 3 = 12.
    assert calls["n"] == 4


def test_create_has_no_adaptive_concurrency_flag() -> None:
    """AIMD is unconditional now, so the flag is gone rather than defaulting to False."""
    import inspect

    params = inspect.signature(MultipartUploader.create).parameters
    assert "adaptive_concurrency" not in params
    assert params["throttle_deadline"].default == DEFAULT_THROTTLE_DEADLINE_S
