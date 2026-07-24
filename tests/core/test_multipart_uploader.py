from __future__ import annotations

import pathlib
from concurrent.futures import Future, ThreadPoolExecutor
from unittest.mock import MagicMock, patch

import pytest

from nominal.core._utils.multipart_uploader import (
    MAX_SMALL_FILE_ROUTE_BYTES,
    MultipartUploader,
    _AdaptiveLimiter,
    _FileUpload,
    _PartBounds,
    _PlannedUpload,
)
from nominal.core.exceptions import NominalMultipartUploadFailed
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


def _done_future(result: object = None, exc: BaseException | None = None) -> "Future[None]":
    f: Future[None] = Future()
    if exc is not None:
        f.set_exception(exc)
    else:
        f.set_result(result)  # type: ignore[arg-type]
    return f


def _coordinator(num_parts: int, complete=None, abort=None) -> tuple[_FileUpload, "Future[str]"]:
    fut: Future[str] = Future()
    fu = _FileUpload(
        future=fut,
        num_parts=num_parts,
        complete=complete or (lambda part_etags: "s3://bucket/obj"),
        abort=abort or MagicMock(),
    )
    return fu, fut


def test_coordinator_all_parts_succeed_completes_once() -> None:
    complete = MagicMock(return_value="s3://bucket/obj")
    abort = MagicMock()
    fu, fut = _coordinator(2, complete=complete, abort=abort)
    fu.part_futures = [_done_future(), _done_future()]

    for pf in fu.part_futures:
        fu.on_part_done(pf)

    assert fut.result() == "s3://bucket/obj"
    complete.assert_called_once()  # called with the collected part ETags
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
        self.list_parts_calls = 0
        self.completed_parts: list[list[tuple[str, int]]] = []  # per-complete: [(etag, part_number), ...]
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
        if self._fail_on_key is not None and key == self._fail_on_key:
            raise RuntimeError(f"sign failed for {key}")
        return MagicMock(url=f"https://s3/{key}/{part}", headers={})

    def list_parts(self, auth_header, key, upload_id):
        self.list_parts_calls += 1
        return [MagicMock(etag="etag", part_number=1)]

    def complete_multipart_upload(self, auth_header, key, upload_id, parts):
        self.completed_parts.append([(p.etag, p.part_number) for p in parts])
        return MagicMock(location=f"s3://bucket/{key}")

    def abort_multipart_upload(self, auth_header, key, upload_id):
        self.aborted.append(key)


def _uploader(client: _FakeUploadService) -> MultipartUploader:
    session = MagicMock(spec=["put", "close"])
    put_response = MagicMock()
    put_response.status_code = 200
    put_response.headers = {"ETag": "etag-put"}  # S3 returns the part ETag on the PUT
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
    )


def test_enqueue_file_resolves_to_location(tmp_path) -> None:
    f = tmp_path / "data.csv"  # name -> "data", safe_filename -> "data.csv", key -> "data.csv"
    f.write_bytes(b"0123456789")
    client = _FakeUploadService()
    with _uploader(client) as up:
        fut = up.enqueue_file(f, file_type=FileTypes.CSV, part_size=4)
        assert fut.result(timeout=5) == "s3://bucket/data.csv"


def test_single_part_upload_completes_without_list_parts(tmp_path) -> None:
    f = tmp_path / "one.csv"
    f.write_bytes(b"0123456789")  # tiny -> one part at the default part size
    client = _FakeUploadService()
    with _uploader(client) as up:
        assert up.enqueue_file(f, file_type=FileTypes.CSV).result(timeout=5) == "s3://bucket/one.csv"
    assert client.list_parts_calls == 0  # single part -> list_parts round-trip skipped
    assert client.completed_parts == [[("etag-put", 1)]]  # completed straight from the PUT's ETag


def test_multipart_upload_completes_via_list_parts(tmp_path) -> None:
    f = tmp_path / "multi.csv"
    f.write_bytes(b"0123456789")  # part_size=4 -> 3 parts
    client = _FakeUploadService()
    with _uploader(client) as up:
        assert up.enqueue_file(f, file_type=FileTypes.CSV, part_size=4).result(timeout=5) == "s3://bucket/multi.csv"
    assert client.list_parts_calls == 1  # >1 part -> list_parts stays authoritative


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
        # _sign_and_put_part (Task 1) wraps exhausted-retry failures in NominalMultipartUploadFailed
        # (an ExceptionGroup subclass), not a bare RuntimeError -- see task-4-report.md Concerns.
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
    import threading

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
    session.put.return_value = MagicMock(status_code=200)
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


def test_adaptive_limiter_blocks_at_limit_and_admits_on_release() -> None:
    import threading

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


def test_adaptive_small_file_retries_on_throttle_then_succeeds(tmp_path) -> None:
    """In adaptive mode a throttled (429) upload_file backs off and retries instead of failing."""
    import requests

    n = {"calls": 0}

    class _ThrottleOnce(_FakeUploadService):
        def upload_file(self, auth_header, body, file_name, size_bytes=None, workspace=None) -> str:  # type: ignore[no-untyped-def]
            n["calls"] += 1
            if n["calls"] == 1:
                raise requests.exceptions.RetryError("too many 429 error responses")
            return super().upload_file(auth_header, body, file_name, size_bytes, workspace)

    client = _ThrottleOnce()
    up = MultipartUploader(
        max_workers=4,
        timeout=30.0,
        max_part_retries=2,
        _upload_client=client,
        _auth_header="auth",
        _workspace_rid="ws-1",
        _session=MagicMock(spec=["put", "close"]),
        _pool=ThreadPoolExecutor(max_workers=4),
        _closed=False,
        _small_file_route_max_bytes=4096,
        _limiter=_AdaptiveLimiter(initial=2, min_limit=1, max_limit=4),
    )
    f = tmp_path / "small.csv"
    f.write_bytes(b"x" * 100)
    with up:
        assert up.enqueue_file(f, file_type=FileTypes.CSV).result(timeout=10) == "s3://backend/small.csv"

    assert n["calls"] == 2  # throttled once, backed off, retried, succeeded
    assert client.upload_file_calls == [("small.csv", 100, 100)]  # only the successful call recorded


def test_adaptive_grows_limit_from_multipart_successes(tmp_path) -> None:
    """Regression: adaptive mode must grow the limit from MULTIPART successes too — otherwise the
    limit stays at its initial value (1) and multipart-only workloads collapse to serial."""
    client = _FakeUploadService()
    session = MagicMock(spec=["put", "close"])
    session.put.return_value = MagicMock(status_code=200)
    limiter = _AdaptiveLimiter(initial=1, min_limit=1, max_limit=8)
    up = MultipartUploader(
        max_workers=8,
        timeout=30.0,
        max_part_retries=2,
        _upload_client=client,
        _auth_header="auth",
        _workspace_rid=None,
        _session=session,
        _pool=ThreadPoolExecutor(max_workers=8),
        _closed=False,
        _small_file_route_max_bytes=1,  # 1-byte threshold -> every file takes the multipart path
        _limiter=limiter,
    )
    files = [tmp_path / f"f{i}.csv" for i in range(10)]
    for f in files:
        f.write_bytes(b"0123456789")
    with up:
        futs = [up.enqueue_file(f, file_type=FileTypes.CSV, part_size=4) for f in files]
        for f, fut in zip(files, futs):
            assert fut.result(timeout=10) == f"s3://bucket/{f.name}"

    assert limiter.limit > 1.0  # grew from multipart completions (the bug left it stuck at 1.0)
