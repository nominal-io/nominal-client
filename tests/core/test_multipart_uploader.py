from __future__ import annotations

import pathlib
from concurrent.futures import Future, ThreadPoolExecutor
from unittest.mock import MagicMock, patch

import pytest

from nominal.core._utils.multipart_uploader import MultipartUploader, _FileUpload, _PartBounds, _PlannedUpload
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
        complete=complete or (lambda: "s3://bucket/obj"),
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
    complete.assert_called_once_with()
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

    def initiate_multipart_upload(self, auth_header, request):
        if self._fail_on_initiate:
            raise RuntimeError("initiate failed")
        return MagicMock(key=request.filename, upload_id=f"uid-{request.filename}")

    def sign_part(self, auth_header, key, part, upload_id):
        if self._fail_on_key is not None and key == self._fail_on_key:
            raise RuntimeError(f"sign failed for {key}")
        return MagicMock(url=f"https://s3/{key}/{part}", headers={})

    def list_parts(self, auth_header, key, upload_id):
        return [MagicMock(etag="etag", part_number=1)]

    def complete_multipart_upload(self, auth_header, key, upload_id, parts):
        return MagicMock(location=f"s3://bucket/{key}")

    def abort_multipart_upload(self, auth_header, key, upload_id):
        self.aborted.append(key)


def _uploader(client: _FakeUploadService) -> MultipartUploader:
    session = MagicMock(spec=["put", "close"])
    put_response = MagicMock()
    put_response.status_code = 200
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
