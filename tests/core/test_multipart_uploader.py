from __future__ import annotations

import pathlib
from concurrent.futures import Future
from unittest.mock import MagicMock

import pytest

from nominal.core._utils.multipart_uploader import _FileUpload, _PartBounds, _PlannedUpload


def _plan(total_size: int, part_size: int) -> _PlannedUpload:
    return _PlannedUpload(
        path=pathlib.Path("x"), key="k", upload_id="u", total_size=total_size, part_size=part_size
    )


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
