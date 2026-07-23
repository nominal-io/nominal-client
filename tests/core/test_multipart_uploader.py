from __future__ import annotations

import pathlib

from nominal.core._utils.multipart_uploader import _PartBounds, _PlannedUpload


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
