from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.experimental import stream_v2


def test_create_write_stream_is_deprecated():
    """Creating a stream_v2 write stream warns and points at `get_write_stream(track_metrics=True)`."""
    with pytest.warns(UserWarning, match=r"get_write_stream\(\)`, passing track_metrics=True") as record:
        stream_v2.create_write_stream(MagicMock())

    assert record[0].filename == __file__
