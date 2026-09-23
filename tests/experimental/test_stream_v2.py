from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.experimental import stream_v2


def test_create_write_stream_is_deprecated():
    """Creating a stream_v2 write stream warns and points at `get_write_stream(track_metrics=True)`."""
    connection = MagicMock()
    connection.nominal_data_source_rid = "ri.catalog.ws.dataset.abc"

    with (
        pytest.warns(UserWarning, match=r"get_write_stream\(\)`, passing track_metrics=True"),
        stream_v2.create_write_stream(connection, serialize_process_workers=1),
    ):
        pass
