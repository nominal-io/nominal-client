from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core.exceptions import LegacyVideoDeprecationWarning
from nominal.core.video import Video

# Requires the `video` extra to import the rust bindings, plus GStreamer at load time. exc_type covers
# the CI case where the package is installed but its GStreamer libs are not (ImportError on import),
# which importorskip otherwise reports as a warning rather than a skip.
pytest.importorskip("nominal_video", exc_type=ImportError)

from nominal.experimental.video import Src, VideoStream  # noqa: E402


def _clients_returning_whip_url(response_attr: str) -> MagicMock:
    clients = MagicMock()
    response = MagicMock(whip_url="https://mediamtx.example/whip/abc?token=tok", ice_servers=[])
    getattr(clients.video, response_attr).return_value = response
    return clients


def _dataset(clients: MagicMock, rid: str = "ri.catalog.dataset.1") -> MagicMock:
    # A bare mock is deliberately not a Video, so create() takes the dataset arm.
    dataset = MagicMock()
    dataset.rid = rid
    dataset._clients = clients
    return dataset


def test_create_streams_to_a_dataset_channel():
    """A dataset target resolves its WHIP endpoint from the channel-series v2 endpoint."""
    clients = _clients_returning_whip_url("generate_whip_stream_v2")
    stream = VideoStream.create(
        _dataset(clients), Src.file("in.mp4"), channel="camera/front", tags={"vehicle": "alpha"}
    )

    clients.video.generate_whip_stream.assert_not_called()
    _auth, request = clients.video.generate_whip_stream_v2.call_args[0]
    source = request.channel_series.data_source
    assert (source.data_source_rid, source.channel, source.tags) == (
        "ri.catalog.dataset.1",
        "camera/front",
        {"vehicle": "alpha"},
    )
    assert stream.rid == "ri.catalog.dataset.1"


def test_create_requires_a_channel_for_a_dataset():
    """Without a channel there is no series to stream to, so fail before calling Nominal."""
    clients = _clients_returning_whip_url("generate_whip_stream_v2")
    with pytest.raises(ValueError, match="'channel' is required"):
        VideoStream.create(_dataset(clients), Src.file("in.mp4"))
    clients.video.generate_whip_stream_v2.assert_not_called()


def test_create_to_legacy_video_warns_and_uses_v1():
    """The legacy Video arm still works, on the v1 endpoint, and warns."""
    clients = _clients_returning_whip_url("generate_whip_stream")
    video = MagicMock(spec=Video)
    video.rid = "ri.video.v.1"
    video._clients = clients

    with pytest.warns(LegacyVideoDeprecationWarning, match="video channels on a dataset"):
        stream = VideoStream.create(video, Src.file("in.mp4"))

    clients.video.generate_whip_stream.assert_called_once()
    clients.video.generate_whip_stream_v2.assert_not_called()
    assert stream.rid == "ri.video.v.1"


def test_create_rejects_a_channel_for_a_legacy_video():
    """A channel is meaningless for a standalone video; don't silently ignore it."""
    clients = _clients_returning_whip_url("generate_whip_stream")
    video = MagicMock(spec=Video)
    video._clients = clients
    with pytest.raises(ValueError, match="does not apply"):
        VideoStream.create(video, Src.file("in.mp4"), channel="camera/front")
    clients.video.generate_whip_stream.assert_not_called()
