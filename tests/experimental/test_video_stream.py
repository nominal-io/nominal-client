from __future__ import annotations

import warnings
from unittest.mock import MagicMock

import pytest

from nominal.core.video import Video
from nominal.exceptions import LegacyVideoDeprecationWarning

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


def _mock_sink(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    sink = MagicMock()
    monkeypatch.setattr("nominal.experimental.video._video_stream.Sink", sink)
    return sink


def test_refresh_requests_a_new_whip_stream_for_the_same_channel(monkeypatch: pytest.MonkeyPatch):
    """The refresh hook asks Nominal for a fresh WHIP stream on the same series and returns its credentials."""
    sink = _mock_sink(monkeypatch)
    clients = MagicMock()
    clients.video.generate_whip_stream_v2.side_effect = [
        MagicMock(whip_url="https://mediamtx.example/whip/abc?token=tok1", ice_servers=[]),
        MagicMock(whip_url="https://mediamtx.example/whip/def?token=tok2", ice_servers=[]),
    ]
    VideoStream.create(_dataset(clients), Src.file("in.mp4"), channel="camera/front", tags={"vehicle": "alpha"})

    whip_kwargs = sink.whip.call_args.kwargs
    assert (whip_kwargs["endpoint"], whip_kwargs["token"]) == ("https://mediamtx.example/whip/abc", "tok1")
    assert whip_kwargs["refresh_credentials"]() == ("https://mediamtx.example/whip/def", "tok2")
    first, second = clients.video.generate_whip_stream_v2.call_args_list
    assert first == second


def test_refresh_for_a_legacy_video_does_not_warn(monkeypatch: pytest.MonkeyPatch):
    """The deprecation warning fires once from create(), never from the refresh on nominal-video's thread."""
    sink = _mock_sink(monkeypatch)
    clients = _clients_returning_whip_url("generate_whip_stream")
    video = MagicMock(spec=Video)
    video.rid = "ri.video.v.1"
    video._clients = clients

    with pytest.warns(LegacyVideoDeprecationWarning):
        VideoStream.create(video, Src.file("in.mp4"))
    with warnings.catch_warnings():
        warnings.simplefilter("error", LegacyVideoDeprecationWarning)
        sink.whip.call_args.kwargs["refresh_credentials"]()

    assert clients.video.generate_whip_stream.call_count == 2
    assert clients.video.generate_whip_stream.call_args[0][1] == "ri.video.v.1"
