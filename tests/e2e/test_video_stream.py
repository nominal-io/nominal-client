"""End-to-end test for WHIP credential refresh on a live video stream.

Streams about 40 s of test pattern in two sessions to one dataset channel. The second session starts with an
invalid WHIP token, so the WHIP server answers 401 and nominal-video asks `VideoStream`'s refresh hook for a new
WHIP stream. The test then checks that video from both sessions landed in the one channel series, with no gap
beyond the reconnect itself.

Needs a Nominal stack with live video (gov-staging has it; EUW2 has no MediaMTX configuration), GStreamer, and the
`video` extra.
"""

from __future__ import annotations

import time
from datetime import timedelta
from typing import Callable, Mapping
from uuid import uuid4

import pytest
from nominal_api import api, scout_video_api

from nominal.core import NominalClient
from nominal.core.dataset import Dataset
from nominal.core.video import Video
from nominal.ts import _SecondsNanos

pytest.importorskip("nominal_video", exc_type=ImportError)

from nominal.experimental.video import Src, VideoStream  # noqa: E402

SEND_SECONDS = 20
SEGMENT_DEADLINE = timedelta(minutes=2)
# Live video is stored in ~6 s segments with about a frame between them, so the gap at the session boundary is
# allowed the reconnect time plus this slack.
GAP_SLACK_NS = 2_000_000_000


def _bad_token_on_first_request(
    real_request: Callable[..., scout_video_api.GenerateWhipStreamResponse], calls: list[str]
) -> Callable[..., scout_video_api.GenerateWhipStreamResponse]:
    """Wrap `_request_whip_stream` so its first response carries a token the WHIP server rejects."""

    def request(
        target: Dataset | Video, *, channel: str | None, tags: Mapping[str, str] | None
    ) -> scout_video_api.GenerateWhipStreamResponse:
        resp = real_request(target, channel=channel, tags=tags)
        calls.append(resp.stream_id)
        if len(calls) > 1:
            return resp
        endpoint, _ = VideoStream._split_whip_url(resp.whip_url)
        return scout_video_api.GenerateWhipStreamResponse(
            ice_servers=resp.ice_servers, stream_id=resp.stream_id, whip_url=f"{endpoint}?token=invalid"
        )

    return request


def _ns(timestamp: api.Timestamp) -> int:
    return _SecondsNanos.from_api(timestamp).to_nanoseconds()


def _starting_in(
    segments: list[scout_video_api.SegmentSummaryV2], start: int, end: int
) -> list[scout_video_api.SegmentSummaryV2]:
    return [s for s in segments if start <= _ns(s.min_absolute_timestamp) <= end]


def test_refreshed_stream_lands_in_the_same_series(
    client: NominalClient, archive: Callable[[object], None], monkeypatch: pytest.MonkeyPatch
):
    dataset = client.create_dataset(f"whip-credential-refresh-{uuid4()}")
    archive(dataset)
    channel = "camera"

    first_start = time.time_ns()
    first = VideoStream.create(dataset, Src.test(), channel=channel)
    first.open()
    first.run(SEND_SECONDS)
    first_end = time.time_ns()

    calls: list[str] = []
    monkeypatch.setattr(
        VideoStream,
        "_request_whip_stream",
        staticmethod(_bad_token_on_first_request(VideoStream._request_whip_stream, calls)),
    )
    second_start = time.time_ns()
    second = VideoStream.create(dataset, Src.test(), channel=channel)
    second.open()
    reopened = time.time_ns()
    second.run(SEND_SECONDS)
    second_end = time.time_ns()
    assert len(calls) == 2, "the rejected token should trigger exactly one refresh"

    clients = dataset._clients
    series = scout_video_api.VideoChannelSeries(
        data_source=scout_video_api.VideoDataSourceChannel(channel=channel, data_source_rid=dataset.rid, tags={})
    )
    bounds = scout_video_api.Bounds(
        start=_SecondsNanos.from_nanoseconds(first_start - 60_000_000_000).to_api(),
        end=_SecondsNanos.from_nanoseconds(second_end + 60_000_000_000).to_api(),
    )
    segments_request = scout_video_api.GetSegmentSummariesInBoundsForChannelRequest(
        bounds=bounds, channel_series=series
    )

    deadline = time.monotonic() + SEGMENT_DEADLINE.total_seconds()
    while True:
        segments = sorted(
            clients.video.get_segment_summaries_in_bounds_v2(clients.auth_header, segments_request),
            key=lambda s: _ns(s.min_absolute_timestamp),
        )
        if (
            _starting_in(segments, first_start, first_end) and _starting_in(segments, second_start, second_end)
        ) or time.monotonic() > deadline:
            break
        time.sleep(5)

    streams = clients.video.get_streams_in_bounds_v2(
        clients.auth_header,
        scout_video_api.GetStreamsInBoundsForChannelRequest(bounds=bounds, channel_series=series),
    ).streams
    published = {s.stream_id for s in streams if s.start is not None}
    print(f"streams: {[(s.stream_id, s.start, s.end) for s in streams]}; refresh minted {calls[1]}")
    assert len(published) >= 2, f"expected the first and refreshed sessions to publish, got {streams}"
    assert calls[1] in published

    assert _starting_in(segments, first_start, first_end), "no segment from the first session"
    assert _starting_in(segments, second_start, second_end), "no segment from the refreshed session"
    for earlier, later in zip(segments, segments[1:]):
        assert _ns(earlier.max_absolute_timestamp) <= _ns(later.min_absolute_timestamp), "segments overlap"

    last_before = max(_ns(s.max_absolute_timestamp) for s in segments if _ns(s.min_absolute_timestamp) < second_start)
    first_after = min(_ns(s.min_absolute_timestamp) for s in segments if _ns(s.min_absolute_timestamp) >= second_start)
    gap = first_after - last_before
    reconnect = reopened - first_end
    print(f"gap at the session boundary: {gap / 1e9:.2f} s; close-to-reopen: {reconnect / 1e9:.2f} s")
    assert gap <= reconnect + GAP_SLACK_NS
