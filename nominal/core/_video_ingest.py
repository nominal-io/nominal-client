from __future__ import annotations

from typing import Iterable, Mapping, Protocol

from nominal_api import api, datasource_api, ingest_api, scout_datasource, scout_video, scout_video_api

from nominal.core._clientsbunch import HasScoutParams
from nominal.core.bounds import Bounds
from nominal.ts import _SecondsNanos


class _VideoChannelClients(HasScoutParams, Protocol):
    """The clients needed to address a video channel and the files backing it."""

    @property
    def video(self) -> scout_video.VideoService: ...
    @property
    def datasource(self) -> scout_datasource.DataSourceService: ...


def build_video_ingest_options(
    target_rid: str,
    *,
    channel: str,
    tags: Mapping[str, str] | None,
    s3_path: str,
    timestamp_manifest: scout_video_api.VideoFileTimestampManifest,
    overwrite_overlapping: bool,
) -> ingest_api.IngestOptions:
    """Build IngestOptions for a VideoOptsV2 ingest into an existing dataset channel."""
    return ingest_api.IngestOptions(
        video_v2=ingest_api.VideoOptsV2(
            source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path)),
            target=ingest_api.DatasetIngestTarget(
                existing=ingest_api.ExistingDatasetIngestDestination(dataset_rid=target_rid)
            ),
            timestamp_manifest=timestamp_manifest,
            channel=channel,
            tags={**(tags or {})},
            over_write_segments=overwrite_overlapping or None,
        )
    )


def video_channel_series(
    dataset_rid: str, channel: str, tags: Mapping[str, str] | None = None
) -> scout_video_api.VideoChannelSeries:
    """Reference a datasource-backed video channel.

    Tags act as a filter on the channel's backing series, so omitting them addresses every series on
    the channel (one series is created per originating ingest job).
    """
    return scout_video_api.VideoChannelSeries(
        data_source=scout_video_api.VideoDataSourceChannel(
            data_source_rid=dataset_rid,
            channel=channel,
            tags={**(tags or {})},
        )
    )


def iter_video_channel_names(clients: _VideoChannelClients, dataset_rid: str) -> Iterable[str]:
    """Yield the names of every VIDEO-typed channel on a dataset."""
    next_page_token = None
    while True:
        response = clients.datasource.search_channels(
            clients.auth_header,
            datasource_api.SearchChannelsRequest(
                data_sources=[dataset_rid],
                exact_match=[],
                fuzzy_search_text="",
                previously_selected_channels={},
                next_page_token=next_page_token,
                data_types=[],
                page_size=None,
                prefix=None,
            ),
        )
        for channel_metadata in response.results:
            if channel_metadata.data_type == api.SeriesDataType.VIDEO:
                yield channel_metadata.name
        if response.next_page_token is None:
            break
        next_page_token = response.next_page_token


def resolve_video_channel_for_file(
    clients: _VideoChannelClients,
    dataset_rid: str,
    dataset_file_id: str,
    bounds: Bounds | None,
) -> str:
    """Find which video channel on a dataset a given dataset file backs.

    The Catalog row for a video file does not record its channel, so this probes each of the dataset's
    video channels for the file. `bounds` (the file's own time range) narrows each probe to the pages
    that could contain the file rather than the channel's whole history.
    """
    probe_bounds = (
        None
        if bounds is None
        else scout_video_api.Bounds(
            start=_SecondsNanos.from_nanoseconds(bounds.start).to_api(),
            end=_SecondsNanos.from_nanoseconds(bounds.end).to_api(),
        )
    )
    for channel in iter_video_channel_names(clients, dataset_rid):
        if _channel_contains_file(clients, dataset_rid, channel, dataset_file_id, probe_bounds):
            return channel
    raise ValueError(
        f"could not determine the video channel for dataset file {dataset_file_id!r} on dataset "
        f"{dataset_rid!r}: no video channel on the dataset contains it"
    )


def _channel_contains_file(
    clients: _VideoChannelClients,
    dataset_rid: str,
    channel: str,
    dataset_file_id: str,
    probe_bounds: scout_video_api.Bounds | None,
) -> bool:
    next_page_token = None
    while True:
        response = clients.video.list_video_channel_dataset_files(
            clients.auth_header,
            scout_video_api.ListVideoChannelDatasetFilesRequest(
                channel_series=video_channel_series(dataset_rid, channel),
                bounds=probe_bounds,
                page_size=None,
                token=next_page_token,
            ),
        )
        if any(file.dataset_file_id == dataset_file_id for file in response.dataset_files):
            return True
        if response.next_page_token is None:
            return False
        next_page_token = response.next_page_token
