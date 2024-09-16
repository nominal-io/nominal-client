from __future__ import annotations

import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime
from io import TextIOBase
from pathlib import Path
from typing import BinaryIO, Iterable, Mapping, Sequence

import certifi
from conjure_python_client import RequestsClient, ServiceConfiguration, SslConfiguration
from typing_extensions import Self

from .. import _config
from .._api.combined import (
    attachments_api,
    ingest_api,
    scout,
    scout_catalog,
    scout_run_api,
    scout_video,
    scout_video_api,
    upload_api,
)
from .._utils import (
    FileType,
    FileTypes,
    IntegralNanosecondsUTC,
    TimestampColumnType,
    _flexible_time_to_conjure_ingest_api,
    _flexible_time_to_conjure_scout_run_api,
    _timestamp_type_to_conjure_ingest_api,
    construct_user_agent_string,
)
from ._multipart import put_multipart_upload
from ._utils import rid_from_instance_or_string, verify_csv_path
from .attachment import Attachment
from .dataset import Dataset
from .run import Run
from .video import Video


@dataclass(frozen=True)
class NominalClient:
    _auth_header: str = field(repr=False)
    _run_client: scout.RunService = field(repr=False)
    _upload_client: upload_api.UploadService = field(repr=False)
    _ingest_client: ingest_api.IngestService = field(repr=False)
    _catalog_client: scout_catalog.CatalogService = field(repr=False)
    _attachment_client: attachments_api.AttachmentService = field(repr=False)
    _video_client: scout_video.VideoService = field(repr=False)

    @classmethod
    def create(cls, base_url: str, token: str | None, trust_store_path: str | None = None) -> Self:
        """Create a connection to the Nominal platform.

        base_url: The URL of the Nominal API platform, e.g. "https://api.gov.nominal.io/api".
        token: An API token to authenticate with. By default, the token will be looked up in ~/.nominal.yml.
        trust_store_path: path to a trust store CA root file to initiate SSL connections. If not provided,
            certifi's trust store is used.
        """
        if token is None:
            token = _config.get_token(base_url)
        trust_store_path = certifi.where() if trust_store_path is None else trust_store_path
        cfg = ServiceConfiguration(uris=[base_url], security=SslConfiguration(trust_store_path=trust_store_path))

        agent = construct_user_agent_string()
        run_client = RequestsClient.create(scout.RunService, agent, cfg)
        upload_client = RequestsClient.create(upload_api.UploadService, agent, cfg)
        ingest_client = RequestsClient.create(ingest_api.IngestService, agent, cfg)
        catalog_client = RequestsClient.create(scout_catalog.CatalogService, agent, cfg)
        attachment_client = RequestsClient.create(attachments_api.AttachmentService, agent, cfg)
        video_client = RequestsClient.create(scout_video.VideoService, agent, cfg)
        auth_header = f"Bearer {token}"
        return cls(
            _auth_header=auth_header,
            _run_client=run_client,
            _upload_client=upload_client,
            _ingest_client=ingest_client,
            _catalog_client=catalog_client,
            _attachment_client=attachment_client,
            _video_client=video_client,
        )

    def create_run(
        self,
        name: str,
        start: datetime | IntegralNanosecondsUTC,
        end: datetime | IntegralNanosecondsUTC,
        description: str | None = None,
        *,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] = (),
        attachments: Iterable[Attachment] | Iterable[str] = (),
    ) -> Run:
        """Create a run."""
        # TODO(alkasm): support links
        request = scout_run_api.CreateRunRequest(
            attachments=[rid_from_instance_or_string(a) for a in attachments],
            data_sources={},
            description=description or "",
            labels=list(labels),
            links=[],
            properties={} if properties is None else dict(properties),
            start_time=_flexible_time_to_conjure_scout_run_api(start),
            title=name,
            end_time=_flexible_time_to_conjure_scout_run_api(end),
        )
        response = self._run_client.create_run(self._auth_header, request)
        return Run._from_conjure(self, response)

    def get_run(self, run: Run | str) -> Run:
        """Retrieve a run by run or run RID."""
        run_rid = rid_from_instance_or_string(run)
        response = self._run_client.get_run(self._auth_header, run_rid)
        return Run._from_conjure(self, response)

    def _search_runs_paginated(self, request: scout_run_api.SearchRunsRequest) -> Iterable[scout_run_api.Run]:
        while True:
            response = self._run_client.search_runs(self._auth_header, request)
            yield from response.results
            if response.next_page_token is None:
                break
            request = scout_run_api.SearchRunsRequest(
                page_size=request.page_size,
                query=request.query,
                sort=request.sort,
                next_page_token=response.next_page_token,
            )

    def _iter_search_runs(
        self,
        start: datetime | IntegralNanosecondsUTC | None = None,
        end: datetime | IntegralNanosecondsUTC | None = None,
        exact_name: str | None = None,
        label: str | None = None,
        property: tuple[str, str] | None = None,
    ) -> Iterable[Run]:
        request = scout_run_api.SearchRunsRequest(
            page_size=100,
            query=_create_search_runs_query(start, end, exact_name, label, property),
            sort=scout_run_api.SortOptions(
                field=scout_run_api.SortField.START_TIME,
                is_descending=True,
            ),
        )
        for run in self._search_runs_paginated(request):
            yield Run._from_conjure(self, run)

    def search_runs(
        self,
        start: datetime | IntegralNanosecondsUTC | None = None,
        end: datetime | IntegralNanosecondsUTC | None = None,
        exact_name: str | None = None,
        label: str | None = None,
        property: tuple[str, str] | None = None,
    ) -> Sequence[Run]:
        """Search for runs meeting the specified filters.
        Filters are ANDed together, e.g. `(run.label == label) AND (run.end <= end)`
        - `start` and `end` times are both inclusive
        - `exact_name` is case-insensitive
        - `property` is a key-value pair, e.g. ("name", "value")
        """
        return list(self._iter_search_runs(start, end, exact_name, label, property))

    def create_csv_dataset(
        self,
        path: Path | str,
        name: str | None,
        timestamp_column: str,
        timestamp_type: TimestampColumnType,
        description: str | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
    ) -> Dataset:
        """Create a dataset from a CSV file.

        If name is None, the name of the file will be used.

        See `create_dataset_from_io` for more details.
        """
        path, file_type = verify_csv_path(path)
        if name is None:
            name = path.name
        with open(path, "rb") as csv_file:
            return self.create_dataset_from_io(
                csv_file,
                name,
                timestamp_column,
                timestamp_type,
                file_type,
                description,
                labels=labels,
                properties=properties,
            )

    def create_dataset_from_io(
        self,
        dataset: BinaryIO,
        name: str,
        timestamp_column: str,
        timestamp_type: TimestampColumnType,
        file_type: tuple[str, str] | FileType = FileTypes.CSV,
        description: str | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
    ) -> Dataset:
        """Create a dataset from a file-like object.
        The dataset must be a file-like object in binary mode, e.g. open(path, "rb") or io.BytesIO.
        If the file is not in binary-mode, the requests library blocks indefinitely.

        Timestamp column types must be a `CustomTimestampFormat` or one of the following literals:
            "iso_8601": ISO 8601 formatted strings,
            "epoch_{unit}": epoch timestamps in UTC (floats or ints),
            "relative_{unit}": relative timestamps (floats or ints),
            where {unit} is one of: nanoseconds | microseconds | milliseconds | seconds | minutes | hours | days
        """
        # TODO(alkasm): create dataset from file/path
        if isinstance(dataset, TextIOBase):
            raise TypeError(f"dataset {dataset} must be open in binary mode, rather than text mode")

        file_type = FileType(*file_type)
        urlsafe_name = urllib.parse.quote_plus(name)
        filename = f"{urlsafe_name}{file_type.extension}"

        s3_path = put_multipart_upload(self._auth_header, dataset, filename, file_type.mimetype, self._upload_client)
        request = ingest_api.TriggerFileIngest(
            destination=ingest_api.IngestDestination(
                new_dataset=ingest_api.NewDatasetIngestDestination(
                    labels=list(labels),
                    properties={} if properties is None else dict(properties),
                    channel_config=None,  # TODO(alkasm): support offsets
                    dataset_description=description,
                    dataset_name=name,
                )
            ),
            source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path)),
            source_metadata=ingest_api.IngestSourceMetadata(
                timestamp_metadata=ingest_api.TimestampMetadata(
                    series_name=timestamp_column,
                    timestamp_type=_timestamp_type_to_conjure_ingest_api(timestamp_type),
                ),
            ),
        )
        response = self._ingest_client.trigger_file_ingest(self._auth_header, request)
        return self.get_dataset(response.dataset_rid)

    def create_video_from_io(
        self,
        video: BinaryIO,
        name: str,
        start: datetime | IntegralNanosecondsUTC,
        description: str | None = None,
        file_type: tuple[str, str] | FileType = FileTypes.MP4,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
    ) -> Video:
        """Create a video from a file-like object.

        The video must be a file-like object in binary mode, e.g. open(path, "rb") or io.BytesIO.
        """
        if isinstance(video, TextIOBase):
            raise TypeError(f"video {video} must be open in binary mode, rather than text mode")

        file_type = FileType(*file_type)
        urlsafe_name = urllib.parse.quote_plus(name)
        filename = f"{urlsafe_name}{file_type.extension}"

        s3_path = put_multipart_upload(self._auth_header, video, filename, file_type.mimetype, self._upload_client)
        request = ingest_api.IngestVideoRequest(
            labels=list(labels),
            properties={} if properties is None else dict(properties),
            sources=[ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path))],
            timestamps=ingest_api.VideoTimestampManifest(
                no_manifest=ingest_api.NoTimestampManifest(
                    starting_timestamp=_flexible_time_to_conjure_ingest_api(start)
                )
            ),
            description=description,
            title=name,
        )
        response = self._ingest_client.ingest_video(self._auth_header, request)
        return self.get_video(response.video_rid)

    def get_video(self, video: Video | str) -> Video:
        """Retrieve a video by video or video RID."""
        video_rid = rid_from_instance_or_string(video)
        response = _get_video(self._auth_header, self._video_client, video_rid)
        return Video._from_conjure(self, response)

    def _iter_get_videos(self, videos: Iterable[Video] | Iterable[str]) -> Iterable[Video]:
        video_rids = [rid_from_instance_or_string(v) for v in videos]
        for response in _get_videos(self._auth_header, self._video_client, video_rids):
            yield Video._from_conjure(self, response)

    def get_videos(self, videos: Iterable[Video] | Iterable[str]) -> Sequence[Video]:
        """Retrieve videos by video or video RID."""
        return list(self._iter_get_videos(videos))

    def get_dataset(self, dataset: Dataset | str) -> Dataset:
        """Retrieve a dataset by dataset or dataset RID."""
        dataset_rid = rid_from_instance_or_string(dataset)
        response = _get_dataset(self._auth_header, self._catalog_client, dataset_rid)
        return Dataset._from_conjure(self, response)

    def _iter_get_datasets(self, datasets: Iterable[Dataset] | Iterable[str]) -> Iterable[Dataset]:
        dataset_rids = (rid_from_instance_or_string(ds) for ds in datasets)
        for ds in _get_datasets(self._auth_header, self._catalog_client, dataset_rids):
            yield Dataset._from_conjure(self, ds)

    def get_datasets(self, datasets: Iterable[Dataset] | Iterable[str]) -> Sequence[Dataset]:
        """Retrieve datasets by dataset or dataset RID."""
        return list(self._iter_get_datasets(datasets))

    def _search_datasets(self) -> Iterable[Dataset]:
        # TODO(alkasm): search filters
        # TODO(alkasm): put in public API when we decide if we only expose search, or search + list.
        request = scout_catalog.SearchDatasetsRequest(
            query=scout_catalog.SearchDatasetsQuery(
                or_=[
                    scout_catalog.SearchDatasetsQuery(archive_status=False),
                    scout_catalog.SearchDatasetsQuery(archive_status=True),
                ]
            ),
            sort_options=scout_catalog.SortOptions(field=scout_catalog.SortField.INGEST_DATE, is_descending=True),
        )
        response = self._catalog_client.search_datasets(self._auth_header, request)
        for ds in response.results:
            yield Dataset._from_conjure(self, ds)

    def create_attachment_from_io(
        self,
        attachment: BinaryIO,
        name: str,
        file_type: tuple[str, str] | FileType = FileTypes.BINARY,
        description: str | None = None,
        *,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] = (),
    ) -> Attachment:
        """Upload an attachment.
        The attachment must be a file-like object in binary mode, e.g. open(path, "rb") or io.BytesIO.
        If the file is not in binary-mode, the requests library blocks indefinitely.
        """

        # TODO(alkasm): create attachment from file/path
        if isinstance(attachment, TextIOBase):
            raise TypeError(f"attachment {attachment} must be open in binary mode, rather than text mode")

        file_type = FileType(*file_type)
        urlsafe_name = urllib.parse.quote_plus(name)
        filename = f"{urlsafe_name}{file_type.extension}"

        s3_path = put_multipart_upload(self._auth_header, attachment, filename, file_type.mimetype, self._upload_client)
        request = attachments_api.CreateAttachmentRequest(
            description=description or "",
            labels=list(labels),
            properties={} if properties is None else dict(properties),
            s3_path=s3_path,
            title=name,
        )
        response = self._attachment_client.create(self._auth_header, request)
        return Attachment._from_conjure(self, response)

    def get_attachment(self, attachment: Attachment | str) -> Attachment:
        """Retrieve an attachment by attachment or attachment RID."""
        attachment_rid = rid_from_instance_or_string(attachment)
        response = self._attachment_client.get(self._auth_header, attachment_rid)
        return Attachment._from_conjure(self, response)

    def _iter_get_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> Iterable[Attachment]:
        rids = [rid_from_instance_or_string(a) for a in attachments]
        request = attachments_api.GetAttachmentsRequest(attachment_rids=rids)
        response = self._attachment_client.get_batch(self._auth_header, request)
        for a in response.response:
            yield Attachment._from_conjure(self, a)

    def get_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> Sequence[Attachment]:
        return list(self._iter_get_attachments(attachments))


def _create_search_runs_query(
    start: datetime | IntegralNanosecondsUTC | None = None,
    end: datetime | IntegralNanosecondsUTC | None = None,
    exact_name: str | None = None,
    label: str | None = None,
    property: tuple[str, str] | None = None,
) -> scout_run_api.SearchQuery:
    queries = []
    if start is not None:
        q = scout_run_api.SearchQuery(start_time_inclusive=_flexible_time_to_conjure_scout_run_api(start))
        queries.append(q)
    if end is not None:
        q = scout_run_api.SearchQuery(end_time_inclusive=_flexible_time_to_conjure_scout_run_api(end))
        queries.append(q)
    if exact_name is not None:
        q = scout_run_api.SearchQuery(exact_match=exact_name)
        queries.append(q)
    if label is not None:
        q = scout_run_api.SearchQuery(label=label)
        queries.append(q)
    if property is not None:
        name, value = property
        q = scout_run_api.SearchQuery(property=scout_run_api.Property(name=name, value=value))
        queries.append(q)
    return scout_run_api.SearchQuery(and_=queries)


def _get_datasets(
    auth_header: str, client: scout_catalog.CatalogService, dataset_rids: Iterable[str]
) -> Iterable[scout_catalog.EnrichedDataset]:
    request = scout_catalog.GetDatasetsRequest(dataset_rids=list(dataset_rids))
    yield from client.get_enriched_datasets(auth_header, request)


def _get_dataset(
    auth_header: str, client: scout_catalog.CatalogService, dataset_rid: str
) -> scout_catalog.EnrichedDataset:
    datasets = list(_get_datasets(auth_header, client, [dataset_rid]))
    if not datasets:
        raise ValueError(f"dataset {dataset_rid!r} not found")
    if len(datasets) > 1:
        raise ValueError(f"expected exactly one dataset, got {len(datasets)}")
    return datasets[0]


def _get_videos(
    auth_header: str, client: scout_video.VideoService, video_rids: Iterable[str]
) -> Iterable[scout_video_api.Video]:
    request = scout_video_api.GetVideosRequest(video_rids=list(video_rids))
    yield from client.batch_get(auth_header, request).responses


def _get_video(auth_header: str, client: scout_video.VideoService, video_rid: str) -> scout_video_api.Video:
    videos = list(_get_videos(auth_header, client, [video_rid]))
    if not videos:
        raise ValueError(f"video {video_rid!r} not found")
    if len(videos) > 1:
        raise ValueError(f"expected exactly one dataset, got {len(videos)}")
    return videos[0]
