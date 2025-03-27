from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from io import TextIOBase
from pathlib import Path
from typing import BinaryIO, Iterable, Mapping, Sequence

import certifi
from conjure_python_client import ServiceConfiguration, SslConfiguration
from nominal_api import (
    api,
    attachments_api,
    datasource,
    datasource_logset_api,
    ingest_api,
    scout_asset_api,
    scout_catalog,
    scout_checklistexecution_api,
    scout_checks_api,
    scout_datareview_api,
    scout_datasource_connection_api,
    scout_notebook_api,
    scout_run_api,
    scout_video_api,
    storage_datasource_api,
    timeseries_logicalseries_api,
)
from typing_extensions import Self

from nominal import _config
from nominal._utils import deprecate_keyword_argument
from nominal.core._clientsbunch import ClientsBunch
from nominal.core._conjure_utils import _available_units, _build_unit_update
from nominal.core._multipart import path_upload_name, upload_multipart_file, upload_multipart_io
from nominal.core._utils import construct_user_agent_string, rid_from_instance_or_string
from nominal.core.asset import Asset
from nominal.core.attachment import Attachment, _iter_get_attachments
from nominal.core.channel import Channel
from nominal.core.checklist import Checklist
from nominal.core.connection import Connection, StreamingConnection
from nominal.core.data_review import DataReview, DataReviewBuilder
from nominal.core.dataset import (
    Dataset,
    _create_dataflash_ingest_request,
    _create_mcap_channels,
    _create_mcap_ingest_request,
    _get_dataset,
    _get_datasets,
)
from nominal.core.filetype import FileType, FileTypes
from nominal.core.log import Log, LogSet, _get_log_set
from nominal.core.run import Run
from nominal.core.unit import Unit
from nominal.core.user import User, _get_user
from nominal.core.video import Video, _build_video_file_timestamp_manifest
from nominal.core.workbook import Workbook
from nominal.exceptions import NominalError, NominalIngestError
from nominal.ts import (
    IntegralNanosecondsUTC,
    LogTimestampType,
    _AnyTimestampType,
    _SecondsNanos,
    _to_typed_timestamp_type,
)

logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 100


@dataclass(frozen=True)
class NominalClient:
    _clients: ClientsBunch = field(repr=False)

    @classmethod
    def create(
        cls, base_url: str, token: str | None, trust_store_path: str | None = None, connect_timeout: float = 30
    ) -> Self:
        """Create a connection to the Nominal platform.

        base_url: The URL of the Nominal API platform, e.g. "https://api.gov.nominal.io/api".
        token: An API token to authenticate with. By default, the token will be looked up in ~/.nominal.yml.
        trust_store_path: path to a trust store CA root file to initiate SSL connections. If not provided,
            certifi's trust store is used.
        """
        if token is None:
            token = _config.get_token(base_url)
        trust_store_path = certifi.where() if trust_store_path is None else trust_store_path
        cfg = ServiceConfiguration(
            uris=[base_url],
            security=SslConfiguration(trust_store_path=trust_store_path),
            connect_timeout=connect_timeout,
        )
        agent = construct_user_agent_string()
        return cls(_clients=ClientsBunch.from_config(cfg, agent, token))

    def get_user(self) -> User:
        """Retrieve the user associated with this client."""
        return _get_user(self._clients.auth_header, self._clients.authentication)

    def create_run(
        self,
        name: str,
        start: datetime | IntegralNanosecondsUTC,
        end: datetime | IntegralNanosecondsUTC | None,
        description: str | None = None,
        *,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] = (),
        attachments: Iterable[Attachment] | Iterable[str] = (),
        asset: Asset | str | None = None,
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
            start_time=_SecondsNanos.from_flexible(start).to_scout_run_api(),
            title=name,
            end_time=None if end is None else _SecondsNanos.from_flexible(end).to_scout_run_api(),
            assets=[] if asset is None else [rid_from_instance_or_string(asset)],
        )
        response = self._clients.run.create_run(self._clients.auth_header, request)
        return Run._from_conjure(self._clients, response)

    def get_run(self, rid: str) -> Run:
        """Retrieve a run by its RID."""
        response = self._clients.run.get_run(self._clients.auth_header, rid)
        return Run._from_conjure(self._clients, response)

    def _search_runs_paginated(self, request: scout_run_api.SearchRunsRequest) -> Iterable[scout_run_api.Run]:
        while True:
            response = self._clients.run.search_runs(self._clients.auth_header, request)
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
        start: str | datetime | IntegralNanosecondsUTC | None = None,
        end: str | datetime | IntegralNanosecondsUTC | None = None,
        name_substring: str | None = None,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
    ) -> Iterable[Run]:
        request = scout_run_api.SearchRunsRequest(
            page_size=DEFAULT_PAGE_SIZE,
            query=_create_search_runs_query(start, end, name_substring, labels, properties),
            sort=scout_run_api.SortOptions(
                field=scout_run_api.SortField.START_TIME,
                is_descending=True,
            ),
        )
        for run in self._search_runs_paginated(request):
            yield Run._from_conjure(self._clients, run)

    @deprecate_keyword_argument("name_substring", "exact_name")
    def search_runs(
        self,
        start: str | datetime | IntegralNanosecondsUTC | None = None,
        end: str | datetime | IntegralNanosecondsUTC | None = None,
        name_substring: str | None = None,
        label: str | None = None,
        property: tuple[str, str] | None = None,
        *,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
    ) -> Sequence[Run]:
        """Search for runs meeting the specified filters.
        Filters are ANDed together, e.g. `(run.label == label) AND (run.end <= end)`

        Args:
            start: Inclusive start time for filtering runs.
            end: Inclusive end time for filtering runs.
            name_substring: Searches for a (case-insensitive) substring in the name.
            label: Deprecated, use labels instead.
            property: Deprecated, use properties instead.
            labels: A sequence of labels that must ALL be present on a run to be included.
            properties: A mapping of key-value pairs that must ALL be present on a run to be included.

        Returns:
            All runs which match all of the provided conditions
        """
        labels, properties = _handle_deprecated_labels_properties(
            "search_runs",
            label,
            labels,
            property,
            properties,
        )

        return list(self._iter_search_runs(start, end, name_substring, labels, properties))

    def create_dataset(
        self,
        name: str,
        *,
        description: str | None = None,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        prefix_tree_delimiter: str | None = None,
    ) -> Dataset:
        """Create an empty dataset.

        Args:
            name: Name of the dataset to create in Nominal.
            description: Human readable description of the dataset.
            labels: Text labels to apply to the created dataset
            properties: Key-value properties to apply to the cleated dataset
            prefix_tree_delimiter: If present, the delimiter to represent tiers when viewing channels hierarchically.

        Returns:
            Reference to the created dataset in Nominal.
        """
        request = scout_catalog.CreateDataset(
            name=name,
            description=description,
            labels=[*labels],
            properties={} if properties is None else {**properties},
            is_v2_dataset=True,
            metadata={},
            origin_metadata=scout_catalog.DatasetOriginMetadata(),
        )
        enriched_dataset = self._clients.catalog.create_dataset(self._clients.auth_header, request)
        dataset = Dataset._from_conjure(self._clients, enriched_dataset)

        if prefix_tree_delimiter:
            dataset.set_channel_prefix_tree(prefix_tree_delimiter)

        return dataset

    def create_csv_dataset(
        self,
        path: Path | str,
        name: str | None,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        description: str | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        prefix_tree_delimiter: str | None = None,
        channel_prefix: str | None = None,
    ) -> Dataset:
        """Create a dataset from a CSV file.

        If name is None, the name of the file will be used.

        See `create_dataset_from_io` for more details.
        """
        return self.create_tabular_dataset(
            path,
            name,
            timestamp_column,
            timestamp_type,
            description,
            labels=labels,
            properties=properties,
            prefix_tree_delimiter=prefix_tree_delimiter,
            channel_prefix=channel_prefix,
        )

    def create_ardupilot_dataflash_dataset(
        self,
        path: Path | str,
        name: str | None,
        description: str | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        prefix_tree_delimiter: str | None = None,
    ) -> Dataset:
        """Create a dataset from an ArduPilot DataFlash log file.

        If name is None, the name of the file will be used.

        See `create_dataset_from_io` for more details.
        """
        path = Path(path)
        file_type = FileTypes.DATAFLASH
        if name is None:
            name = path.name

        s3_path = upload_multipart_file(self._clients.auth_header, path, self._clients.upload, file_type)
        target = ingest_api.DatasetIngestTarget(
            new=ingest_api.NewDatasetIngestDestination(
                labels=list(labels),
                properties={} if properties is None else dict(properties),
                dataset_description=description,
                dataset_name=name,
                channel_config=_build_channel_config(prefix_tree_delimiter),
            )
        )
        request = _create_dataflash_ingest_request(s3_path, target)
        response = self._clients.ingest.ingest(self._clients.auth_header, request)
        if response.details.dataset is None:
            raise NominalIngestError("error ingesting dataflash: no dataset created")
        return self.get_dataset(response.details.dataset.dataset_rid)

    def create_tabular_dataset(
        self,
        path: Path | str,
        name: str | None,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        description: str | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        prefix_tree_delimiter: str | None = None,
        channel_prefix: str | None = None,
    ) -> Dataset:
        """Create a dataset from a table-like file (CSV, parquet, etc.).

        If name is None, the name of the file will be used.

        See `create_dataset_from_io` for more details.
        """
        path = Path(path)
        file_type = FileType.from_path_dataset(path)
        if name is None:
            name = path.name

        with path.open("rb") as data_file:
            return self.create_dataset_from_io(
                data_file,
                name=name,
                timestamp_column=timestamp_column,
                timestamp_type=timestamp_type,
                file_type=file_type,
                description=description,
                labels=labels,
                properties=properties,
                prefix_tree_delimiter=prefix_tree_delimiter,
                channel_prefix=channel_prefix,
            )

    def create_journal_json_dataset(
        self,
        path: Path | str,
        name: str | None,
        description: str | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        prefix_tree_delimiter: str | None = None,
    ) -> Dataset:
        """Create a dataset from a journal log file with json output format.

        Intended to be used with the recorded output of `journalctl --output json ...`.
        The path extension is expected to be `.jsonl` or `.jsonl.gz` if gzipped.

        If name is None, the name of the file will be used.

        See `create_dataset_from_io` for more details.
        """
        path = Path(path)
        file_type = FileType.from_path_journal_json(path)

        if name is None:
            name = path.name

        s3_path = upload_multipart_file(self._clients.auth_header, path, self._clients.upload, file_type)
        request = ingest_api.IngestRequest(
            options=ingest_api.IngestOptions(
                journal_json=ingest_api.JournalJsonOpts(
                    source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path)),
                    target=ingest_api.DatasetIngestTarget(
                        new=ingest_api.NewDatasetIngestDestination(
                            labels=list(labels),
                            properties={} if properties is None else dict(properties),
                            dataset_description=description,
                            dataset_name=name,
                            channel_config=_build_channel_config(prefix_tree_delimiter),
                        )
                    ),
                )
            ),
        )

        response = self._clients.ingest.ingest(self._clients.auth_header, request)
        if response.details.dataset is None:
            raise NominalIngestError("error ingesting journal json: no dataset created")
        return self.get_dataset(response.details.dataset.dataset_rid)

    def create_dataset_from_io(
        self,
        dataset: BinaryIO,
        name: str,
        timestamp_column: str,
        timestamp_type: _AnyTimestampType,
        file_type: tuple[str, str] | FileType = FileTypes.CSV,
        description: str | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        prefix_tree_delimiter: str | None = None,
        channel_prefix: str | None = None,
        file_name: str | None = None,
    ) -> Dataset:
        """Create a dataset from a file-like object.
        The dataset must be a file-like object in binary mode, e.g. open(path, "rb") or io.BytesIO.
        If the file is not in binary-mode, the requests library blocks indefinitely.

        Timestamp column types must be a `CustomTimestampFormat` or one of the following literals:
            "iso_8601": ISO 8601 formatted strings,
            "epoch_{unit}": epoch timestamps in UTC (floats or ints),
            "relative_{unit}": relative timestamps (floats or ints),
            where {unit} is one of: nanoseconds | microseconds | milliseconds | seconds | minutes | hours | days

        Args:
            dataset: Binary file-like tabular data stream
            name: Name of the dataset to create
            timestamp_column: Column of data containing timestamp information for all other columns
            timestamp_type: Type of timestamps contained within timestamp_column
            file_type: Type of file being ingested (e.g. CSV, parquet, etc.). Used for naming the file uploaded
                to cloud storage as part of ingestion.
            description: Human-readable description of the dataset to create
            labels: Text labels to apply to the created dataset
            properties: Key-value properties to apply to the cleated dataset
            prefix_tree_delimiter: If present, the delimiter to represent tiers when viewing channels hierarchically.
            channel_prefix: Prefix to apply to newly created channels
            file_name: Name of the file (without extension) to create when uploading.

        Returns:
            Reference to the constructed dataset object.
        """
        if isinstance(dataset, TextIOBase):
            raise TypeError(f"dataset {dataset} must be open in binary mode, rather than text mode")

        file_type = FileType(*file_type)

        # Prevent breaking changes from customers using create_dataset_from_io directly
        if file_name is None:
            file_name = name

        s3_path = upload_multipart_io(self._clients.auth_header, dataset, file_name, file_type, self._clients.upload)
        request = ingest_api.IngestRequest(
            options=ingest_api.IngestOptions(
                csv=ingest_api.CsvOpts(
                    source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path)),
                    target=ingest_api.DatasetIngestTarget(
                        new=ingest_api.NewDatasetIngestDestination(
                            labels=list(labels),
                            properties={} if properties is None else dict(properties),
                            channel_config=_build_channel_config(prefix_tree_delimiter),
                            dataset_description=description,
                            dataset_name=name,
                        )
                    ),
                    timestamp_metadata=ingest_api.TimestampMetadata(
                        series_name=timestamp_column,
                        timestamp_type=_to_typed_timestamp_type(timestamp_type)._to_conjure_ingest_api(),
                    ),
                    additional_file_tags=None,
                    channel_prefix=channel_prefix,
                    tag_keys_from_columns=None,
                )
            )
        )
        response = self._clients.ingest.ingest(self._clients.auth_header, request)
        if not response.details.dataset:
            raise NominalIngestError("error ingesting dataset: no dataset created")
        return self.get_dataset(response.details.dataset.dataset_rid)

    def create_mcap_dataset(
        self,
        path: Path | str,
        name: str | None,
        description: str | None = None,
        include_topics: Iterable[str] | None = None,
        exclude_topics: Iterable[str] | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        prefix_tree_delimiter: str | None = None,
    ) -> Dataset:
        """Create a dataset from an MCAP file.

        If name is None, the name of the file will be used.

        See `create_dataset_from_mcap_io` for more details on the other arguments.
        """
        mcap_path = Path(path)
        if name is None:
            name = mcap_path.name

        with mcap_path.open("rb") as mcap_file:
            return self.create_dataset_from_mcap_io(
                mcap_file,
                name=name,
                description=description,
                include_topics=include_topics,
                exclude_topics=exclude_topics,
                labels=labels,
                properties=properties,
                prefix_tree_delimiter=prefix_tree_delimiter,
                file_name=path_upload_name(mcap_path, FileTypes.MCAP),
            )

    def create_dataset_from_mcap_io(
        self,
        dataset: BinaryIO,
        name: str,
        description: str | None = None,
        include_topics: Iterable[str] | None = None,
        exclude_topics: Iterable[str] | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        prefix_tree_delimiter: str | None = None,
        file_name: str | None = None,
    ) -> Dataset:
        """Create a dataset from an mcap file-like object.

        The dataset must be a file-like object in binary mode, e.g. open(path, "rb") or io.BytesIO.
        If the file is not in binary-mode, the requests library blocks indefinitely.

        Args:
            dataset: Binary file-like MCAP stream
            name: Name of the dataset to create
            description: Human-readable description of the dataset to create
            include_topics: If present, list of topics to restrict ingestion to.
                If not present, defaults to all protobuf-encoded topics present in the MCAP.
            exclude_topics: If present, list of topics to not ingest from the MCAP.
            labels: Text labels to apply to the created dataset
            properties: Key-value properties to apply to the cleated dataset
            prefix_tree_delimiter: If present, the delimiter to represent tiers when viewing channels hierarchically.
            file_name: If present, name (without extension) to use when uploading file. Otherwise, defaults to name.

        Returns:
            Reference to the constructed dataset object.
        """
        if isinstance(dataset, TextIOBase):
            raise TypeError(f"dataset {dataset} must be open in binary mode, rather than text mode")

        if file_name is None:
            file_name = name

        s3_path = upload_multipart_io(
            self._clients.auth_header,
            dataset,
            file_name,
            file_type=FileTypes.MCAP,
            upload_client=self._clients.upload,
        )
        channels = _create_mcap_channels(include_topics, exclude_topics)
        target = ingest_api.DatasetIngestTarget(
            new=ingest_api.NewDatasetIngestDestination(
                dataset_name=name,
                dataset_description=description,
                properties={} if properties is None else dict(properties),
                labels=list(labels),
                channel_config=_build_channel_config(prefix_tree_delimiter),
            )
        )
        request = _create_mcap_ingest_request(s3_path, channels, target)
        resp = self._clients.ingest.ingest(self._clients.auth_header, request)
        if resp.details.dataset is not None:
            dataset_rid = resp.details.dataset.dataset_rid
            if dataset_rid is not None:
                return self.get_dataset(dataset_rid)
            raise NominalIngestError("error ingesting mcap: no dataset rid")
        raise NominalIngestError("error ingesting mcap: no dataset created")

    def create_video(
        self,
        path: Path | str,
        name: str | None = None,
        start: datetime | IntegralNanosecondsUTC | None = None,
        frame_timestamps: Sequence[IntegralNanosecondsUTC] | None = None,
        description: str | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
    ) -> Video:
        """Create a video from an h264/h265 encoded video file (mp4, mkv, ts, etc.).

        If name is None, the name of the file will be used.

        See `create_video_from_io` for more details.
        """
        path = Path(path)
        file_type = FileType.from_video(path)
        if name is None:
            name = path.name

        with path.open("rb") as data_file:
            return self.create_video_from_io(
                data_file,
                name=name,
                start=start,
                frame_timestamps=frame_timestamps,
                file_type=file_type,
                description=description,
                labels=labels,
                properties=properties,
                file_name=path_upload_name(path, file_type),
            )

    def create_video_from_io(
        self,
        video: BinaryIO,
        name: str,
        start: datetime | IntegralNanosecondsUTC | None = None,
        frame_timestamps: Sequence[IntegralNanosecondsUTC] | None = None,
        description: str | None = None,
        file_type: tuple[str, str] | FileType = FileTypes.MP4,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        file_name: str | None = None,
    ) -> Video:
        """Create a video from a file-like object.
        The video must be a file-like object in binary mode, e.g. open(path, "rb") or io.BytesIO.

        Args:
        ----
            video: file-like object to read video data from
            name: Name of the video to create in Nominal
            start: Starting timestamp of the video
            frame_timestamps: Per-frame timestamps (in nanoseconds since unix epoch) for every frame of the video
            description: Description of the video to create in nominal
            file_type: Type of data being uploaded, used for naming the file uploaded to cloud storage as part
                of ingestion.
            labels: Labels to apply to the video in nominal
            properties: Properties to apply to the video in nominal
            file_name: Name (without extension) to use when uploading the video file. Defaults to video name.

        Returns:
        -------
            Handle to the created video

        Note:
        ----
            Exactly one of 'start' and 'frame_timestamps' **must** be provided. Most users will
            want to provide a starting timestamp: frame_timestamps is primarily useful when the scale
            of the video data is not 1:1 with the playback speed or non-uniform over the course of the video,
            for example, 200fps video artificially slowed to 30 fps without dropping frames. This will result
            in the playhead on charts within the product playing at the rate of the underlying data rather than
            time elapsed in the video playback.

        """
        if isinstance(video, TextIOBase):
            raise TypeError(f"video {video} must be open in binary mode, rather than text mode")

        timestamp_manifest = _build_video_file_timestamp_manifest(
            self._clients.auth_header, self._clients.upload, start, frame_timestamps
        )

        if file_name is None:
            file_name = name

        file_type = FileType(*file_type)
        s3_path = upload_multipart_io(self._clients.auth_header, video, file_name, file_type, self._clients.upload)
        request = ingest_api.IngestRequest(
            ingest_api.IngestOptions(
                video=ingest_api.VideoOpts(
                    source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(s3_path)),
                    target=ingest_api.VideoIngestTarget(
                        new=ingest_api.NewVideoIngestDestination(
                            title=name,
                            description=description,
                            properties={} if properties is None else dict(properties),
                            labels=list(labels),
                        )
                    ),
                    timestamp_manifest=timestamp_manifest,
                )
            )
        )
        response = self._clients.ingest.ingest(self._clients.auth_header, request)
        if response.details.video is None:
            raise NominalIngestError("error ingesting video: no video created")
        return self.get_video(response.details.video.video_rid)

    def create_log_set(
        self,
        name: str,
        logs: Iterable[Log] | Iterable[tuple[datetime | IntegralNanosecondsUTC, str]],
        timestamp_type: LogTimestampType = "absolute",
        description: str | None = None,
    ) -> LogSet:
        """Create an immutable log set with the given logs.

        The logs are attached during creation and cannot be modified afterwards. Logs can either be of type `Log`
        or a tuple of a timestamp and a string. Timestamp type must be either 'absolute' or 'relative'.
        """
        request = datasource_logset_api.CreateLogSetRequest(
            name=name,
            description=description,
            origin_metadata={},
            timestamp_type=_log_timestamp_type_to_conjure(timestamp_type),
        )
        response = self._clients.logset.create(self._clients.auth_header, request)
        return self._attach_logs_and_finalize(response.rid, _logs_to_conjure(logs))

    def _attach_logs_and_finalize(self, rid: str, logs: Iterable[datasource_logset_api.Log]) -> LogSet:
        request = datasource_logset_api.AttachLogsAndFinalizeRequest(logs=list(logs))
        response = self._clients.logset.attach_logs_and_finalize(
            auth_header=self._clients.auth_header, log_set_rid=rid, request=request
        )
        return LogSet._from_conjure(self._clients, response)

    def get_video(self, rid: str) -> Video:
        """Retrieve a video by its RID."""
        response = self._clients.video.get(self._clients.auth_header, rid)
        return Video._from_conjure(self._clients, response)

    def _iter_get_videos(self, rids: Iterable[str]) -> Iterable[Video]:
        request = scout_video_api.GetVideosRequest(video_rids=list(rids))
        for response in self._clients.video.batch_get(self._clients.auth_header, request).responses:
            yield Video._from_conjure(self._clients, response)

    def get_videos(self, rids: Iterable[str]) -> Sequence[Video]:
        """Retrieve videos by their RID."""
        return list(self._iter_get_videos(rids))

    def get_dataset(self, rid: str) -> Dataset:
        """Retrieve a dataset by its RID."""
        response = _get_dataset(self._clients.auth_header, self._clients.catalog, rid)
        return Dataset._from_conjure(self._clients, response)

    def get_log_set(self, log_set_rid: str) -> LogSet:
        """Retrieve a log set along with its metadata given its RID."""
        response = _get_log_set(self._clients, log_set_rid)
        return LogSet._from_conjure(self._clients, response)

    def _iter_get_datasets(self, rids: Iterable[str]) -> Iterable[Dataset]:
        for ds in _get_datasets(self._clients.auth_header, self._clients.catalog, rids):
            yield Dataset._from_conjure(self._clients, ds)

    def get_datasets(self, rids: Iterable[str]) -> Sequence[Dataset]:
        """Retrieve datasets by their RIDs."""
        return list(self._iter_get_datasets(rids))

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
        response = self._clients.catalog.search_datasets(self._clients.auth_header, request)
        for ds in response.results:
            yield Dataset._from_conjure(self._clients, ds)

    def get_checklist(self, rid: str) -> Checklist:
        response = self._clients.checklist.get(self._clients.auth_header, rid)
        return Checklist._from_conjure(self._clients, response)

    def search_checklists(
        self,
        search_text: str | None = None,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
    ) -> Sequence[Checklist]:
        """Search for checklists meeting the specified filters.
        Filters are ANDed together, e.g. `(checklist.label == label) AND (checklist.search_text =~ field)`

        Args:
            search_text: case-insensitive search for any of the keywords in all string fields
            labels: A sequence of labels that must ALL be present on a checklist to be included.
            properties: A mapping of key-value pairs that must ALL be present on a checklist to be included.

        Returns:
            All checklists which match all of the provided conditions
        """
        page_token = None
        query = _create_search_checklists_query(search_text, labels, properties)
        archived_statuses = [api.ArchivedStatus.NOT_ARCHIVED]

        raw_checklists = []
        while True:
            request = scout_checks_api.SearchChecklistsRequest(
                query=query,
                archived_statuses=archived_statuses,
                next_page_token=page_token,
                page_size=DEFAULT_PAGE_SIZE,
            )
            response = self._clients.checklist.search(self._clients.auth_header, request)
            raw_checklists.extend(response.values)
            page_token = response.next_page_token
            if not page_token:
                break

        return [Checklist._from_conjure(self._clients, checklist) for checklist in raw_checklists]

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
        s3_path = upload_multipart_io(
            self._clients.auth_header,
            attachment,
            name,
            file_type,
            self._clients.upload,
        )
        request = attachments_api.CreateAttachmentRequest(
            description=description or "",
            labels=list(labels),
            properties={} if properties is None else dict(properties),
            s3_path=s3_path,
            title=name,
        )
        response = self._clients.attachment.create(self._clients.auth_header, request)
        return Attachment._from_conjure(self._clients, response)

    def get_attachment(self, rid: str) -> Attachment:
        """Retrieve an attachment by its RID."""
        response = self._clients.attachment.get(self._clients.auth_header, rid)
        return Attachment._from_conjure(self._clients, response)

    def get_attachments(self, rids: Iterable[str]) -> Sequence[Attachment]:
        """Retrive attachments by their RIDs."""
        return [
            Attachment._from_conjure(self._clients, a)
            for a in _iter_get_attachments(self._clients.auth_header, self._clients.attachment, rids)
        ]

    def get_all_units(self) -> Sequence[Unit]:
        """Retrieve list of metadata for all supported units within Nominal"""
        return _available_units(self._clients.auth_header, self._clients.units)

    def get_unit(self, unit_symbol: str) -> Unit | None:
        """Get details of the given unit symbol, or none if invalid
        Args:
            unit_symbol: Symbol of the unit to get metadata for.
                NOTE: This currently requires that units are formatted as laid out in
                      the latest UCUM standards (see https://ucum.org/ucum)

        Returns:
        -------
            Rendered Unit metadata if the symbol is valid and supported by Nominal, or None
            if no such unit symbol matches.

        """
        api_unit = self._clients.units.get_unit(self._clients.auth_header, unit_symbol)
        return None if api_unit is None else Unit._from_conjure(api_unit)

    def get_commensurable_units(self, unit_symbol: str) -> Sequence[Unit]:
        """Get the list of units that are commensurable (convertible to/from) the given unit symbol."""
        return [
            Unit._from_conjure(unit)
            for unit in self._clients.units.get_commensurable_units(self._clients.auth_header, unit_symbol)
        ]

    def get_channel(self, rid: str) -> Channel:
        """Get metadata for a given channel by looking up its rid
        Args:
            rid: Identifier for the channel to look up
        Returns:
            Resolved metadata for the requested channel
        Raises:
            conjure_python_client.ConjureHTTPError: An error occurred while looking up the channel.
                This typically occurs when there is no such channel for the given RID.
        """
        warnings.warn(
            "get_channel is deprecated. Use dataset.get_channel() or connection.get_channel() instead.",
            UserWarning,
        )
        return Channel._from_conjure_logicalseries_api(
            self._clients, self._clients.logical_series.get_logical_series(self._clients.auth_header, rid)
        )

    def set_channel_units(self, rids_to_types: Mapping[str, str | None]) -> Iterable[Channel]:
        """Sets the units for a set of channels based on user-provided unit symbols
        Args:
            rids_to_types: Mapping of channel RIDs -> unit symbols (e.g. 'm/s').
                NOTE: Providing `None` as the unit symbol clears any existing units for the channels.

        Returns:
        -------
            A sequence of metadata for all updated channels
        Raises:
            conjure_python_client.ConjureHTTPError: An error occurred while setting metadata on the channel.
                This typically occurs when either the units are invalid, or there are no
                channels with the given RIDs present.

        """
        warnings.warn(
            "set_channel_units is deprecated. Use dataset.set_channel_units() or connection.set_channel_units()",
            UserWarning,
        )

        series_updates = []
        for rid, series_type in rids_to_types.items():
            series_updates.append(
                timeseries_logicalseries_api.UpdateLogicalSeries(
                    logical_series_rid=rid,
                    unit_update=_build_unit_update(series_type),
                )
            )

        request = timeseries_logicalseries_api.BatchUpdateLogicalSeriesRequest(series_updates)
        response = self._clients.logical_series.batch_update_logical_series(self._clients.auth_header, request)
        return [Channel._from_conjure_logicalseries_api(self._clients, resp) for resp in response.responses]

    def get_connection(self, rid: str) -> Connection:
        """Retrieve a connection by its RID."""
        response = self._clients.connection.get_connection(self._clients.auth_header, rid)
        return Connection._from_conjure(self._clients, response)

    def create_video_from_mcap(
        self,
        path: Path | str,
        topic: str,
        name: str | None = None,
        description: str | None = None,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
    ) -> Video:
        """Create a video from an MCAP file containing H264 or H265 video data.

        If name is None, the name of the file will be used.

        See `create_video_from_mcap_io` for more details.
        """
        path = Path(path)
        if name is None:
            name = path.name

        with path.open("rb") as data_file:
            return self.create_video_from_mcap_io(
                data_file,
                name=name,
                topic=topic,
                file_type=FileTypes.MCAP,
                description=description,
                labels=labels,
                properties=properties,
                file_name=path_upload_name(path, FileTypes.MCAP),
            )

    def create_video_from_mcap_io(
        self,
        mcap: BinaryIO,
        topic: str,
        name: str,
        description: str | None = None,
        file_type: tuple[str, str] | FileType = FileTypes.MCAP,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        file_name: str | None = None,
    ) -> Video:
        """Create video from topic in a mcap file.

        Mcap must be a file-like object in binary mode, e.g. open(path, "rb") or io.BytesIO.

        If name is None, the name of the file will be used.
        """
        if isinstance(mcap, TextIOBase):
            raise TypeError(f"dataset {mcap} must be open in binary mode, rather than text mode")

        if file_name is None:
            file_name = name

        file_type = FileType(*file_type)
        s3_path = upload_multipart_io(self._clients.auth_header, mcap, file_name, file_type, self._clients.upload)
        request = ingest_api.IngestRequest(
            options=ingest_api.IngestOptions(
                video=ingest_api.VideoOpts(
                    source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(s3_path)),
                    target=ingest_api.VideoIngestTarget(
                        new=ingest_api.NewVideoIngestDestination(
                            title=name,
                            description=description,
                            properties={} if properties is None else dict(properties),
                            labels=list(labels),
                        )
                    ),
                    timestamp_manifest=scout_video_api.VideoFileTimestampManifest(
                        mcap=scout_video_api.McapTimestampManifest(api.McapChannelLocator(topic=topic))
                    ),
                )
            )
        )
        response = self._clients.ingest.ingest(self._clients.auth_header, request)
        if response.details.video is None:
            raise NominalIngestError("error ingesting mcap video: no video created")
        return self.get_video(response.details.video.video_rid)

    def create_streaming_connection(
        self,
        datasource_id: str,
        connection_name: str,
        datasource_description: str | None = None,
        *,
        required_tag_names: list[str] | None = None,
    ) -> StreamingConnection:
        datasource_response = self._clients.storage.create(
            self._clients.auth_header,
            storage_datasource_api.CreateNominalDataSourceRequest(
                id=datasource_id,
                description=datasource_description,
            ),
        )
        connection_response = self._clients.connection.create_connection(
            self._clients.auth_header,
            scout_datasource_connection_api.CreateConnection(
                name=connection_name,
                connection_details=scout_datasource_connection_api.ConnectionDetails(
                    nominal=scout_datasource_connection_api.NominalConnectionDetails(
                        nominal_data_source_rid=datasource_response.rid
                    ),
                ),
                metadata={},
                scraping=scout_datasource_connection_api.ScrapingConfig(
                    nominal=scout_datasource_connection_api.NominalScrapingConfig(
                        channel_name_components=[
                            scout_datasource_connection_api.NominalChannelNameComponent(channel=api.Empty())
                        ],
                        separator=".",
                    )
                ),
                required_tag_names=required_tag_names or [],
                available_tag_values={},
                should_scrape=True,
            ),
        )
        conn = Connection._from_conjure(self._clients, connection_response)
        if isinstance(conn, StreamingConnection):
            return conn
        raise NominalError(f"Expected StreamingConnection but got {type(conn).__name__}")

    def create_workbook_from_template(
        self,
        template_rid: str,
        run_rid: str,
        title: str | None = None,
        description: str | None = None,
        is_draft: bool = False,
    ) -> Workbook:
        template = self._clients.template.get(self._clients.auth_header, template_rid)

        notebook = self._clients.notebook.create(
            self._clients.auth_header,
            scout_notebook_api.CreateNotebookRequest(
                title=title if title is not None else f"Workbook from {template.metadata.title}",
                description=description or "",
                notebook_type=None,
                is_draft=is_draft,
                state_as_json="{}",
                charts=None,
                run_rid=run_rid,
                data_scope=None,
                layout=template.layout,
                content=template.content,
                content_v2=None,
                check_alert_refs=[],
                event_refs=[],
            ),
        )

        return Workbook._from_conjure(self._clients, notebook)

    def create_asset(
        self,
        name: str,
        description: str | None = None,
        *,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] = (),
    ) -> Asset:
        """Create an asset."""
        request = scout_asset_api.CreateAssetRequest(
            description=description,
            labels=list(labels),
            properties={} if properties is None else dict(properties),
            title=name,
            attachments=[],
            data_scopes=[],
            links=[],
        )
        response = self._clients.assets.create_asset(self._clients.auth_header, request)
        return Asset._from_conjure(self._clients, response)

    def get_asset(self, rid: str) -> Asset:
        """Retrieve an asset by its RID."""
        response = self._clients.assets.get_assets(self._clients.auth_header, [rid])
        if len(response) == 0 or rid not in response:
            raise ValueError(f"no asset found with RID {rid!r}: {response!r}")
        if len(response) > 1:
            raise ValueError(f"multiple assets found with RID {rid!r}: {response!r}")
        return Asset._from_conjure(self._clients, response[rid])

    def _search_assets_paginated(self, request: scout_asset_api.SearchAssetsRequest) -> Iterable[scout_asset_api.Asset]:
        while True:
            response = self._clients.assets.search_assets(self._clients.auth_header, request)
            yield from response.results
            if response.next_page_token is None:
                break
            request = scout_asset_api.SearchAssetsRequest(
                page_size=request.page_size,
                query=request.query,
                sort=request.sort,
                next_page_token=response.next_page_token,
            )

    def _iter_search_assets(
        self,
        search_text: str | None = None,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
    ) -> Iterable[Asset]:
        request = scout_asset_api.SearchAssetsRequest(
            page_size=DEFAULT_PAGE_SIZE,
            query=_create_search_assets_query(search_text, labels, properties),
            sort=scout_asset_api.SortOptions(
                field=scout_asset_api.SortField.CREATED_AT,
                is_descending=True,
            ),
        )
        for asset in self._search_assets_paginated(request):
            yield Asset._from_conjure(self._clients, asset)

    def search_assets(
        self,
        search_text: str | None = None,
        label: str | None = None,
        property: tuple[str, str] | None = None,
        *,
        labels: Sequence[str] | None = None,
        properties: Mapping[str, str] | None = None,
    ) -> Sequence[Asset]:
        """Search for assets meeting the specified filters.
        Filters are ANDed together, e.g. `(asset.label == label) AND (asset.search_text =~ field)`

        Args:
            search_text: case-insensitive search for any of the keywords in all string fields
            label: Deprecated, use labels instead.
            property: Deprecated, use properties instead.
            labels: A sequence of labels that must ALL be present on a asset to be included.
            properties: A mapping of key-value pairs that must ALL be present on a asset to be included.

        Returns:
            All assets which match all of the provided conditions
        """
        labels, properties = _handle_deprecated_labels_properties(
            "search_assets",
            label,
            labels,
            property,
            properties,
        )

        return list(self._iter_search_assets(search_text, labels, properties))

    def list_streaming_checklists(self, asset: Asset | str | None = None) -> Iterable[str]:
        """List all Streaming Checklists.

        Args:
            asset: if provided, only return checklists associated with the given asset.
        """
        next_page_token = None

        while True:
            if asset is None:
                response = self._clients.checklist_execution.list_streaming_checklist(
                    self._clients.auth_header,
                    scout_checklistexecution_api.ListStreamingChecklistRequest(page_token=next_page_token),
                )
                yield from response.checklists
                next_page_token = response.next_page_token
            else:
                for_asset_response = self._clients.checklist_execution.list_streaming_checklist_for_asset(
                    self._clients.auth_header,
                    scout_checklistexecution_api.ListStreamingChecklistForAssetRequest(
                        asset_rid=rid_from_instance_or_string(asset), page_token=next_page_token
                    ),
                )
                yield from for_asset_response.checklists
                next_page_token = for_asset_response.next_page_token

            if next_page_token is None:
                break

    def data_review_builder(self) -> DataReviewBuilder:
        return DataReviewBuilder([], [], self._clients)

    def get_data_review(self, rid: str) -> DataReview:
        response = self._clients.datareview.get(self._clients.auth_header, rid)
        return DataReview._from_conjure(self._clients, response)

    def search_data_reviews(
        self,
        assets: Sequence[Asset | str] | None = None,
        runs: Sequence[Run | str] | None = None,
    ) -> Sequence[DataReview]:
        """Search for any data reviews present within a collection of runs and assets."""
        page_token = None
        raw_data_reviews = []
        while True:
            # TODO (drake-nominal): Expose checklist_refs to users
            request = scout_datareview_api.FindDataReviewsRequest(
                asset_rids=[rid_from_instance_or_string(asset) for asset in assets] if assets else [],
                checklist_refs=[],
                run_rids=[rid_from_instance_or_string(run) for run in runs] if runs else [],
                archived_statuses=[api.ArchivedStatus.NOT_ARCHIVED],
                next_page_token=page_token,
                page_size=DEFAULT_PAGE_SIZE,
            )
            response = self._clients.datareview.find_data_reviews(self._clients.auth_header, request)
            raw_data_reviews.extend(response.data_reviews)
            page_token = response.next_page_token

            if page_token is None:
                break

        return [DataReview._from_conjure(self._clients, data_review) for data_review in raw_data_reviews]


def _build_channel_config(prefix_tree_delimiter: str | None) -> ingest_api.ChannelConfig | None:
    if prefix_tree_delimiter is None:
        return None
    else:
        return ingest_api.ChannelConfig(prefix_tree_delimiter=prefix_tree_delimiter)


def _create_search_runs_query(
    start: str | datetime | IntegralNanosecondsUTC | None = None,
    end: str | datetime | IntegralNanosecondsUTC | None = None,
    name_substring: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
) -> scout_run_api.SearchQuery:
    queries = []
    if start is not None:
        start_time = _SecondsNanos.from_flexible(start).to_scout_run_api()
        queries.append(scout_run_api.SearchQuery(start_time_inclusive=start_time))

    if end is not None:
        end_time = _SecondsNanos.from_flexible(end).to_scout_run_api()
        queries.append(scout_run_api.SearchQuery(end_time_inclusive=end_time))

    if name_substring is not None:
        queries.append(scout_run_api.SearchQuery(exact_match=name_substring))

    if labels:
        for label in labels:
            queries.append(scout_run_api.SearchQuery(label=label))

    if properties:
        for name, value in properties.items():
            queries.append(scout_run_api.SearchQuery(property=api.Property(name=name, value=value)))

    return scout_run_api.SearchQuery(and_=queries)


def _log_timestamp_type_to_conjure(log_timestamp_type: LogTimestampType) -> datasource.TimestampType:
    if log_timestamp_type == "absolute":
        return datasource.TimestampType.ABSOLUTE
    elif log_timestamp_type == "relative":
        return datasource.TimestampType.RELATIVE
    raise ValueError(f"timestamp type {log_timestamp_type} must be 'relative' or 'absolute'")


def _logs_to_conjure(
    logs: Iterable[Log] | Iterable[tuple[datetime | IntegralNanosecondsUTC, str]],
) -> Iterable[datasource_logset_api.Log]:
    for log in logs:
        if isinstance(log, Log):
            yield log._to_conjure()
        elif isinstance(log, tuple):
            ts, body = log
            yield Log(timestamp=_SecondsNanos.from_flexible(ts).to_nanoseconds(), body=body)._to_conjure()


def _create_search_assets_query(
    search_text: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
) -> scout_asset_api.SearchAssetsQuery:
    queries = []
    if search_text is not None:
        queries.append(scout_asset_api.SearchAssetsQuery(search_text=search_text))

    if labels is not None:
        for label in labels:
            queries.append(scout_asset_api.SearchAssetsQuery(label=label))

    if properties:
        for name, value in properties.items():
            queries.append(scout_asset_api.SearchAssetsQuery(property=api.Property(name=name, value=value)))

    return scout_asset_api.SearchAssetsQuery(and_=queries)


def _create_search_checklists_query(
    search_text: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
) -> scout_checks_api.ChecklistSearchQuery:
    queries = []
    if search_text is not None:
        queries.append(scout_checks_api.ChecklistSearchQuery(search_text=search_text))

    if labels is not None:
        for label in labels:
            queries.append(scout_checks_api.ChecklistSearchQuery(label=label))

    if properties is not None:
        for prop_key, prop_value in properties.items():
            queries.append(scout_checks_api.ChecklistSearchQuery(property=api.Property(prop_key, prop_value)))

    return scout_checks_api.ChecklistSearchQuery(and_=queries)


def _handle_deprecated_labels_properties(
    function_name: str,
    label: str | None,
    labels: Sequence[str] | None,
    property: tuple[str, str] | None,
    properties: Mapping[str, str] | None,
) -> tuple[Sequence[str], Mapping[str, str]]:
    if all([label, labels]):
        raise ValueError(f"Cannot use both label and labels for {function_name}.")
    elif label:
        warnings.warn(
            f"parameter 'label' of {function_name} is deprecated, use 'labels' instead",
            UserWarning,
            stacklevel=2,
        )
        labels = [label]
    elif labels is None:
        labels = []

    if all([property, properties]):
        raise ValueError(f"Cannot use both property and propertiess for {function_name}.")
    elif property:
        warnings.warn(
            f"parameter 'property' of {function_name} is deprecated, use 'properties' instead",
            UserWarning,
            stacklevel=2,
        )
        properties = {property[0]: property[1]}
    elif properties is None:
        properties = {}

    return labels, properties
