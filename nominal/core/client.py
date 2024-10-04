from __future__ import annotations

import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime
from io import TextIOBase
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, Iterable, Mapping, Sequence

import certifi
from conjure_python_client import RequestsClient, ServiceConfiguration, SslConfiguration
from typing_extensions import Self

from nominal import _config

from .attachment import Attachment
from .dataset import Dataset
from .log import Log, LogSet
from .run import Run
from .video import Video

from .._api.combined import (
    attachments_api,
    authentication_api,
    datasource,
    datasource_logset,
    datasource_logset_api,
    ingest_api,
    scout,
    scout_catalog,
    scout_checks_api,
    scout_compute_representation_api,
    scout_run_api,
    scout_units_api,
    scout_video,
    scout_video_api,
    upload_api,
)
from .._multipart import put_multipart_upload
from .._utils import (
    FileType,
    FileTypes,
    construct_user_agent_string,
    deprecate_keyword_argument,
)
from ..ts import IntegralNanosecondsUTC, LogTimestampType, _AnyTimestampType, _SecondsNanos, _to_typed_timestamp_type
from .checklist import Checklist, ChecklistBuilder


@dataclass(frozen=True)
class NominalClient:
    _auth_header: str = field(repr=False)
    _run_client: scout.RunService = field(repr=False)
    _upload_client: upload_api.UploadService = field(repr=False)
    _authentication_client: authentication_api.AuthenticationServiceV2 = field(repr=False)
    _ingest_client: ingest_api.IngestService = field(repr=False)
    _catalog_client: scout_catalog.CatalogService = field(repr=False)
    _attachment_client: attachments_api.AttachmentService = field(repr=False)
    _compute_representation_client: scout_compute_representation_api.ComputeRepresentationService = field(repr=False)
    _checklist_api_client: scout_checks_api.ChecklistService = field(repr=False)
    _video_client: scout_video.VideoService = field(repr=False)
    _logset_client: datasource_logset.LogSetService = field(repr=False)
    _units_client: scout_units_api.UnitsService = field(repr=False)

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
        run_client = RequestsClient.create(scout.RunService, agent, cfg)
        upload_client = RequestsClient.create(upload_api.UploadService, agent, cfg)
        ingest_client = RequestsClient.create(ingest_api.IngestService, agent, cfg)
        catalog_client = RequestsClient.create(scout_catalog.CatalogService, agent, cfg)
        attachment_client = RequestsClient.create(attachments_api.AttachmentService, agent, cfg)
        compute_representation_client = RequestsClient.create(
            scout_compute_representation_api.ComputeRepresentationService, agent, cfg
        )
        checklist_api_client = RequestsClient.create(scout_checks_api.ChecklistService, agent, cfg)
        authentication_client = RequestsClient.create(authentication_api.AuthenticationServiceV2, agent, cfg)
        video_client = RequestsClient.create(scout_video.VideoService, agent, cfg)
        logset_client = RequestsClient.create(datasource_logset.LogSetService, agent, cfg)
        authentication_client = RequestsClient.create(authentication_api.AuthenticationServiceV2, agent, cfg)
        unit_client = RequestsClient.create(scout_units_api.UnitsService, agent, cfg)
        auth_header = f"Bearer {token}"
        return cls(
            _auth_header=auth_header,
            _run_client=run_client,
            _upload_client=upload_client,
            _ingest_client=ingest_client,
            _catalog_client=catalog_client,
            _attachment_client=attachment_client,
            _compute_representation_client=compute_representation_client,
            _checklist_api_client=checklist_api_client,
            _authentication_client=authentication_client,
            _video_client=video_client,
            _logset_client=logset_client,
            _units_client=unit_client,
        )

    def get_user(self) -> User:
        """Retrieve the user associated with this client."""
        response = self._authentication_client.get_my_profile(self._auth_header)
        return User(rid=response.rid, display_name=response.display_name, email=response.email)

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
            attachments=[_rid_from_instance_or_string(a) for a in attachments],
            data_sources={},
            description=description or "",
            labels=list(labels),
            links=[],
            properties={} if properties is None else dict(properties),
            start_time=_SecondsNanos.from_flexible(start).to_scout_run_api(),
            title=name,
            end_time=_SecondsNanos.from_flexible(end).to_scout_run_api(),
        )
        response = self._run_client.create_run(self._auth_header, request)
        return Run._from_conjure(self, response)

    def get_run(self, rid: str) -> Run:
        """Retrieve a run by its RID."""
        response = self._run_client.get_run(self._auth_header, rid)
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
        name_substring: str | None = None,
        label: str | None = None,
        property: tuple[str, str] | None = None,
    ) -> Iterable[Run]:
        request = scout_run_api.SearchRunsRequest(
            page_size=100,
            query=_create_search_runs_query(start, end, name_substring, label, property),
            sort=scout_run_api.SortOptions(
                field=scout_run_api.SortField.START_TIME,
                is_descending=True,
            ),
        )
        for run in self._search_runs_paginated(request):
            yield Run._from_conjure(self, run)

    @deprecate_keyword_argument("name_substring", "exact_name")
    def search_runs(
        self,
        start: datetime | IntegralNanosecondsUTC | None = None,
        end: datetime | IntegralNanosecondsUTC | None = None,
        name_substring: str | None = None,
        label: str | None = None,
        property: tuple[str, str] | None = None,
    ) -> Sequence[Run]:
        """Search for runs meeting the specified filters.
        Filters are ANDed together, e.g. `(run.label == label) AND (run.end <= end)`
        - `start` and `end` times are both inclusive
        - `name_substring`: search for a (case-insensitive) substring in the name
        - `property` is a key-value pair, e.g. ("name", "value")
        """
        return list(self._iter_search_runs(start, end, name_substring, label, property))

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
    ) -> Dataset:
        """Create a dataset from a CSV file.

        If name is None, the name of the file will be used.

        See `create_dataset_from_io` for more details.
        """
        path, file_type = _verify_csv_path(path)
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
        timestamp_type: _AnyTimestampType,
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
                    timestamp_type=_to_typed_timestamp_type(timestamp_type)._to_conjure_ingest_api(),
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
                    starting_timestamp=_SecondsNanos.from_flexible(start).to_ingest_api()
                )
            ),
            description=description,
            title=name,
        )
        response = self._ingest_client.ingest_video(self._auth_header, request)
        return self.get_video(response.video_rid)

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
        response = self._logset_client.create(self._auth_header, request)
        return self._attach_logs_and_finalize(response.rid, _logs_to_conjure(logs))

    def _attach_logs_and_finalize(self, rid: str, logs: Iterable[datasource_logset_api.Log]) -> LogSet:
        request = datasource_logset_api.AttachLogsAndFinalizeRequest(logs=list(logs))
        response = self._logset_client.attach_logs_and_finalize(
            auth_header=self._auth_header, log_set_rid=rid, request=request
        )
        return LogSet._from_conjure(self, response)

    def get_video(self, rid: str) -> Video:
        """Retrieve a video by its RID."""
        response = self._video_client.get(self._auth_header, rid)
        return Video._from_conjure(self, response)

    def _iter_get_videos(self, rids: Iterable[str]) -> Iterable[Video]:
        request = scout_video_api.GetVideosRequest(video_rids=list(rids))
        for response in self._video_client.batch_get(self._auth_header, request).responses:
            yield Video._from_conjure(self, response)

    def get_videos(self, rids: Iterable[str]) -> Sequence[Video]:
        """Retrieve videos by their RID."""
        return list(self._iter_get_videos(rids))

    def get_dataset(self, rid: str) -> Dataset:
        """Retrieve a dataset by its RID."""
        response = _get_dataset(self._auth_header, self._catalog_client, rid)
        return Dataset._from_conjure(self, response)

    def get_log_set(self, log_set_rid: str) -> LogSet:
        """Retrieve a log set along with its metadata given its RID."""
        response = _get_log_set(self._auth_header, self._logset_client, log_set_rid)
        return LogSet._from_conjure(self, response)

    def _iter_get_datasets(self, rids: Iterable[str]) -> Iterable[Dataset]:
        for ds in _get_datasets(self._auth_header, self._catalog_client, rids):
            yield Dataset._from_conjure(self, ds)

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
        response = self._catalog_client.search_datasets(self._auth_header, request)
        for ds in response.results:
            yield Dataset._from_conjure(self, ds)

    def _get_user_rid_from_email(self, user_email: str) -> str:
        request = authentication_api.SearchUsersRequest(
            query=authentication_api.SearchUsersQuery(
                exact_match=user_email,
            )
        )
        response = self._authentication_client.search_users_v2(self._auth_header, request)
        if len(response.results) == 0:
            raise ValueError(f"user {user_email!r} not found")
        if len(response.results) > 1:
            raise ValueError(f"found multiple users with email {user_email!r}")
        return response.results[0].rid

    def get_checklist(self, rid: str) -> Checklist:
        response = self._checklist_api_client.get(self._auth_header, rid)
        return Checklist._from_conjure(self, response)

    def checklist_builder(
        self,
        name: str,
        description: str = "",
        assignee_email: str | None = None,
        assignee_rid: str | None = None,
        default_ref_name: str | None = None,
    ) -> ChecklistBuilder:
        """Creates a checklist builder.

        You can provide one of `assignee_email` or `assignee_rid`. If neither are provided, the rid for the user
        executing the script will be used as the assignee. If both are provided, a ValueError is raised.
        """
        return ChecklistBuilder(
            name=name,
            description=description,
            assignee_rid=_get_assignee_rid(self, assignee_email, assignee_rid),
            _default_ref_name=default_ref_name,
            _variables=[],
            _checks=[],
            _properties={},
            _labels=[],
            _client=self,
        )

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

    def get_attachment(self, rid: str) -> Attachment:
        """Retrieve an attachment by its RID."""
        response = self._attachment_client.get(self._auth_header, rid)
        return Attachment._from_conjure(self, response)

    def _iter_get_attachments(self, rids: Iterable[str]) -> Iterable[Attachment]:
        request = attachments_api.GetAttachmentsRequest(attachment_rids=list(rids))
        response = self._attachment_client.get_batch(self._auth_header, request)
        for a in response.response:
            yield Attachment._from_conjure(self, a)

    def get_attachments(self, rids: Iterable[str]) -> Sequence[Attachment]:
        """Retrive attachments by their RIDs."""
        return list(self._iter_get_attachments(rids))

    def get_all_units(self) -> Sequence[scout_units_api.Unit]:
        """Retrieve list of all allowable units"""
        response = self._units_client.get_all_units(self._auth_header)
        return [unit for units in response.units_by_property.values() for unit in units]

    def get_unit(self, unit_symbol: str) -> scout_units_api.Unit | None:
        """Get details of the given unit symbol, or none if invalid"""
        return self._units_client.get_unit(self._auth_header, unit_symbol)

    def get_commensurable_units(self, unit_symbol: str) -> Sequence[scout_units_api.Unit]:
        """Get the list of units that are commensurable (convertible to/from) the given unit symbol"""
        return self._units_client.get_commensurable_units(self._auth_header, unit_symbol)


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


def _get_log_set(
    auth_header: str, client: datasource_logset.LogSetService, log_set_rid: str
) -> datasource_logset_api.LogSetMetadata:
    return client.get_log_set_metadata(auth_header, log_set_rid)


def _create_search_runs_query(
    start: datetime | IntegralNanosecondsUTC | None = None,
    end: datetime | IntegralNanosecondsUTC | None = None,
    name_substring: str | None = None,
    label: str | None = None,
    property: tuple[str, str] | None = None,
) -> scout_run_api.SearchQuery:
    queries = []
    if start is not None:
        q = scout_run_api.SearchQuery(start_time_inclusive=_SecondsNanos.from_flexible(start).to_scout_run_api())
        queries.append(q)
    if end is not None:
        q = scout_run_api.SearchQuery(end_time_inclusive=_SecondsNanos.from_flexible(end).to_scout_run_api())
        queries.append(q)
    if name_substring is not None:
        q = scout_run_api.SearchQuery(exact_match=name_substring)
        queries.append(q)
    if label is not None:
        q = scout_run_api.SearchQuery(label=label)
        queries.append(q)
    if property is not None:
        name, value = property
        q = scout_run_api.SearchQuery(property=scout_run_api.Property(name=name, value=value))
        queries.append(q)
    return scout_run_api.SearchQuery(and_=queries)


def _verify_csv_path(path: Path | str) -> tuple[Path, FileType]:
    path = Path(path)
    file_type = FileType.from_path_dataset(path)
    if file_type.extension not in (".csv", ".csv.gz"):
        raise ValueError(f"file {path} must end with '.csv' or '.csv.gz'")
    return path, file_type


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


def _get_assignee_rid(client: NominalClient, assignee_email: str | None, assignee_rid: str | None) -> str:
    if assignee_email is not None and assignee_rid is not None:
        raise ValueError("only one of assignee_email or assignee_rid should be provided")
    if assignee_email is not None:
        return client._get_user_rid_from_email(assignee_email)
    if assignee_rid is not None:
        return assignee_rid
    return client.get_user().rid


def _rid_from_instance_or_string(value: Attachment | Run | Dataset | Video | LogSet | str) -> str:
    from . import Attachment, Dataset, LogSet, Run, Video

    if isinstance(value, str):
        return value
    elif isinstance(value, (Attachment, Dataset, LogSet, Run, Video)):
        return value.rid
    raise TypeError("{value!r} is not a string nor supported instance")
