from __future__ import annotations

import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from io import TextIOBase
from io import TextIOBase
from types import MappingProxyType
from typing import BinaryIO, Iterable, Literal, Mapping, Sequence, cast

import certifi
from conjure_python_client import RequestsClient, ServiceConfiguration, SslConfiguration

from ._api.combined import attachments_api
from ._api.combined import scout_catalog
from ._api.combined import scout
from ._api.combined import scout_run_api
from ._api.combined import ingest_api
from ._api.combined import upload_api
from ._multipart import put_multipart_upload
from ._utils import (
    FileType,
    FileTypes,
    _conjure_time_to_integral_nanoseconds,
    _flexible_time_to_conjure_scout_run_api,
    _timestamp_type_to_conjure_ingest_api,
    construct_user_agent_string,
    CustomTimestampFormat,
    IntegralNanosecondsUTC,
    Self,
    TimestampColumnType,
    update_dataclass,
)
from .exceptions import NominalIngestError, NominalIngestFailed


_AllowedFileExtensions = Literal[".csv", ".csv.gz", ".parquet"]

__all__ = [
    "NominalClient",
    "Run",
    "Dataset",
    "Attachment",
    "IntegralNanosecondsUTC",
    "CustomTimestampFormat",
    "NominalIngestError",
    "NominalIngestFailed",
]


@dataclass(frozen=True)
class Run:
    rid: str
    title: str
    description: str
    properties: Mapping[str, str]
    labels: Sequence[str]
    start: IntegralNanosecondsUTC
    end: IntegralNanosecondsUTC | None
    _client: NominalClient = field(repr=False)

    def add_dataset(self, ref_name: str, dataset: Dataset | str) -> None:
        """Add a dataset to this run.

        Datasets map "ref names" (their name within the run) to a Dataset (or dataset rid). The same type of datasets
        should use the same ref name across runs, since checklists and templates use ref names to reference datasets.
        """
        # TODO(alkasm): support series tags & offset
        data_sources = {
            ref_name: scout_run_api.CreateRunDataSource(
                data_source=scout_run_api.DataSource(dataset=_rid_from_instance_or_string(dataset)),
                series_tags={},
                offset=None,
            )
        }
        self._client._run_client.add_data_sources_to_run(self._client._auth_header, data_sources, self.rid)

    def list_datasets(self) -> Iterable[tuple[str, Dataset]]:
        """List the datasets associated with this run.
        Yields (ref_name, dataset) pairs.
        """
        run = self._client._run_client.get_run(self._client._auth_header, self.rid)
        dataset_rids_by_ref_name = {}
        for ref_name, source in run.data_sources.items():
            if source.data_source.type == "dataset":
                dataset_rid = cast(str, source.data_source.dataset)
                dataset_rids_by_ref_name[ref_name] = dataset_rid
        datasets_by_rids = {ds.rid: ds for ds in self._client.get_datasets(dataset_rids_by_ref_name.values())}
        for ref_name, rid in dataset_rids_by_ref_name.items():
            dataset = datasets_by_rids[rid]
            yield (ref_name, dataset)

    def add_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        """Add attachments that have already been uploaded to this run.

        `attachments` can be `Attachment` instances, or attachment RIDs.
        """
        rids = [_rid_from_instance_or_string(a) for a in attachments]
        request = scout_run_api.UpdateAttachmentsRequest(attachments_to_add=rids, attachments_to_remove=[])
        self._client._run_client.update_run_attachment(self._client._auth_header, request, self.rid)

    def remove_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> None:
        """Remove attachments from this run.
        Does not remove the attachments from Nominal.

        `attachments` can be `Attachment` instances, or attachment RIDs.
        """
        rids = [_rid_from_instance_or_string(a) for a in attachments]
        request = scout_run_api.UpdateAttachmentsRequest(attachments_to_add=[], attachments_to_remove=rids)
        self._client._run_client.update_run_attachment(self._client._auth_header, request, self.rid)

    def list_attachments(self) -> Iterable[Attachment]:
        run = self._client._run_client.get_run(self._client._auth_header, self.rid)
        return self._client.get_attachments(run.attachments)

    def update(
        self,
        *,
        title: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> Self:
        """Replace run metadata.
        Updates the current instance, and returns it.
        Only the metadata passed in will be replaced, the rest will remain untouched.

        Note: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
        calling this method. E.g.:

            new_labels = ["new-label-a", "new-label-b"]
            for old_label in run.labels:
                new_labels.append(old_label)
            run = run.update(labels=new_labels)
        """
        request = scout_run_api.UpdateRunRequest(
            description=description,
            labels=None if labels is None else list(labels),
            properties=None if properties is None else dict(properties),
            title=title,
        )
        response = self._client._run_client.update_run(self._client._auth_header, request, self.rid)
        run = self.__class__._from_conjure(self._client, response)
        update_dataclass(self, run, fields=self.__dataclass_fields__)
        return self

    @classmethod
    def _from_conjure(cls, nominal_client: NominalClient, run: scout_run_api.Run) -> Self:
        return cls(
            rid=run.rid,
            title=run.title,
            description=run.description,
            properties=MappingProxyType(run.properties),
            labels=tuple(run.labels),
            start=_conjure_time_to_integral_nanoseconds(run.start_time),
            end=(_conjure_time_to_integral_nanoseconds(run.end_time) if run.end_time else None),
            _client=nominal_client,
        )


@dataclass(frozen=True)
class Dataset:
    rid: str
    name: str
    description: str | None
    properties: Mapping[str, str]
    labels: Sequence[str]
    _client: NominalClient = field(repr=False)

    def poll_until_ingestion_completed(self, interval: timedelta = timedelta(seconds=1)) -> None:
        """Block until dataset ingestion has completed.
        This method polls Nominal for ingest status after uploading a dataset on an interval.

        Raises:
            NominalIngestFailed: if the ingest failed
            NominalIngestError: if the ingest status is not known
        """

        while True:
            progress = self._client._catalog_client.get_ingest_progress_v2(self._client._auth_header, self.rid)
            if progress.ingest_status.type == "success":
                return
            elif progress.ingest_status.type == "inProgress":  # "type" strings are camelCase
                pass
            elif progress.ingest_status.type == "error":
                error = progress.ingest_status.error
                if error is not None:
                    raise NominalIngestFailed(
                        f"ingest failed for dataset {self.rid!r}: {error.message} ({error.error_type})"
                    )
                raise NominalIngestError(
                    f"ingest status type marked as 'error' but with no instance for dataset {self.rid!r}"
                )
            else:
                raise NominalIngestError(
                    f"unhandled ingest status {progress.ingest_status.type!r} for dataset {self.rid!r}"
                )
            time.sleep(interval.total_seconds())

    def update(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> Self:
        """Replace dataset metadata.
        Updates the current instance, and returns it.

        Only the metadata passed in will be replaced, the rest will remain untouched.

        Note: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
        calling this method. E.g.:

            new_labels = ["new-label-a", "new-label-b"]
            for old_label in dataset.labels:
                new_labels.append(old_label)
            dataset = dataset.update(labels=new_labels)
        """
        request = scout_catalog.UpdateDatasetMetadata(
            description=description,
            labels=None if labels is None else list(labels),
            name=name,
            properties=None if properties is None else dict(properties),
        )
        response = self._client._catalog_client.update_dataset_metadata(self._client._auth_header, self.rid, request)

        dataset = self.__class__._from_conjure(self._client, response)
        update_dataclass(self, dataset, fields=self.__dataclass_fields__)
        return self

    def add_to_dataset_from_io(
        self,
        dataset: BinaryIO,
        timestamp_column: str,
        timestamp_type: TimestampColumnType,
        file_type: FileType = FileTypes.CSV,
    ) -> None:
        """Append to a dataset from a file-like object."""

        if not isinstance(timestamp_type, CustomTimestampFormat):
            if timestamp_type.startswith("relative"):
                raise ValueError(
                    "multifile datasets with relative timestamps are not yet supported by the client library"
                )

        if isinstance(dataset, TextIOBase):
            raise TypeError(f"dataset {dataset!r} must be open in binary mode, rather than text mode")

        file_type = FileType(*file_type)

        self.poll_until_ingestion_completed()
        urlsafe_name = urllib.parse.quote_plus(self.name)
        filename = f"{urlsafe_name}{file_type.extension}"
        s3_path = put_multipart_upload(
            self._client._auth_header, dataset, filename, file_type.mimetype, self._client._upload_client
        )
        request = ingest_api.TriggerFileIngest(
            destination=ingest_api.IngestDestination(
                existing_dataset=ingest_api.ExistingDatasetIngestDestination(dataset_rid=self.rid)
            ),
            source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path)),
            source_metadata=ingest_api.IngestSourceMetadata(
                timestamp_metadata=ingest_api.TimestampMetadata(
                    series_name=timestamp_column,
                    timestamp_type=_timestamp_type_to_conjure_ingest_api(timestamp_type),
                ),
            ),
        )
        self._client._ingest_client.trigger_file_ingest(self._client._auth_header, request)

    @classmethod
    def _from_conjure(cls, client: NominalClient, dataset: scout_catalog.EnrichedDataset) -> Self:
        return cls(
            rid=dataset.rid,
            name=dataset.name,
            description=dataset.description,
            properties=MappingProxyType(dataset.properties),
            labels=tuple(dataset.labels),
            _client=client,
        )


@dataclass(frozen=True)
class Attachment:
    rid: str
    title: str
    description: str
    properties: Mapping[str, str]
    labels: Sequence[str]
    _client: NominalClient = field(repr=False)

    def update(
        self,
        *,
        title: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> Self:
        """Replace attachment metadata.
        Updates the current instance, and returns it.

        Only the metadata passed in will be replaced, the rest will remain untouched.

        Note: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
        calling this method. E.g.:

            new_labels = ["new-label-a", "new-label-b", *attachment.labels]
            attachment = attachment.update(labels=new_labels)
        """
        request = attachments_api.UpdateAttachmentRequest(
            description=description,
            labels=None if labels is None else list(labels),
            properties=None if properties is None else dict(properties),
            title=title,
        )
        response = self._client._attachment_client.update(self._client._auth_header, request, self.rid)
        attachment = self.__class__._from_conjure(self._client, response)
        update_dataclass(self, attachment, fields=self.__dataclass_fields__)
        return self

    def get_contents(self) -> BinaryIO:
        """Retrieves the contents of this attachment.
        Returns a file-like object in binary mode for reading.
        """
        response = self._client._attachment_client.get_content(self._client._auth_header, self.rid)
        # note: the response is the same as the requests.Response.raw field, with stream=True on the request;
        # this acts like a file-like object in binary-mode.
        return cast(BinaryIO, response)

    @classmethod
    def _from_conjure(
        cls,
        client: NominalClient,
        attachment: attachments_api.Attachment,
    ) -> Self:
        return cls(
            rid=attachment.rid,
            title=attachment.title,
            description=attachment.description,
            properties=MappingProxyType(attachment.properties),
            labels=tuple(attachment.labels),
            _client=client,
        )


@dataclass(frozen=True)
class NominalClient:
    _auth_header: str = field(repr=False)
    _run_client: scout.RunService = field(repr=False)
    _upload_client: upload_api.UploadService = field(repr=False)
    _ingest_client: ingest_api.IngestService = field(repr=False)
    _catalog_client: scout_catalog.CatalogService = field(repr=False)
    _attachment_client: attachments_api.AttachmentService = field(repr=False)

    @classmethod
    def create(cls, base_url: str, token: str, trust_store_path: str | None = None) -> Self:
        """Create a connection to the Nominal platform.

        base_url: The URL of the Nominal API platform, e.g. https://api.gov.nominal.io/api.
        token: An API token to authenticate with. You can grab a client token from the Nominal sandbox, e.g.
            at https://app.gov.nominal.io/sandbox.
        trust_store_path: path to a trust store CA root file to initiate SSL connections. If not provided,
            certifi's trust store is used.
        """
        trust_store_path = certifi.where() if trust_store_path is None else trust_store_path
        cfg = ServiceConfiguration(uris=[base_url], security=SslConfiguration(trust_store_path=trust_store_path))

        agent = construct_user_agent_string()
        run_client = RequestsClient.create(scout.RunService, agent, cfg)
        upload_client = RequestsClient.create(upload_api.UploadService, agent, cfg)
        ingest_client = RequestsClient.create(ingest_api.IngestService, agent, cfg)
        catalog_client = RequestsClient.create(scout_catalog.CatalogService, agent, cfg)
        attachment_client = RequestsClient.create(attachments_api.AttachmentService, agent, cfg)
        auth_header = f"Bearer {token}"
        return cls(
            _auth_header=auth_header,
            _run_client=run_client,
            _upload_client=upload_client,
            _ingest_client=ingest_client,
            _catalog_client=catalog_client,
            _attachment_client=attachment_client,
        )

    def create_run(
        self,
        title: str,
        description: str,
        start: datetime | IntegralNanosecondsUTC,
        end: datetime | IntegralNanosecondsUTC,
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
            description=description,
            labels=list(labels),
            links=[],
            properties={} if properties is None else dict(properties),
            start_time=_flexible_time_to_conjure_scout_run_api(start),
            title=title,
            end_time=_flexible_time_to_conjure_scout_run_api(end),
        )
        response = self._run_client.create_run(self._auth_header, request)
        return Run._from_conjure(self, response)

    def get_run(self, run: Run | str) -> Run:
        """Retrieve a run by run or run RID."""
        run_rid = _rid_from_instance_or_string(run)
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

    def search_runs(
        self,
        start: datetime | IntegralNanosecondsUTC | None = None,
        end: datetime | IntegralNanosecondsUTC | None = None,
        exact_title: str | None = None,
        label: str | None = None,
        property: tuple[str, str] | None = None,
    ) -> Iterable[Run]:
        """Search for runs meeting the specified filters.
        Filters are ANDed together, e.g. `(run.label == label) AND (run.end <= end)`
        - `start` and `end` times are both inclusive
        - `exact_title` is case-insensitive
        - `property` is a key-value pair, e.g. ("name", "value")
        """
        request = scout_run_api.SearchRunsRequest(
            page_size=100,
            query=_create_search_runs_query(start, end, exact_title, label, property),
            sort=scout_run_api.SortOptions(
                field=scout_run_api.SortField.START_TIME,
                is_descending=True,
            ),
        )
        for run in self._search_runs_paginated(request):
            yield Run._from_conjure(self, run)

    def create_dataset_from_io(
        self,
        dataset: BinaryIO,
        name: str,
        timestamp_column: str,
        timestamp_type: TimestampColumnType,
        file_type: FileType = FileTypes.CSV,
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

    def get_dataset(self, dataset: Dataset | str) -> Dataset:
        """Retrieve a dataset by dataset or dataset RID."""
        dataset_rid = _rid_from_instance_or_string(dataset)
        response = _get_dataset(self._auth_header, self._catalog_client, dataset_rid)
        return Dataset._from_conjure(self, response)

    def get_datasets(self, datasets: Iterable[Dataset] | Iterable[str]) -> Iterable[Dataset]:
        """Retrieve datasets by dataset or dataset RID."""
        dataset_rids = (_rid_from_instance_or_string(ds) for ds in datasets)
        for ds in _get_datasets(self._auth_header, self._catalog_client, dataset_rids):
            yield Dataset._from_conjure(self, ds)

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
        title: str,
        description: str,
        file_type: FileType = FileTypes.BINARY,
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
        urlsafe_name = urllib.parse.quote_plus(title)
        filename = f"{urlsafe_name}{file_type.extension}"

        s3_path = put_multipart_upload(self._auth_header, attachment, filename, file_type.mimetype, self._upload_client)
        request = attachments_api.CreateAttachmentRequest(
            description=description,
            labels=list(labels),
            properties={} if properties is None else dict(properties),
            s3_path=s3_path,
            title=title,
        )
        response = self._attachment_client.create(self._auth_header, request)
        return Attachment._from_conjure(self, response)

    def get_attachment(self, attachment: Attachment | str) -> Attachment:
        """Retrieve an attachment by attachment or attachment RID."""
        attachment_rid = _rid_from_instance_or_string(attachment)
        response = self._attachment_client.get(self._auth_header, attachment_rid)
        return Attachment._from_conjure(self, response)

    def get_attachments(self, attachments: Iterable[Attachment] | Iterable[str]) -> Iterable[Attachment]:
        """Retrieve multiple attachments by attachment or attachment RIDs."""
        rids = [_rid_from_instance_or_string(a) for a in attachments]
        request = attachments_api.GetAttachmentsRequest(attachment_rids=rids)
        response = self._attachment_client.get_batch(self._auth_header, request)
        for a in response.response:
            yield Attachment._from_conjure(self, a)


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


def _rid_from_instance_or_string(value: Attachment | Run | Dataset | str) -> str:
    if isinstance(value, str):
        return value
    elif isinstance(value, (Attachment, Run, Dataset)):
        return value.rid
    elif hasattr(value, "rid"):
        return value.rid
    raise TypeError("{value!r} is not a string nor has the attribute 'rid'")


def _create_search_runs_query(
    start: datetime | IntegralNanosecondsUTC | None = None,
    end: datetime | IntegralNanosecondsUTC | None = None,
    exact_title: str | None = None,
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
    if exact_title is not None:
        q = scout_run_api.SearchQuery(exact_match=exact_title)
        queries.append(q)
    if label is not None:
        q = scout_run_api.SearchQuery(label=label)
        queries.append(q)
    if property is not None:
        name, value = property
        q = scout_run_api.SearchQuery(property=scout_run_api.Property(name=name, value=value))
        queries.append(q)
    return scout_run_api.SearchQuery(and_=queries)
