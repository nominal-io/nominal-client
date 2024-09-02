from __future__ import annotations

from io import TextIOBase
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import BinaryIO, Iterable, Literal, Mapping, Self, Sequence, TextIO, Type, cast

import certifi
from conjure_python_client import RequestsClient, Service, ServiceConfiguration, SslConfiguration

from ._multipart import put_multipart_upload

from ._api.combined import attachments_api
from ._api.combined import scout_catalog
from ._api.combined import scout
from ._api.combined import scout_run_api
from ._api.ingest import ingest_api
from ._api.ingest import upload_api
from ._utils import (
    TimestampColumnType,
    _flexible_time_to_conjure_scout_run_api,
    _conjure_time_to_integral_nanoseconds,
    _timestamp_type_to_conjure_ingest_api,
    IntegralNanosecondsUTC,
    CustomTimestampFormat,
    construct_user_agent_string,
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
    _auth_header: str = field(repr=False)
    _run_client: scout.RunService = field(repr=False)

    def add_datasets(self, datasets: Mapping[str, str]) -> None:
        """Add datasets to this run.
        Datasets map "ref names" (their name within the run) to dataset RIDs.
            The same type of datasets should use the same ref name across runs,
            since checklists and templates use ref names to reference datasets.
            The RIDs are retrieved from creating or getting a `Dataset` object.
        """
        data_sources = {
            ref_name: scout_run_api.CreateRunDataSource(
                data_source=scout_run_api.DataSource(dataset=rid),
                series_tags={},
                offset=None,  # TODO(alkasm): support per-dataset offsets
            )
            for ref_name, rid in datasets.items()
        }
        self._run_client.add_data_sources_to_run(self._auth_header, data_sources, self.rid)

    def list_datasets(self) -> Iterable[tuple[str, str]]:
        """List the datasets associated with this run.
        Yields (ref_name, dataset_rid) pairs.
        """
        run = self._run_client.get_run(self._auth_header, self.rid)
        for ref_name, source in run.data_sources.items():
            if source.data_source.type == "dataset":
                dataset_rid = cast(str, source.data_source.dataset)
                yield (ref_name, dataset_rid)

    def add_attachments(self, attachment_rids: Iterable[str]) -> None:
        """Add attachments that have already been uploaded to this run."""
        request = scout_run_api.UpdateAttachmentsRequest(
            attachments_to_add=list(attachment_rids), attachments_to_remove=[]
        )
        self._run_client.update_run_attachment(self._auth_header, request, self.rid)

    def remove_attachments(self, attachment_rids: Iterable[str]) -> None:
        """Remove attachments from this run.
        Does not remove the attachments from Nominal.
        """
        request = scout_run_api.UpdateAttachmentsRequest(
            attachments_to_add=[], attachments_to_remove=list(attachment_rids)
        )
        self._run_client.update_run_attachment(self._auth_header, request, self.rid)

    def replace(
        self,
        *,
        title: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> Self:
        """Replace run metadata.
        Returns the run with updated metadata.
        Only the metadata passed in will be replaced, the rest will remain untouched.

        Note: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
        calling this method. E.g.:

            new_labels = ["new-label-a", "new-label-b"]
            for old_label in run.labels:
                new_labels.append(old_label)
            run = run.replace(labels=new_labels)
        """
        request = scout_run_api.UpdateRunRequest(
            description=description,
            labels=None if labels is None else list(labels),
            properties=None if properties is None else dict(properties),
            title=title,
        )
        response = self._run_client.update_run(self._auth_header, request, self.rid)
        return self.__class__._from_conjure(self._auth_header, self._run_client, response)

    @classmethod
    def _from_conjure(cls, auth_header: str, run_client: scout.RunService, run: scout_run_api.Run) -> Self:
        return cls(
            rid=run.rid,
            title=run.title,
            description=run.description,
            properties=MappingProxyType(run.properties),
            labels=tuple(run.labels),
            start=_conjure_time_to_integral_nanoseconds(run.start_time),
            end=(_conjure_time_to_integral_nanoseconds(run.end_time) if run.end_time else None),
            _auth_header=auth_header,
            _run_client=run_client,
        )


@dataclass(frozen=True)
class Dataset:
    rid: str
    name: str
    description: str | None
    properties: Mapping[str, str]
    labels: Sequence[str]
    _auth_header: str = field(repr=False)
    _catalog_client: scout_catalog.CatalogService = field(repr=False)

    def poll_until_ingestion_completed(self, dataset_rid: str, interval: timedelta = timedelta(seconds=2)) -> None:
        """Block until dataset ingestion has completed.
        This method polls Nominal for ingest status after uploading a dataset on an interval.

        Raises:
            NominalIngestError: if the ingest status is not known
            NominalIngestFailed: if the ingest failed
        """
        while True:
            dataset = _get_dataset(self._auth_header, self._catalog_client, dataset_rid)
            if dataset.ingest_status == scout_catalog.IngestStatus.COMPLETED:
                return
            elif dataset.ingest_status == scout_catalog.IngestStatus.FAILED:
                raise NominalIngestFailed(f"ingest failed for dataset: {dataset.rid}")
            elif dataset.ingest_status == scout_catalog.IngestStatus.UNKNOWN:
                raise NominalIngestError(f"ingest status unknown for dataset: {dataset.rid}")
            time.sleep(interval.total_seconds())

    def replace(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> Self:
        """Replace dataset metadata.
        Returns the dataset with updated metadata.
        Only the metadata passed in will be replaced, the rest will remain untouched.

        Note: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
        calling this method. E.g.:

            new_labels = ["new-label-a", "new-label-b"]
            for old_label in dataset.labels:
                new_labels.append(old_label)
            dataset = dataset.replace(labels=new_labels)
        """
        request = scout_catalog.UpdateDatasetMetadata(
            description=description,
            labels=None if labels is None else list(labels),
            name=name,
            properties=None if properties is None else dict(properties),
        )
        response = self._catalog_client.update_dataset_metadata(self._auth_header, self.rid, request)
        return self.__class__._from_conjure(self._auth_header, self._catalog_client, response)

    @classmethod
    def _from_conjure(
        cls, auth_header: str, catalog_client: scout_catalog.CatalogService, ds: scout_catalog.EnrichedDataset
    ) -> Self:
        return cls(
            rid=ds.rid,
            name=ds.name,
            description=ds.description,
            properties=MappingProxyType(ds.properties),
            labels=tuple(ds.labels),
            _auth_header=auth_header,
            _catalog_client=catalog_client,
        )


@dataclass(frozen=True)
class Attachment:
    rid: str
    title: str
    description: str
    properties: Mapping[str, str]
    labels: Sequence[str]
    _auth_header: str = field(repr=False)
    _attachment_client: attachments_api.AttachmentService = field(repr=False)

    def replace(
        self,
        *,
        title: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> Self:
        """Replace attachment metadata.
        Returns the attachment with updated metadata.
        Only the metadata passed in will be replaced, the rest will remain untouched.

        Note: This replaces the metadata rather than appending it. To append to labels or properties, merge them before
        calling this method. E.g.:

            new_labels = ["new-label-a", "new-label-b", *attachment.labels]
            attachment = attachment.replace(labels=new_labels)
        """
        request = attachments_api.UpdateAttachmentRequest(
            description=description,
            labels=None if labels is None else list(labels),
            properties=None if properties is None else dict(properties),
            title=title,
        )
        response = self._attachment_client.update(self._auth_header, request, self.rid)
        return self.__class__._from_conjure(self._auth_header, self._attachment_client, response)

    def get_contents(self) -> BinaryIO:
        """Retrieves the contents of this attachment.
        Returns a file-like object in binary mode for reading.
        """
        response = self._attachment_client.get_content(self._auth_header, self.rid)
        # note: the response is the same as the requests.Response.raw field, with stream=True on the request;
        # this acts like a file-like object in binary-mode.
        return cast(BinaryIO, response)

    @classmethod
    def _from_conjure(
        cls,
        auth_header: str,
        attachment_client: attachments_api.AttachmentService,
        attachment: attachments_api.Attachment,
    ) -> Self:
        return cls(
            rid=attachment.rid,
            title=attachment.title,
            description=attachment.description,
            properties=MappingProxyType(attachment.properties),
            labels=tuple(attachment.labels),
            _auth_header=auth_header,
            _attachment_client=attachment_client,
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
        start_time: datetime | IntegralNanosecondsUTC,
        end_time: datetime | IntegralNanosecondsUTC | None = None,
        *,
        datasets: Mapping[str, str] | None = None,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        attachment_rids: Sequence[str] = (),
    ) -> Run:
        """Create a run.

        Datasets map "ref names" (their name within the run) to dataset RIDs. The same type of datasets should use the
            same ref name across runs, since checklists and templates use ref names to reference datasets. RIDs can be
            retrieved from `Dataset.rid` after getting or creating a dataset.
        """
        start_abs = _flexible_time_to_conjure_scout_run_api(start_time)
        end_abs = _flexible_time_to_conjure_scout_run_api(end_time) if end_time else None
        datasets = datasets or {}
        request = scout_run_api.CreateRunRequest(
            attachments=list(attachment_rids),
            data_sources={
                ref_name: scout_run_api.CreateRunDataSource(
                    data_source=scout_run_api.DataSource(dataset=rid),
                    series_tags={},
                    offset=None,  # TODO(alkasm): support per-dataset offsets
                )
                for ref_name, rid in datasets.items()
            },
            description=description,
            labels=list(labels),
            links=[],  # TODO(alkasm): support links
            properties={} if properties is None else dict(properties),
            start_time=start_abs,
            title=title,
            end_time=end_abs,
        )
        response = self._run_client.create_run(self._auth_header, request)
        return Run._from_conjure(self._auth_header, self._run_client, response)

    def get_run(self, run_rid: str) -> Run:
        """Retrieve a run."""
        response = self._run_client.get_run(self._auth_header, run_rid)
        return Run._from_conjure(self._auth_header, self._run_client, response)

    def _list_runs_paginated(self, request: scout_run_api.SearchRunsRequest) -> Iterable[scout_run_api.Run]:
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

    def _list_runs(self) -> Iterable[Run]:
        # TODO(alkasm): search filters
        # TODO(alkasm): put in public API when we decide if we only expose search, or search + list.
        request = scout_run_api.SearchRunsRequest(
            page_size=100,
            query=scout_run_api.SearchQuery(),
            sort=scout_run_api.SortOptions(
                field=scout_run_api.SortField.START_TIME,
                is_descending=True,
            ),
        )
        for run in self._list_runs_paginated(request):
            yield Run._from_conjure(self._auth_header, self._run_client, run)

    def create_dataset_from_io(
        self,
        dataset: BinaryIO,
        name: str,
        timestamp_column_name: str,
        timestamp_column_type: TimestampColumnType,
        file_extension: _AllowedFileExtensions = ".csv",
        *,
        description: str | None = None,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
    ) -> str:
        """Create a dataset from a file-like object.
        The dataset must be a file-like object in binary mode, e.g. open(path, "rb") or io.BytesIO.
        If the file is not in binary-mode, the requests library blocks indefinitely.

        Timestamp column types must be a `CustomTimestampFormat` or one of the following literals:
            "iso_8601": ISO 8601 formatted strings,
            "epoch_{unit}": epoch timestamps in UTC (floats or ints),
            "relative_{unit}": relative timestamps (floats or ints),
            where {unit} is one of: nanoseconds | microseconds | milliseconds | seconds | minutes | hours | days
        """

        if isinstance(dataset, TextIOBase):
            raise TypeError(f"dataset {dataset} must be open in binary mode, rather than text mode")
        filename = f"{name}{file_extension}"
        s3_path = put_multipart_upload(self._auth_header, dataset, filename, "text/csv", self._upload_client)
        request = ingest_api.TriggerIngest(
            labels=list(labels),
            properties={} if properties is None else dict(properties),
            source=ingest_api.IngestSource(s3=ingest_api.S3IngestSource(path=s3_path)),
            dataset_description=description,
            dataset_name=name,
            timestamp_metadata=ingest_api.TimestampMetadata(
                series_name=timestamp_column_name,
                timestamp_type=_timestamp_type_to_conjure_ingest_api(timestamp_column_type),
            ),
        )
        response = self._ingest_client.trigger_ingest(self._auth_header, request)
        return response.dataset_rid

    def get_dataset(self, dataset_rid: str) -> Dataset:
        """Retrieve a dataset."""
        dataset = _get_dataset(self._auth_header, self._catalog_client, dataset_rid)
        return Dataset._from_conjure(self._auth_header, self._catalog_client, dataset)

    def get_datasets(self, dataset_rids: Iterable[str]) -> Iterable[Dataset]:
        """Retrieve datasets."""
        for ds in _get_datasets(self._auth_header, self._catalog_client, dataset_rids):
            yield Dataset._from_conjure(self._auth_header, self._catalog_client, ds)

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
            yield Dataset._from_conjure(self._auth_header, self._catalog_client, ds)

    def create_attachment_from_io(
        self,
        attachment: BinaryIO,
        mimetype: str,
        title: str,
        description: str,
        *,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
    ) -> Attachment:
        """Upload an attachment.
        The attachment must be a file-like object in binary mode, e.g. open(path, "rb") or io.BytesIO.
        If the file is not in binary-mode, the requests library blocks indefinitely.
        """
        if isinstance(attachment, TextIOBase):
            raise TypeError(f"attachment {attachment} must be open in binary mode, rather than text mode")
        s3_path = put_multipart_upload(self._auth_header, attachment, title, mimetype, self._upload_client)
        request = attachments_api.CreateAttachmentRequest(
            description=description,
            labels=list(labels),
            properties={} if properties is None else dict(properties),
            s3_path=s3_path,
            title=title,
        )
        response = self._attachment_client.create(self._auth_header, request)
        return Attachment._from_conjure(self._auth_header, self._attachment_client, response)

    def get_attachment(self, attachment_rid: str) -> Attachment:
        """Retrieve an attachment."""
        attachment = self._attachment_client.get(self._auth_header, attachment_rid)
        return Attachment._from_conjure(self._auth_header, self._attachment_client, attachment)


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
        raise ValueError(f"dataset not found: {dataset_rid}")
    if len(datasets) > 1:
        raise ValueError(f"expected exactly one dataset, got: {len(datasets)}")
    return datasets[0]
