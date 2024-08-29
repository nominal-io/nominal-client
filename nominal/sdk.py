from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import BinaryIO, Iterable, Literal, Mapping, Sequence, TextIO, Type, cast

from conjure_python_client import RequestsClient, Service, ServiceConfiguration

from ._api.combined import attachments_api
from ._api.combined import scout_catalog
from ._api.combined import scout
from ._api.combined import scout_run_api
from ._api.ingest import ingest_api
from ._api.ingest import upload_api
from ._timeutils import (
    _TimestampColumnType,
    _flexible_time_to_conjure_scout_run_api,
    _conjure_time_to_integral_nanoseconds,
    _timestamp_type_to_conjure_ingest_api,
    IntegralNanosecondsUTC,
    CustomTimestampFormat,
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
    _client: NominalClient

    def add_datasets(self, datasets: Mapping[str, str]) -> None:
        """Adds datasets to this run.

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
        self._client._run_client.add_data_sources_to_run(self._client._auth_header, data_sources, self.rid)

    def list_datasets(self) -> Iterable[tuple[str, str]]:
        """Lists the datasets associated with this run.

        Yields (ref_name, dataset_rid) pairs.
        """
        run = self._client._run_client.get_run(self._client._auth_header, self.rid)
        for ref_name, source in run.data_sources.items():
            if source.data_source.type == "dataset":
                dataset_rid = cast(str, source.data_source.dataset)
                yield (ref_name, dataset_rid)

    def add_attachment(self) -> None:
        raise NotImplementedError()

    def create_attachment(self) -> Dataset:
        raise NotImplementedError()

    def list_attachments(self) -> list[Attachment]:
        raise NotImplementedError()

    def replace(
        self,
        *,
        title: str | None,
        description: str | None,
        properties: Mapping[str, str] | None,
        labels: Sequence[str] | None,
    ) -> Run:
        request = scout_run_api.UpdateRunRequest(
            description=description or self.description,
            labels=list(labels or self.labels),
            properties=dict(properties or self.properties),
            title=title or self.title,
        )
        response = self._client._run_client.update_run(self._client._auth_header, request, self.rid)
        return Run._from_conjure_scout_run_api(self._client, response)

    @classmethod
    def _from_conjure_scout_run_api(cls, client: NominalClient, run: scout_run_api.Run) -> Run:
        return cls(
            rid=run.rid,
            title=run.title,
            description=run.description,
            properties=MappingProxyType(run.properties),
            labels=tuple(run.labels),
            start=_conjure_time_to_integral_nanoseconds(run.start_time),
            end=(_conjure_time_to_integral_nanoseconds(run.end_time) if run.end_time else None),
            _client=client,
        )


@dataclass(frozen=True)
class Dataset:
    rid: str
    name: str
    description: str | None
    properties: Mapping[str, str]
    labels: Sequence[str]
    _client: NominalClient

    def poll_until_ingestion_completed(self, dataset_rid: str, interval: timedelta = timedelta(seconds=2)) -> None:
        while True:
            dataset = self._client._get_dataset(dataset_rid)
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
    ) -> Dataset:
        request = scout_catalog.UpdateDatasetMetadata(
            description=description or self.description,
            labels=list(labels or self.labels),
            name=name or self.name,
            properties=dict(properties or self.properties),
        )
        response = self._client._catalog_client.update_dataset_metadata(self._client._auth_header, self.rid, request)
        return Dataset._from_conjure_scout_catalog(self._client, response)

    @classmethod
    def _from_conjure_scout_catalog(cls, client: NominalClient, ds: scout_catalog.EnrichedDataset) -> Dataset:
        return cls(
            rid=ds.rid,
            name=ds.name,
            description=ds.description,
            properties=MappingProxyType(ds.properties),
            labels=tuple(ds.labels),
            _client=client,
        )


@dataclass(frozen=True)
class Attachment:
    rid: str
    title: str
    description: str
    properties: Mapping[str, str]
    labels: Sequence[str]
    _client: NominalClient

    def replace(
        self,
        *,
        title: str | None = None,
        description: str | None = None,
        properties: Mapping[str, str] | None = None,
        labels: Sequence[str] | None = None,
    ) -> Attachment:
        raise NotImplementedError()

    @classmethod
    def _from_conjure(cls, client: NominalClient, attachment: attachments_api.Attachment) -> Attachment:
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
    _auth_header: str
    _run_client: scout.RunService
    _upload_client: upload_api.UploadService
    _ingest_client: ingest_api.IngestService
    _catalog_client: scout_catalog.CatalogService

    @classmethod
    def create(cls, base_url: str, token: str) -> NominalClient:
        cfg = ServiceConfiguration(uris=[base_url])
        # TODO(alkasm): add library version to user agent
        agent = "nominal-python"
        run_client = RequestsClient.create(scout.RunService, agent, cfg)
        upload_client = RequestsClient.create(upload_api.UploadService, agent, cfg)
        ingest_client = RequestsClient.create(ingest_api.IngestService, agent, cfg)
        catalog_client = RequestsClient.create(scout_catalog.CatalogService, agent, cfg)
        auth_header = f"Bearer {token}"
        return cls(
            _auth_header=auth_header,
            _run_client=run_client,
            _upload_client=upload_client,
            _ingest_client=ingest_client,
            _catalog_client=catalog_client,
        )

    def create_run(
        self,
        title: str,
        start_time: datetime | IntegralNanosecondsUTC,
        description: str = "",
        datasets: Mapping[str, str] | None = None,
        end_time: datetime | IntegralNanosecondsUTC | None = None,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
        attachment_rids: Sequence[str] = (),
    ) -> Run:
        """Creates a run in the Nominal platform.

        Datasets map "ref names" (their name within the run) to dataset RIDs.
            The same type of datasets should use the same ref name across runs,
            since checklists and templates use ref names to reference datasets.
            The RIDs are retrieved from creating or getting a `Dataset` object.
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
        return Run._from_conjure_scout_run_api(self, response)

    def get_run(self, run_rid: str) -> Run:
        response = self._run_client.get_run(self._auth_header, run_rid)
        return Run._from_conjure_scout_run_api(self, response)

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

    def list_runs(self) -> Iterable[Run]:
        # TODO(alkasm): search filters
        request = scout_run_api.SearchRunsRequest(
            page_size=100,
            query=scout_run_api.SearchQuery(),
            sort=scout_run_api.SortOptions(
                field=scout_run_api.SortField.START_TIME,
                is_descending=True,
            ),
        )
        for run in self._list_runs_paginated(request):
            yield Run._from_conjure_scout_run_api(self, run)

    def create_dataset_from_io(
        self,
        name: str,
        csvfile: TextIO | BinaryIO,
        timestamp_column_name: str,
        timestamp_column_type: _TimestampColumnType,
        file_extension: _AllowedFileExtensions = ".csv",
        description: str | None = None,
        labels: Sequence[str] = (),
        properties: Mapping[str, str] | None = None,
    ) -> str:
        s3_path = self._upload_client.upload_file(self._auth_header, csvfile, file_name=f"{name}{file_extension}")
        request = ingest_api.TriggerIngest(
            labels=list(labels),
            properties=dict(properties or {}),
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

    def _get_datasets(self, dataset_rids: Iterable[str]) -> Iterable[scout_catalog.EnrichedDataset]:
        request = scout_catalog.GetDatasetsRequest(dataset_rids=list(dataset_rids))
        yield from self._catalog_client.get_enriched_datasets(self._auth_header, request)

    def _get_dataset(self, dataset_rid: str) -> scout_catalog.EnrichedDataset:
        datasets = list(self._get_datasets([dataset_rid]))
        if not datasets:
            raise ValueError(f"dataset not found: {dataset_rid}")
        if len(datasets) > 1:
            raise ValueError(f"expected exactly one dataset, got: {len(datasets)}")
        return datasets[0]

    def get_dataset(self, dataset_rid: str) -> Dataset:
        return Dataset._from_conjure_scout_catalog(self, self._get_dataset(dataset_rid))

    def get_datasets(self, dataset_rids: Iterable[str]) -> Iterable[Dataset]:
        for ds in self._get_datasets(dataset_rids):
            yield Dataset._from_conjure_scout_catalog(self, ds)

    def search_datasets(self) -> Iterable[Dataset]:
        # TODO(alkasm): search filters
        request = scout_catalog.SearchDatasetsRequest(
            query=scout_catalog.SearchDatasetsQuery(),
            sort_options=scout_catalog.SortOptions(field=scout_catalog.SortField.INGEST_DATE, is_descending=True),
        )
        response = self._catalog_client.search_datasets(self._auth_header, request)
        for ds in response.results:
            yield Dataset._from_conjure_scout_catalog(self, ds)

    def create_attachment(self) -> Attachment:
        raise NotImplementedError()

    def get_attachment(self, attachment_rid: str) -> Attachment:
        raise NotImplementedError()

    def list_attachments(self) -> Iterable[Attachment]:
        raise NotImplementedError()
