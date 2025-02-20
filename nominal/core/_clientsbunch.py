from __future__ import annotations

import time
from dataclasses import dataclass
from functools import partial
from typing import Protocol

from conjure_python_client import RequestsClient, Service, ServiceConfiguration
from nominal_api import (
    attachments_api,
    authentication_api,
    datasource_logset,
    ingest_api,
    scout,
    scout_assets,
    scout_catalog,
    scout_checklistexecution_api,
    scout_checks_api,
    scout_compute_api,
    scout_dataexport_api,
    scout_datareview_api,
    scout_datasource,
    scout_datasource_connection,
    scout_video,
    storage_datasource_api,
    storage_writer_api,
    timeseries_logicalseries,
    upload_api,
)
from typing_extensions import Self

from nominal.ts import IntegralNanosecondsUTC


@dataclass(frozen=True)
class RequestMetrics:
    largest_latency_before_request: float
    """
    delta between current time and oldest timestamp before request (seconds)
    """
    smallest_latency_before_request: float
    """
    delta between current time and newest timestamp before request (seconds)
    """
    request_rtt: float
    """
    delta between before and after request (seconds)
    """
    largest_latency_after_request: float
    """
    delta between current time and oldest timestamp after request (seconds)
    """
    smallest_latency_after_request: float
    """
    delta between current time and newest timestamp after request (seconds)
    """


class ProtoWriteService(Service):
    def write_nominal_batches(self, auth_header: str, data_source_rid: str, request: bytes) -> None:
        _headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-protobuf",
            "Authorization": auth_header,
        }
        _path = f"/storage/writer/v1/nominal/{data_source_rid}"
        self._request("POST", self._uri + _path, params={}, headers=_headers, data=request)

    def write_nominal_batches_with_metrics(
        self,
        auth_header: str,
        data_source_rid: str,
        request: bytes,
        oldest_timestamp: IntegralNanosecondsUTC,
        newest_timestamp: IntegralNanosecondsUTC,
    ) -> RequestMetrics:
        _headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-protobuf",
            "Authorization": auth_header,
        }
        _path = f"/storage/writer/v1/nominal/{data_source_rid}"
        before_req = time.time_ns()

        self._request("POST", self._uri + _path, params={}, headers=_headers, data=request)

        after_req = time.time_ns()

        return RequestMetrics(
            largest_latency_before_request=(before_req - oldest_timestamp) / 1e9,
            smallest_latency_before_request=(before_req - newest_timestamp) / 1e9,
            request_rtt=(after_req - before_req) / 1e9,
            largest_latency_after_request=(after_req - oldest_timestamp) / 1e9,
            smallest_latency_after_request=(after_req - newest_timestamp) / 1e9,
        )

    def write_prometheus_batches(self, auth_header: str, data_source_rid: str, request: bytes) -> None:
        _headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-protobuf",
            "Authorization": auth_header,
        }
        _path = f"/storage/writer/v1/prometheus/{data_source_rid}"
        self._request("POST", self._uri + _path, params={}, headers=_headers, data=request)


@dataclass(frozen=True)
class ClientsBunch:
    auth_header: str

    assets: scout_assets.AssetService
    attachment: attachments_api.AttachmentService
    authentication: authentication_api.AuthenticationServiceV2
    catalog: scout_catalog.CatalogService
    checklist: scout_checks_api.ChecklistService
    connection: scout_datasource_connection.ConnectionService
    dataexport: scout_dataexport_api.DataExportService
    datasource: scout_datasource.DataSourceService
    ingest: ingest_api.IngestService
    logical_series: timeseries_logicalseries.LogicalSeriesService
    logset: datasource_logset.LogSetService
    run: scout.RunService
    units: scout.UnitsService
    upload: upload_api.UploadService
    video: scout_video.VideoService
    compute: scout_compute_api.ComputeService
    storage: storage_datasource_api.NominalDataSourceService
    storage_writer: storage_writer_api.NominalChannelWriterService
    template: scout.TemplateService
    notebook: scout.NotebookService
    checklist_execution: scout_checklistexecution_api.ChecklistExecutionService
    datareview: scout_datareview_api.DataReviewService
    proto_write: ProtoWriteService

    @classmethod
    def from_config(cls, cfg: ServiceConfiguration, agent: str, token: str) -> Self:
        client_factory = partial(RequestsClient.create, user_agent=agent, service_config=cfg)

        return cls(
            auth_header=f"Bearer {token}",
            assets=client_factory(scout_assets.AssetService),
            attachment=client_factory(attachments_api.AttachmentService),
            authentication=client_factory(authentication_api.AuthenticationServiceV2),
            catalog=client_factory(scout_catalog.CatalogService),
            checklist=client_factory(scout_checks_api.ChecklistService),
            connection=client_factory(scout_datasource_connection.ConnectionService),
            dataexport=client_factory(scout_dataexport_api.DataExportService),
            datasource=client_factory(scout_datasource.DataSourceService),
            ingest=client_factory(ingest_api.IngestService),
            logical_series=client_factory(timeseries_logicalseries.LogicalSeriesService),
            logset=client_factory(datasource_logset.LogSetService),
            run=client_factory(scout.RunService),
            units=client_factory(scout.UnitsService),
            upload=client_factory(upload_api.UploadService),
            video=client_factory(scout_video.VideoService),
            compute=client_factory(scout_compute_api.ComputeService),
            storage=client_factory(storage_datasource_api.NominalDataSourceService),
            storage_writer=client_factory(storage_writer_api.NominalChannelWriterService),
            template=client_factory(scout.TemplateService),
            notebook=client_factory(scout.NotebookService),
            checklist_execution=client_factory(scout_checklistexecution_api.ChecklistExecutionService),
            datareview=client_factory(scout_datareview_api.DataReviewService),
            proto_write=client_factory(ProtoWriteService),
        )


class HasAuthHeader(Protocol):
    @property
    def auth_header(self) -> str: ...
