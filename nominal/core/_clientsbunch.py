from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from typing import Protocol

import requests
from conjure_python_client import RequestsClient, ServiceConfiguration
from typing_extensions import Self

from nominal._api.combined import (
    attachments_api,
    authentication_api,
    datasource_logset,
    ingest_api,
    scout,
    scout_assets,
    scout_catalog,
    scout_checks_api,
    scout_compute_api,
    scout_compute_representation_api,
    scout_dataexport_api,
    scout_datasource,
    scout_datasource_connection,
    scout_video,
    storage_datasource_api,
    storage_writer_api,
    timeseries_logicalseries,
    upload_api,
)


@dataclass(frozen=True)
class ClientsBunch:
    auth_header: str
    requests_session: requests.Session
    """The session should be used for requests to utilize the same cert as conjure calls."""

    assets: scout_assets.AssetService
    attachment: attachments_api.AttachmentService
    authentication: authentication_api.AuthenticationServiceV2
    catalog: scout_catalog.CatalogService
    checklist: scout_checks_api.ChecklistService
    compute_representation: scout_compute_representation_api.ComputeRepresentationService
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

    @classmethod
    def from_config(cls, cfg: ServiceConfiguration, agent: str, token: str, trust_store_path: str) -> Self:
        client_factory = partial(RequestsClient.create, user_agent=agent, service_config=cfg)
        requests_session = requests.Session()
        requests_session.cert = trust_store_path
        return cls(
            auth_header=f"Bearer {token}",
            requests_session=requests_session,
            assets=client_factory(scout_assets.AssetService),
            attachment=client_factory(attachments_api.AttachmentService),
            authentication=client_factory(authentication_api.AuthenticationServiceV2),
            catalog=client_factory(scout_catalog.CatalogService),
            checklist=client_factory(scout_checks_api.ChecklistService),
            compute_representation=client_factory(scout_compute_representation_api.ComputeRepresentationService),
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
        )


class HasAuthHeader(Protocol):
    @property
    def auth_header(self) -> str: ...


class HasRequestsSession(Protocol):
    @property
    def requests_session(self) -> requests.Session: ...
