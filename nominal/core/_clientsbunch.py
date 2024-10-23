from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from typing import Protocol

from conjure_python_client import RequestsClient, ServiceConfiguration
from typing_extensions import Self

from nominal._api.combined import (
    attachments_api,
    authentication_api,
    datasource_logset,
    ingest_api,
    scout,
    scout_catalog,
    scout_checks_api,
    scout_compute_representation_api,
    scout_dataexport_api,
    scout_datasource,
    scout_datasource_connection,
    scout_video,
    timeseries_logicalseries,
    upload_api,
)


@dataclass(frozen=True)
class ClientsBunch:
    auth_header: str

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

    @classmethod
    def from_config(cls, cfg: ServiceConfiguration, agent: str, token: str) -> Self:
        client_factory = partial(RequestsClient.create, user_agent=agent, service_config=cfg)

        return cls(
            auth_header=f"Bearer {token}",
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
        )


class HasAuthHeader(Protocol):
    @property
    def auth_header(self) -> str: ...
