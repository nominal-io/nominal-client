from __future__ import annotations

from dataclasses import dataclass

from conjure_python_client import RequestsClient, ServiceConfiguration
from typing_extensions import Self

from .._api.combined import (
    attachments_api,
    authentication_api,
    datasource_logset,
    ingest_api,
    scout,
    scout_catalog,
    scout_checks_api,
    scout_compute_representation_api,
    scout_datasource,
    scout_units_api,
    scout_video,
    timeseries_logicalseries_api,
    upload_api,
)


@dataclass(frozen=True)
class ClientsBunch:
    auth_header: str
    run: scout.RunService
    upload: upload_api.UploadService
    authentication: authentication_api.AuthenticationServiceV2
    ingest: ingest_api.IngestService
    catalog: scout_catalog.CatalogService
    attachment: attachments_api.AttachmentService
    compute_representation: scout_compute_representation_api.ComputeRepresentationService
    checklist: scout_checks_api.ChecklistService
    video: scout_video.VideoService
    logset: datasource_logset.LogSetService
    units: scout_units_api.UnitsService
    datasource: scout_datasource.DataSourceService
    logical_series: timeseries_logicalseries_api.LogicalSeriesService

    @classmethod
    def from_config(cls, cfg: ServiceConfiguration, agent: str, token: str) -> Self:
        run_client = RequestsClient.create(scout.RunService, agent, cfg)
        upload_client = RequestsClient.create(upload_api.UploadService, agent, cfg)
        ingest_client = RequestsClient.create(ingest_api.IngestService, agent, cfg)
        catalog_client = RequestsClient.create(scout_catalog.CatalogService, agent, cfg)
        attachment_client = RequestsClient.create(attachments_api.AttachmentService, agent, cfg)
        compute_representation_client = RequestsClient.create(
            scout_compute_representation_api.ComputeRepresentationService, agent, cfg
        )
        checklist_client = RequestsClient.create(scout_checks_api.ChecklistService, agent, cfg)
        authentication_client = RequestsClient.create(authentication_api.AuthenticationServiceV2, agent, cfg)
        video_client = RequestsClient.create(scout_video.VideoService, agent, cfg)
        logset_client = RequestsClient.create(datasource_logset.LogSetService, agent, cfg)
        unit_client = RequestsClient.create(scout_units_api.UnitsService, agent, cfg)
        datasource_client = RequestsClient.create(scout_datasource.DataSourceService, agent, cfg)
        logical_series_client = RequestsClient.create(timeseries_logicalseries_api.LogicalSeriesService, agent, cfg)

        auth_header = f"Bearer {token}"
        return cls(
            auth_header=auth_header,
            run=run_client,
            upload=upload_client,
            ingest=ingest_client,
            catalog=catalog_client,
            attachment=attachment_client,
            compute_representation=compute_representation_client,
            checklist=checklist_client,
            authentication=authentication_client,
            video=video_client,
            logset=logset_client,
            units=unit_client,
            datasource=datasource_client,
            logical_series=logical_series_client,
        )
