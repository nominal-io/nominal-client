from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass
from functools import partial

import jwt
from conjure_python_client import ConjureHTTPError, RequestsClient, ServiceConfiguration
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
    scout_dataexport_api,
    scout_datasource,
    scout_units_api,
    scout_video,
    timeseries_logicalseries_api,
    upload_api,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ClientsBunch:
    auth_header: str

    attachment: attachments_api.AttachmentService
    authentication: authentication_api.AuthenticationServiceV2
    catalog: scout_catalog.CatalogService
    checklist: scout_checks_api.ChecklistService
    compute_representation: scout_compute_representation_api.ComputeRepresentationService
    dataexport: scout_dataexport_api.DataExportService
    datasource: scout_datasource.DataSourceService
    ingest: ingest_api.IngestService
    logical_series: timeseries_logicalseries_api.LogicalSeriesService
    logset: datasource_logset.LogSetService
    run: scout.RunService
    units: scout_units_api.UnitsService
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
            dataexport=client_factory(scout_dataexport_api.DataExportService),
            datasource=client_factory(scout_datasource.DataSourceService),
            ingest=client_factory(ingest_api.IngestService),
            logical_series=client_factory(timeseries_logicalseries_api.LogicalSeriesService),
            logset=client_factory(datasource_logset.LogSetService),
            run=client_factory(scout.RunService),
            units=client_factory(scout_units_api.UnitsService),
            upload=client_factory(upload_api.UploadService),
            video=client_factory(scout_video.VideoService),
        )

    def auth_expiration(self) -> datetime.datetime:
        """Returns the timestamp at which the current auth token becomes invalid

        Raises:
            ValueError: Malformed auth token present
        """
        token_parts = self.auth_header.split()
        if len(token_parts) != 2:
            raise ValueError(f"Could not parse auth header: expected two space-separated parts")

        token = token_parts[1]
        payload = jwt.decode(token, options={"verify_signature": False}, algorithms=["RS256"])
        expiry_s = payload.get("exp")
        if expiry_s is None:
            raise ValueError("Expected expiry to be present in auth token, but not present")

        return datetime.datetime.fromtimestamp(expiry_s, datetime.timezone.utc)

    def auth_valid(self) -> bool:
        """Validates that authentication tokens are valid and we can successfully hit the nominal API."""
        try:
            # simplest possible API call with minimal overhead to validate conectivity and
            # proper authentication
            self.authentication.get_my_profile(self.auth_header)
            return True
        except ConjureHTTPError as ex:
            status_code = ex.response.status_code
            if status_code == 401:
                logger.error(
                    "Invalid authentication token: please visit 'https://app.gov.nominal.io/settings/user?tab=tokens' and set a new one with nom auth set-token"
                )
            elif status_code != 200:
                logger.error(f"Unknown error {status_code} received when validating authentication token")

        return False
