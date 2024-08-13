from conjure_python_client import (
    RequestsClient,
    ServiceConfiguration,
)

from _api.ingest.ingest_api import IngestService, TriggerIngest, IngestSource, S3IngestSource, TimestampMetadata


def create_service(uri: str) -> IngestService:
    config = ServiceConfiguration()
    config.uris = [uri]
    service = RequestsClient.create(
        IngestService, user_agent="nominal-client", service_config=config
    )
    return service
