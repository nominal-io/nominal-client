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


def trigger_ingest(service: IngestService, auth: str, s3_path: str, dataset_name: str = "some_dataset"):
    ingest_request = TriggerIngest(source=IngestSource(S3IngestSource(s3_path)), dataset_name=dataset_name)

    service.trigger_ingest(auth, ingest_request)
