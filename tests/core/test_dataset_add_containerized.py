from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core.containerized_extractor import ContainerizedExtractor
from nominal.core.dataset import Dataset, DatasetBounds
from nominal.core.ingestion_job import IngestionJob
from nominal.protos.ingest.v2 import containerized_extractor_pb2
from nominal.protos.ingestjobs.v1 import ingest_jobs_pb2
from nominal.protos.registry.v2 import registry_pb2
from nominal.protos.types.time import timestamp_parsers_pb2


def _dataset(clients: MagicMock) -> Dataset:
    return Dataset(
        rid="ri.dataset.1",
        name="d",
        description=None,
        properties={},
        labels=(),
        bounds=DatasetBounds(start=0, end=1),
        is_archived=False,
        _clients=clients,
    )


def _image_proto(required_input: str) -> registry_pb2.ContainerImage:
    return registry_pb2.ContainerImage(
        rid="ri.img.1",
        tag="v1",
        extractor_rid="ri.ext.1",
        status=registry_pb2.CONTAINER_IMAGE_STATUS_READY,
        file_output_format=registry_pb2.FILE_OUTPUT_FORMAT_PARQUET,
        inputs=[
            registry_pb2.FileExtractionInput(environment_variable=required_input, name=required_input, required=True)
        ],
        default_timestamp_metadata=registry_pb2.TimestampMetadata(
            series_name="ts",
            timestamp_type=timestamp_parsers_pb2.TimestampType(
                absolute=timestamp_parsers_pb2.AbsoluteTimestamp(iso8601=timestamp_parsers_pb2.Iso8601Timestamp())
            ),
        ),
    )


def _extractor(clients: MagicMock, *, active_image: registry_pb2.ContainerImage | None) -> ContainerizedExtractor:
    proto = containerized_extractor_pb2.ContainerizedExtractor(
        rid="ri.ext.1", workspace_rid="ri.ws", name="parser", is_archived=False
    )
    if active_image is not None:
        proto.active_container_image.CopyFrom(active_image)
    return ContainerizedExtractor._from_proto(clients, proto)


def test_add_containerized_validates_against_active_image_without_refetch() -> None:
    """Validation reads the extractor's nested active image; it does not re-fetch the image via GetImage."""
    clients = MagicMock()
    dataset = _dataset(clients)
    extractor = _extractor(clients, active_image=_image_proto("INPUT_FILE"))
    # If the SDK were to re-fetch the active image, this is what the server would return; it must go unused.
    clients.registry.GetImage.return_value = registry_pb2.GetImageResponse(image=_image_proto("INPUT_FILE"))

    with pytest.raises(ValueError, match="INPUT_FILE"):
        dataset.add_containerized(extractor, sources={"WRONG_KEY": "./data.bin"})

    clients.registry.GetImage.assert_not_called()


def test_add_containerized_raises_when_no_active_image() -> None:
    """An extractor with no active image raises a clear pre-flight error."""
    clients = MagicMock()
    dataset = _dataset(clients)
    extractor = _extractor(clients, active_image=None)

    with pytest.raises(ValueError, match="no active container image"):
        dataset.add_containerized(extractor, sources={"INPUT_FILE": "./data.bin"})


def test_add_containerized_returns_ingestion_job() -> None:
    """A successful trigger eagerly returns a populated IngestionJob built from the response's ingest_job_rid."""
    clients = MagicMock()
    dataset = _dataset(clients)
    # Active image with no required inputs, so empty sources passes validation and reaches the ingest trigger.
    image = registry_pb2.ContainerImage(
        rid="ri.img.1", tag="v1", extractor_rid="ri.ext.1", file_output_format=registry_pb2.FILE_OUTPUT_FORMAT_PARQUET
    )
    extractor = _extractor(clients, active_image=image)
    clients.ingest.ingest.return_value = MagicMock(ingest_job_rid="ri.ingest-job.1")
    clients.grpc_ingest_jobs.GetIngestJob.return_value = ingest_jobs_pb2.GetIngestJobResponse(
        ingest_job=ingest_jobs_pb2.IngestJob(
            created_by="ri.user",
            rid="ri.ingest-job.1",
            type=ingest_jobs_pb2.INGEST_JOB_TYPE_CONTAINERIZED,
            status=ingest_jobs_pb2.INGEST_JOB_STATUS_IN_PROGRESS,
        )
    )

    job = dataset.add_containerized(extractor, sources={})

    assert isinstance(job, IngestionJob)
    assert job.rid == "ri.ingest-job.1"
    clients.grpc_ingest_jobs.GetIngestJob.assert_called_once_with(
        ingest_jobs_pb2.GetIngestJobRequest(ingest_job_rid="ri.ingest-job.1")
    )
