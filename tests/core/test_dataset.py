from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, Mock, patch

import pytest
from nominal_api import ingest_api

from nominal.core.dataset import Dataset, DatasetBounds
from nominal.core.log import LogPoint
from nominal.core.unit import Unit

UNITS = [
    Unit(name="coulomb", symbol="C"),
    Unit(name="kilograms", symbol="kg"),
    Unit(name="mole", symbol="mol"),
]
EXTRACTOR_RID = "ri.extractor.test.1"
CONTAINER_IMAGE_RID = "ri.container-image.test.1"


@pytest.fixture
def mock_clients():
    clients = MagicMock()
    clients.logical_series = MagicMock()
    return clients


@pytest.fixture
def mock_dataset(mock_clients):
    ds = Dataset(
        rid="test-rid",
        name="Test Dataset",
        description="A dataset for testing",
        bounds=DatasetBounds(start=123455, end=123456),
        properties={},
        labels=[],
        _clients=mock_clients,
    )

    spy: MagicMock = MagicMock(wraps=ds.refresh)
    object.__setattr__(ds, "refresh", spy)
    spy.return_value = ds

    return ds


def test_write_logs_more_than_batch(mock_dataset: Dataset):
    endpoint = Mock()
    cast(Any, mock_dataset._clients.storage_writer).write_logs = endpoint

    log_0 = LogPoint(0, "a", {})
    log_1 = LogPoint(1, "b", {})
    log_2 = LogPoint(2, "c", {})

    def log_generator() -> Iterator[LogPoint]:
        yield log_0
        yield log_1
        yield log_2

    mock_dataset.write_logs(log_generator(), batch_size=2)

    assert len(endpoint.call_args_list) == 2

    _auth, _rid, first_req = endpoint.call_args_list[0][0]
    assert len(first_req.logs) == 2

    _auth, _rid, second_req = endpoint.call_args_list[1][0]
    assert len(second_req.logs) == 1


def test_write_logs_less_than_batch(mock_dataset: Dataset):
    endpoint = Mock()
    cast(Any, mock_dataset._clients.storage_writer).write_logs = endpoint

    log_0 = LogPoint(0, "a", {})
    log_1 = LogPoint(1, "b", {})
    log_2 = LogPoint(2, "c", {})

    def log_generator() -> Iterator[LogPoint]:
        yield log_0
        yield log_1
        yield log_2

    mock_dataset.write_logs(log_generator(), batch_size=1000)

    assert len(endpoint.call_args_list) == 1
    _auth, _rid, req = endpoint.call_args_list[0][0]
    assert len(req.logs) == 3


def _raw_docker_image() -> ingest_api.DockerImageSource:
    return ingest_api.DockerImageSource(
        authentication=ingest_api.Authentication(public=ingest_api.PublicAuthentication()),
        registry="",
        repository="",
        tag_details=ingest_api.TagDetails(default_tag="", tags=[]),
    )


def _raw_containerized_extractor(
    *,
    extractor_rid: str,
    container_image_rid: str | None,
) -> ingest_api.ContainerizedExtractor:
    return ingest_api.ContainerizedExtractor(
        created_at="2026-05-13T00:00:00Z",
        image=_raw_docker_image(),
        inputs=[
            ingest_api.FileExtractionInput(
                environment_variable="INPUT_FILE",
                file_filters=[ingest_api.FileFilter(suffix=".pkl")],
                name="input.pkl",
                required=True,
            )
        ],
        is_archived=False,
        labels=[],
        name="test-unpickle-extractor",
        output_file_format=ingest_api.FileOutputFormat.CSV,
        parameters=[],
        properties={},
        rid=extractor_rid,
        container_image_rid=container_image_rid,
    )


def test_add_containerized_accepts_self_hosted_extractor_rid(mock_dataset: Dataset, tmp_path: Path) -> None:
    source_path = tmp_path / "input.pkl"
    source_path.write_bytes(b"pickle")
    sentinel = object()
    mock_dataset._clients.auth_header = "Bearer token"
    mock_dataset._clients.resolve_default_workspace_rid.return_value = "ri.workspace.default"
    mock_dataset._clients.containerized_extractors.get_containerized_extractor.return_value = (
        _raw_containerized_extractor(
            extractor_rid=EXTRACTOR_RID,
            container_image_rid=CONTAINER_IMAGE_RID,
        )
    )

    with (
        patch("nominal.core.dataset.upload_multipart_file", return_value="s3://input.pkl") as upload,
        patch.object(Dataset, "_handle_ingest_response", return_value=sentinel),
    ):
        result = mock_dataset.add_containerized(
            EXTRACTOR_RID,
            {"INPUT_FILE": source_path},
            tags={"session_id": "test"},
            timestamp_column="timestamps-nanos",
            timestamp_type="epoch_nanoseconds",
        )

    assert result is sentinel
    mock_dataset._clients.containerized_extractors.get_containerized_extractor.assert_called_once_with(
        "Bearer token",
        EXTRACTOR_RID,
    )
    upload.assert_called_once()
    request = mock_dataset._clients.ingest.ingest.call_args.kwargs["trigger_ingest"]
    options = request.options.containerized
    assert options.extractor_rid == EXTRACTOR_RID
    assert options.tag is None
    assert options.sources["INPUT_FILE"].s3.path == "s3://input.pkl"
    assert options.additional_file_tags == {"session_id": "test"}
    assert options.timestamp_metadata.series_name == "timestamps-nanos"
    assert options.target.existing.dataset_rid == mock_dataset.rid


def test_add_containerized_rejects_tag_for_extractor_rid(mock_dataset: Dataset, tmp_path: Path) -> None:
    source_path = tmp_path / "input.pkl"
    source_path.write_bytes(b"pickle")
    mock_dataset._clients.auth_header = "Bearer token"

    with pytest.raises(ValueError, match="self-hosted"):
        mock_dataset.add_containerized(
            EXTRACTOR_RID,
            {"INPUT_FILE": source_path},
            tag="latest",
        )

    mock_dataset._clients.containerized_extractors.get_containerized_extractor.assert_not_called()
    mock_dataset._clients.ingest.ingest.assert_not_called()


def test_add_containerized_requires_timestamp_metadata_for_extractor_rid(
    mock_dataset: Dataset,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "input.pkl"
    source_path.write_bytes(b"pickle")
    mock_dataset._clients.auth_header = "Bearer token"

    with pytest.raises(ValueError, match="timestamp_column"):
        mock_dataset.add_containerized(
            EXTRACTOR_RID,
            {"INPUT_FILE": source_path},
        )

    mock_dataset._clients.containerized_extractors.get_containerized_extractor.assert_not_called()
    mock_dataset._clients.ingest.ingest.assert_not_called()
