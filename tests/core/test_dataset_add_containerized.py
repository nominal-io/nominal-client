from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core.containerized_extractor import (
    ContainerImage,
    ContainerImageStatus,
    ContainerizedExtractor,
    FileExtractionInput,
)
from nominal.core.dataset import Dataset, DatasetBounds


def _dataset(clients: MagicMock) -> Dataset:
    return Dataset(
        rid="ri.dataset.1",
        name="d",
        description=None,
        properties={},
        labels=(),
        bounds=DatasetBounds(start=0, end=1),
        _clients=clients,
    )


def _extractor(clients: MagicMock, *, active_image_rid: str | None) -> ContainerizedExtractor:
    return ContainerizedExtractor(
        rid="ri.ext.1",
        name="parser",
        description=None,
        is_archived=False,
        active_container_image_rid=active_image_rid,
        _workspace_rid="ri.ws",
        _clients=clients,
    )


def _image_with_required_input(clients: MagicMock) -> ContainerImage:
    return ContainerImage(
        rid="ri.img.1",
        tag="v1",
        status=ContainerImageStatus.READY,
        size_bytes=1,
        created_at=0,
        extractor_rid="ri.ext.1",
        inputs=(FileExtractionInput("Input", environment_variable="INPUT_FILE", required=True),),
        parameters=(),
        file_output_format=MagicMock(),
        _workspace_rid="ri.ws",
        _clients=clients,
    )


def test_add_containerized_raises_when_required_input_missing() -> None:
    """A required input absent from `sources` raises before any upload."""
    clients = MagicMock()
    dataset = _dataset(clients)
    extractor = _extractor(clients, active_image_rid="ri.img.1")
    extractor_get_image = MagicMock(return_value=_image_with_required_input(clients))
    object.__setattr__(extractor, "get_image", extractor_get_image)  # frozen dataclass

    with pytest.raises(ValueError, match="INPUT_FILE"):
        dataset.add_containerized(extractor, sources={"WRONG_KEY": "./data.bin"})


def test_add_containerized_raises_when_no_active_image() -> None:
    """An extractor with no active image raises a clear pre-flight error."""
    clients = MagicMock()
    dataset = _dataset(clients)
    extractor = _extractor(clients, active_image_rid=None)

    with pytest.raises(ValueError, match="no active container image"):
        dataset.add_containerized(extractor, sources={"INPUT_FILE": "./data.bin"})
