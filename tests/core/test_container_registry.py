from __future__ import annotations

from io import BytesIO
from unittest.mock import MagicMock

import pytest

from nominal.core.client import NominalClient


def test_upload_container_image_requires_profile_workspace() -> None:
    clients = MagicMock()
    clients.workspace_rid = None
    clients.resolve_default_workspace_rid = MagicMock()
    client = NominalClient(_clients=clients)

    with pytest.raises(ValueError, match="client profile must specify workspace_rid"):
        client.upload_container_image_from_io(BytesIO(b"image"), "extractor", "v1")

    clients.resolve_default_workspace_rid.assert_not_called()
    clients.upload.initiate_multipart_upload.assert_not_called()


def test_create_containerized_extractor_requires_profile_workspace() -> None:
    clients = MagicMock()
    clients.workspace_rid = None
    clients.resolve_default_workspace_rid = MagicMock()
    client = NominalClient(_clients=clients)

    with pytest.raises(ValueError, match="client profile must specify workspace_rid"):
        client.create_containerized_extractor(
            "extractor",
            container_image_rid="ri.container-image.main.image.1",
            timestamp_column="timestamp",
            timestamp_type="absolute",
        )

    clients.resolve_default_workspace_rid.assert_not_called()
    clients.containerized_extractors.register_containerized_extractor.assert_not_called()
