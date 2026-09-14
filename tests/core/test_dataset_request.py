from unittest.mock import MagicMock, patch

from nominal_api import scout_catalog

from nominal.core.dataset import _create_dataset_request
from nominal.core.dataset_file import IngestStatus
from tests.e2e.conftest import ingested_dataset


def test_dataset_request_uses_server_default_backing() -> None:
    """Omitting a backing type leaves it unset, so the server applies its own default."""
    request = _create_dataset_request("dataset")

    assert request.dataset_type is None


def test_dataset_request_preserves_explicit_legacy_backing() -> None:
    """An explicit backing type and workspace reach the request, alongside the split tag keys the server requires."""
    request = _create_dataset_request(
        "dataset", workspace_rid="workspace-rid", dataset_type=scout_catalog.DatasetBackingType.LEGACY
    )

    assert request.dataset_type == scout_catalog.DatasetBackingType.LEGACY
    assert request.workspace == "workspace-rid"
    assert request.channel_search_split_tag_keys == []


def test_readonly_e2e_fixture_constructs_legacy_dataset() -> None:
    """The read-only fixture builds a valid request and archives its dataset after use."""
    client = MagicMock()
    client._clients.resolve_default_workspace_rid.return_value = "workspace-rid"
    dataset = MagicMock()
    dataset.add_from_io.return_value.poll_until_ingestion_completed.return_value.ingest_status = IngestStatus.SUCCESS

    with patch("tests.e2e.conftest.Dataset._from_conjure", return_value=dataset):
        fixture = ingested_dataset.__wrapped__(client, b"timestamp,value\n")
        try:
            assert next(fixture) is dataset
            request = client._clients.catalog.create_dataset.call_args.args[1]
            assert request.dataset_type == scout_catalog.DatasetBackingType.LEGACY
            assert request.workspace == "workspace-rid"
            assert request.channel_search_split_tag_keys == []
        finally:
            fixture.close()

    dataset.archive.assert_called_once_with()
