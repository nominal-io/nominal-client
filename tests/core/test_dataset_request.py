from nominal_api import scout_catalog

from nominal.core.dataset import _create_dataset_request


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
