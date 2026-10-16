from __future__ import annotations

from nominal_api import scout_catalog
from nominal_api import scout_datasource_connection_api as connection_api

from nominal.core.dataset import Dataset
from nominal.core.external_connection import ApiConnectionDetails, ExternalConnectionConfig


def test_registration_replaces_config_and_refreshes_same_dataset(mock_clients, make_enriched_dataset):
    """Registration targets the dataset and uses authoritative metadata without a second read."""
    dataset = Dataset._from_conjure(mock_clients, make_enriched_dataset())
    response = make_enriched_dataset(
        name="Server name",
        external_connection_metadata=scout_catalog.ExternalConnectionMetadata(
            connection_details=connection_api.ConnectionDetails(
                api=connection_api.ApiConnectionDetails(api_uri="https://example.test", token_secret_rid="secret-rid")
            ),
            connection_status=connection_api.ConnectionStatus(is_connected=False),
        ),
    )
    mock_clients.catalog.register_external_connection_config.return_value = response

    result = dataset.register_external_connection(
        ExternalConnectionConfig(ApiConnectionDetails("https://example.test", "secret-rid"))
    )

    assert result is dataset
    assert dataset.name == "Server name"
    assert dataset.external_connection is not None
    assert not dataset.external_connection.is_connected
    assert dataset.external_connection.details == ApiConnectionDetails("https://example.test", "secret-rid")
    auth, request = mock_clients.catalog.register_external_connection_config.call_args.args
    assert auth == mock_clients.auth_header
    assert request.dataset_rid == dataset.rid
    assert request.metadata == {}
    assert request.required_tag_names == []
    assert request.available_tag_values is None
    assert request.scraping is None
    assert request.limits is None
    assert request.should_scrape is False
    mock_clients.catalog.get_enriched_datasets.assert_not_called()


def test_refresh_clears_absent_external_metadata(mock_clients, make_enriched_dataset):
    """A refresh removes an old external snapshot when the server no longer returns it."""
    metadata = scout_catalog.ExternalConnectionMetadata(
        connection_details=connection_api.ConnectionDetails(
            api=connection_api.ApiConnectionDetails(api_uri="https://example.test", token_secret_rid="secret-rid")
        ),
        connection_status=connection_api.ConnectionStatus(is_connected=True),
    )
    dataset = Dataset._from_conjure(mock_clients, make_enriched_dataset(external_connection_metadata=metadata))
    assert dataset.external_connection is not None
    dataset._refresh_from_api(make_enriched_dataset())
    assert dataset.external_connection is None
