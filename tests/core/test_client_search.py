from unittest.mock import MagicMock, patch

from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.core.client import NominalClient


def test_search_data_reviews_passes_archive_status():
    """NominalClient.search_data_reviews forwards archive_status to the shared data-review iterator."""
    client = NominalClient(_clients=MagicMock())

    with patch("nominal.core.client._iter_search_data_reviews", return_value=iter(())) as mock_reviews:
        result = client.search_data_reviews(archive_status=ArchiveStatusFilter.ANY)

    assert result == []
    mock_reviews.assert_called_once()
    assert mock_reviews.call_args.kwargs["archive_status"] == ArchiveStatusFilter.ANY
