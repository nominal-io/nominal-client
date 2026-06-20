from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.core.run import Run


@pytest.fixture
def mock_clients():
    return MagicMock()


@pytest.fixture
def mock_run(mock_clients):
    return Run(
        rid="run-rid-1",
        name="Test Run",
        description="",
        properties={},
        labels=[],
        links=[],
        start=0,
        end=1,
        run_number=1,
        assets=["asset-rid-1", "asset-rid-2"],
        created_at=0,
        _clients=mock_clients,
    )


def test_search_events_scopes_to_run_assets(mock_run):
    """Run.search_events forwards the run's assets to the shared event search helper."""
    with patch("nominal.core.run._search_events", return_value=[]) as mock_search_events:
        result = mock_run.search_events()

    assert result == []
    mock_search_events.assert_called_once()
    assert mock_search_events.call_args.kwargs["asset_rids"] == ["asset-rid-1", "asset-rid-2"]


def test_search_events_passes_archive_status(mock_run):
    """Run.search_events forwards archive_status to the shared event search helper."""
    with patch("nominal.core.run._search_events", return_value=[]) as mock_search_events:
        result = mock_run.search_events(archive_status=ArchiveStatusFilter.ANY)

    assert result == []
    mock_search_events.assert_called_once()
    assert mock_search_events.call_args.kwargs["archive_status"] == ArchiveStatusFilter.ANY
