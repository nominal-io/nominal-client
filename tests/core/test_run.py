from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from nominal_api import api

from nominal.core.run import Run


@pytest.fixture
def mock_clients():
    return MagicMock()


@pytest.fixture
def make_run(mock_clients):
    def _make_run(assets):
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
            assets=assets,
            created_at=0,
            _clients=mock_clients,
        )

    return _make_run


@pytest.fixture
def mock_run(make_run):
    return make_run(["asset-rid-1", "asset-rid-2"])


def _empty_search_response():
    response = MagicMock()
    response.results = []
    response.next_page_token = None
    return response


def test_search_events_ors_run_assets(mock_run, mock_clients):
    """Run.search_events matches events on any of the run's assets (a single OR asset filter)."""
    mock_clients.event.search_events.return_value = _empty_search_response()

    result = mock_run.search_events()

    assert result == []
    mock_clients.event.search_events.assert_called_once()
    _, request = mock_clients.event.search_events.call_args[0]
    asset_filters = [sub.assets for sub in request.query.and_ if sub.assets is not None]
    assert len(asset_filters) == 1
    assert asset_filters[0].assets == ["asset-rid-1", "asset-rid-2"]
    assert asset_filters[0].operator == api.SetOperator.OR


def test_search_events_empty_assets_returns_no_events(make_run, mock_clients):
    """A run with no associated assets returns no events instead of searching all events."""
    run = make_run([])

    result = run.search_events()

    assert result == []
    mock_clients.event.search_events.assert_not_called()


