from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core.run import Run
from nominal.protos.event.v2 import event_pb2


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


def test_search_events_ors_run_assets(mock_run, mock_clients):
    """Run.search_events matches events on any of the run's assets (a single OR asset filter)."""
    mock_clients.event.SearchEvents.return_value = event_pb2.SearchEventsResponse()

    result = mock_run.search_events()

    assert result == []
    mock_clients.event.SearchEvents.assert_called_once()
    request = mock_clients.event.SearchEvents.call_args.args[0]
    assert request.query == event_pb2.SearchQuery(
        **{
            "and": event_pb2.SearchQueryList(
                queries=[
                    event_pb2.SearchQuery(
                        assets=event_pb2.AssetsFilter(assets=["asset-rid-1", "asset-rid-2"], operator=event_pb2.OR)
                    )
                ]
            )
        }
    )


def test_search_events_empty_assets_returns_no_events(make_run, mock_clients):
    """A run with no associated assets returns no events instead of searching all events."""
    run = make_run([])

    result = run.search_events()

    assert result == []
    mock_clients.event.SearchEvents.assert_not_called()


def test_nominal_url_identifies_the_run_by_rid(mock_run, mock_clients):
    """Run pages are addressed by rid: the app no longer serves run-number URLs."""
    mock_clients.app_base_url = "https://app.nominal.test"
    mock_clients.resolve_default_workspace_rid.return_value = "ri.workspace.test"

    assert mock_run.nominal_url == "https://app.nominal.test/w/ri.workspace.test/runs/run-rid-1"
