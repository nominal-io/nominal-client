from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core.asset import Asset
from nominal.core.channel import Channel, ChannelDataType
from nominal.core.connection import Connection
from nominal.core.dataset import Dataset, DatasetBounds
from nominal.core.run import Run
from nominal.core.video import Video


@pytest.fixture
def mock_clients():
    """A mock _ClientsBunch with a preset auth header."""
    clients = MagicMock()
    clients.auth_header = "Bearer test-token"
    return clients


@pytest.fixture
def make_channel(mock_clients):
    """Factory fixture that creates Channel instances sharing the same mock clients."""

    def _make(
        name: str,
        data_type: ChannelDataType | None = ChannelDataType.DOUBLE,
        data_source: str = "ds-1",
    ) -> Channel:
        return Channel(
            name=name,
            data_source=data_source,
            data_type=data_type,
            unit=None,
            description=None,
            _clients=mock_clients,
        )

    return _make


@pytest.fixture
def make_series_count_response():
    """Factory fixture that builds a mock BatchGetSeriesCountResponse from a list of counts.

    Pass `None` in a slot to simulate a channel on an external datasource (series_count absent).
    """

    def _make(counts: list[int | None]):
        response = MagicMock()
        response.responses = [MagicMock(series_count=count) for count in counts]
        return response

    return _make


@pytest.fixture
def mock_asset(mock_clients):
    return Asset(
        rid="asset-rid-1",
        name="Test Asset",
        description=None,
        properties={},
        labels=[],
        created_at=0,
        _clients=mock_clients,
    )


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
        end=None,
        run_number=1,
        assets=[],
        created_at=0,
        _clients=mock_clients,
    )


@pytest.fixture
def mock_dataset(mock_clients):
    return Dataset(
        rid="dataset-rid-1",
        name="Test Dataset",
        description=None,
        bounds=DatasetBounds(start=0, end=1),
        properties={},
        labels=[],
        _clients=mock_clients,
    )


@pytest.fixture
def mock_connection(mock_clients):
    return Connection(
        rid="connection-rid-1",
        name="Test Connection",
        description=None,
        _clients=mock_clients,
    )


@pytest.fixture
def mock_video(mock_clients):
    return Video(
        rid="video-rid-1",
        name="Test Video",
        description=None,
        properties={},
        labels=[],
        created_at=0,
        _clients=mock_clients,
    )
