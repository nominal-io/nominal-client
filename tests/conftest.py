from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core.channel import Channel, ChannelDataType


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
